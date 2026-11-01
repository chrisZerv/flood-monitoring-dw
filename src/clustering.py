# =============================================================================
# Flood Monitoring System — Flood Risk Profiling (K-Means)
# =============================================================================
#
# Groups the 13 Greek regions into flood risk profiles (High / Medium / Low)
# using 5 features computed from the warehouse itself.
#
# Flow:
#   1. Extract   — 5 features per region from Fact_Flood + Fact_Weather
#   2. Transform — log-transform of the area features (limits the dominance
#                  of Thessaly as an outlier), standardisation (z-score)
#   3. Choose k  — silhouette score for k=2..6 (as a check, not a blind pick)
#   4. K-Means with k=3
#   5. Interpret — map cluster numbers to High/Medium/Low by mean flooded
#                  area (the cluster with the largest mean flooding = High)
#   6. Write-back — update Dim_Location.risk_profile
#
# Note on choosing k=3:
# The silhouette score favours k=2. k=3 is used because:
#   (a) Three risk levels (high/medium/low) are more actionable for civil
#       protection than two.
#   (b) The silhouette of k=3 (~0.32) is acceptable (>0.3 = reasonable
#       structure).
#   (c) The resulting profiles have a clear geographic/hydrological reading
#       (lowland river basins vs mainland vs islands).
# This is a deliberate trade-off, not an arbitrary choice.
# =============================================================================

import os
import sys
import logging

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score


# =============================================================================
# CONFIG
# =============================================================================

DB_HOST = os.environ.get("FLOOD_DW_HOST", "localhost")
DB_NAME = os.environ.get("FLOOD_DW_NAME", "flood_dw")
DB_USER = os.environ.get("FLOOD_DW_USER", "root")

# The 5 clustering features — each captures a different dimension of risk
FEATURE_COLS = [
    "total_flooded_km2",   # cumulative flooded area over all observations
    "peak_flooded_km2",    # extreme event — how bad it can get
    "high_signal_count",   # frequency of strong signal outside named events
    "avg_likelihood",      # detection confidence/quality
    "heavy_rain_days",     # meteorological exposure (the driver)
]

N_CLUSTERS   = 3
RANDOM_STATE = 42   # for reproducibility


# =============================================================================
# Logging
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("clustering")


# =============================================================================
# Connection — same pattern as the other scripts
# =============================================================================

try:
    from db_config import DB_PASSWORD as db_pwd
except ImportError:
    db_pwd = os.environ.get("FLOOD_DW_PWD")
    if not db_pwd:
        log.error("No DB password found in db_config.py or in FLOOD_DW_PWD.")
        sys.exit(1)

engine = create_engine(
    f"mysql+pymysql://{DB_USER}:{db_pwd}@{DB_HOST}/{DB_NAME}",
    pool_pre_ping=True,
    pool_recycle=1800,
)


# =============================================================================
# 1. Extract — features per region
# =============================================================================
#
# Two queries because the two fact tables (Fact_Flood, Fact_Weather) are
# separate — a fact constellation, joined through Dim_Location.

FLOOD_FEATURES_SQL = text("""
    SELECT
        l.location_key,
        l.region_name,
        COALESCE(SUM(f.flooded_area_km2), 0)                            AS total_flooded_km2,
        COALESCE(MAX(f.flooded_area_km2), 0)                            AS peak_flooded_km2,
        COALESCE(SUM(CASE WHEN f.quality_flag = 'high_signal'
                          THEN 1 ELSE 0 END), 0)                        AS high_signal_count,
        COALESCE(AVG(f.flood_likelihood), 0)                            AS avg_likelihood
    FROM Dim_Location l
    LEFT JOIN Fact_Flood f
           ON l.location_key = f.location_key
          AND f.is_current = 1
    GROUP BY l.location_key, l.region_name
""")

WEATHER_FEATURES_SQL = text("""
    SELECT
        l.location_key,
        COALESCE(SUM(CASE WHEN w.precipitation_mm > 20
                          THEN 1 ELSE 0 END), 0)                        AS heavy_rain_days
    FROM Dim_Location l
    LEFT JOIN Fact_Weather w ON l.location_key = w.location_key
    GROUP BY l.location_key
""")


def extract_features():
    """Reads the features from the warehouse and joins them into one DataFrame."""
    with engine.connect() as conn:
        flood_df   = pd.read_sql(FLOOD_FEATURES_SQL, conn)
        weather_df = pd.read_sql(WEATHER_FEATURES_SQL, conn)

    df = flood_df.merge(weather_df, on="location_key", how="left")
    log.info("Extracted features for %d regions", len(df))
    return df


# =============================================================================
# 2. Transform — log + standardisation
# =============================================================================
#
# Two transformations:
#
# (a) log1p on the area features. Thessaly has 6,000+ km² of cumulative
#     flooded area, the next region (Central Macedonia) about 1,300, and the
#     islands less than 50. On a linear scale Thessaly would pull the whole
#     K-Means solution. log1p (log(1+x)) compresses the range but keeps the
#     order.
#
# (b) StandardScaler (z-score) on everything. K-Means relies on Euclidean
#     distances, so a feature ranging 0–10,000 would dominate one ranging
#     0–1 (likelihood). The z-score puts all features on the same scale
#     (mean 0, std 1).

def prepare_features(df):
    """Log-transform of the area features + standardisation of all features."""
    X = df[FEATURE_COLS].copy()

    for col in ["total_flooded_km2", "peak_flooded_km2", "high_signal_count"]:
        X[col] = np.log1p(X[col])

    scaler = StandardScaler()
    return scaler.fit_transform(X)


# =============================================================================
# 3. Evaluating k — silhouette score
# =============================================================================

def evaluate_k(X_scaled, k_range=range(2, 7)):
    """
    Runs K-Means for k=2..6 and computes the silhouette score of each k,
    so the choice of k is made knowing what each value gives statistically.
    """
    log.info("Evaluating k with the silhouette score:")
    scores = {}
    for k in k_range:
        km = KMeans(n_clusters=k, random_state=RANDOM_STATE, n_init=10)
        labels = km.fit_predict(X_scaled)
        score = silhouette_score(X_scaled, labels)
        scores[k] = score
        log.info("   k=%d : silhouette = %.4f", k, score)
    best_k = max(scores, key=scores.get)
    log.info("Statistically best k: %d", best_k)
    log.info("k chosen for interpretation: %d", N_CLUSTERS)
    return scores


# =============================================================================
# 4 & 5. K-Means + cluster interpretation
# =============================================================================

def run_clustering(df, X_scaled):
    """
    Runs K-Means with k=3 and maps the cluster numbers (0/1/2) to the labels
    High/Medium/Low. The mapping uses the mean flooded area of each cluster,
    so the labels are deterministic and do not depend on how K-Means
    happened to number the clusters.
    """
    km = KMeans(n_clusters=N_CLUSTERS, random_state=RANDOM_STATE, n_init=10)
    df = df.copy()
    df["cluster"] = km.fit_predict(X_scaled)

    # Order clusters from lowest to highest mean flooding.
    cluster_means = df.groupby("cluster")["total_flooded_km2"].mean().sort_values()
    ordered = list(cluster_means.index)
    label_map = {
        ordered[0]: "Low",
        ordered[1]: "Medium",
        ordered[2]: "High",
    }
    df["risk_profile"] = df["cluster"].map(label_map)
    return df


# =============================================================================
# 6. Write-back to the database
# =============================================================================
#
# The result is stored in Dim_Location as the risk_profile column. The
# clustering thus becomes part of the warehouse — available to queries, to
# the Power BI dashboard and to any other tool connected to the database.

def write_back(df):
    """Updates Dim_Location.risk_profile for every region."""
    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(
                text("""
                    UPDATE Dim_Location
                    SET risk_profile = :profile
                    WHERE location_key = :lk
                """),
                {"profile": row["risk_profile"], "lk": int(row["location_key"])},
            )
    log.info("Updated Dim_Location.risk_profile for %d regions", len(df))


# =============================================================================
# Main
# =============================================================================

def main():
    log.info("=" * 60)
    log.info("Flood Risk Profiling — K-Means Clustering")
    log.info("=" * 60)

    df = extract_features()
    X_scaled = prepare_features(df)
    evaluate_k(X_scaled)
    df = run_clustering(df, X_scaled)

    # Print the results
    log.info("-" * 60)
    log.info("Clustering results:")
    result = df[[
        "region_name", "total_flooded_km2", "peak_flooded_km2",
        "high_signal_count", "heavy_rain_days", "risk_profile",
    ]].sort_values("total_flooded_km2", ascending=False)
    for _, r in result.iterrows():
        log.info("   %-32s %-8s (total %.1f km², peak %.1f km²)",
                 r["region_name"], r["risk_profile"],
                 r["total_flooded_km2"], r["peak_flooded_km2"])

    write_back(df)

    log.info("-" * 60)
    summary = df.groupby("risk_profile").size()
    for profile in ["High", "Medium", "Low"]:
        count = summary.get(profile, 0)
        log.info("   %-8s : %d regions", profile, count)


if __name__ == "__main__":
    main()
