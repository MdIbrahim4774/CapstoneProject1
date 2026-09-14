import joblib
import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path
import numpy as np

DB = "mysql+pymysql://root:root@localhost/network_operations"
MODEL = Path(__file__).resolve().parent / "ml3_outputs" / "ml3_xgboost_model.joblib"

engine = create_engine(DB)
model = joblib.load(MODEL)

FEATURES = [
    "avg_activity", "activity_growth", "active_hours",
    "peak_ratio", "variability", "internet_share",
    "hour_sin", "hour_cos"
]

df = pd.read_sql("""
    SELECT grid_id, feature_timestamp AS timestamp,
           avg_activity, activity_growth, active_hours,
           peak_ratio, variability, internet_share
    FROM network_feature_table
""", engine)

df["hour"] = pd.to_datetime(df["timestamp"]).dt.hour
df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24)

df["risk_score"] = model.predict_proba(df[FEATURES])[:, 1]

df["risk_score"] = model.predict_proba(df[FEATURES])[:, 1]

df["risk_level"] = pd.cut(
    df["risk_score"],
    [-1, 0.40, 0.70, 1],
    labels=["LOW", "MEDIUM", "HIGH"]
)

df["model_version"] = "ml3-xgboost-v1"

from sqlalchemy import text

rows = df[[
    "grid_id", "timestamp", "risk_score",
    "risk_level", "model_version"
]].to_dict("records")

with engine.begin() as conn:
    for r in rows:
        conn.execute(text("""
            INSERT INTO network_risk_scores
            (grid_id, timestamp, risk_score, risk_level, model_version)
            VALUES (:grid_id, :timestamp, :risk_score, :risk_level, :model_version)
            ON DUPLICATE KEY UPDATE
                risk_score = VALUES(risk_score),
                risk_level = VALUES(risk_level),
                model_version = VALUES(model_version)
        """), r)

print(f"ML6 scored {len(df)} rows")