import pandas as pd
from sqlalchemy import create_engine

engine = create_engine(
    "mysql+pymysql://root:root@localhost/network_operations"
)

df = pd.read_sql("""
    SELECT grid_id, timestamp, risk_score,
           risk_level, model_version
    FROM network_risk_scores
    ORDER BY risk_score DESC
    LIMIT 20
""", engine)

df.to_csv("output/top_20_operational_risk.csv", index=False)

print(df.to_string(index=False))