"""
SQL queries for network analytics.
"""

GET_TOTAL_ACTIVITY = """
SELECT
    COALESCE(SUM(f.total_activity), 0) AS total_activity
FROM fact_network_activity AS f
INNER JOIN dim_time AS t
    ON f.time_key = t.time_key
WHERE t.timestamp <= %s
"""


GET_ACTIVE_GRIDS = """
SELECT
    COUNT(*) AS active_grids
FROM (
    SELECT
        f.grid_key
    FROM fact_network_activity AS f
    INNER JOIN dim_time AS t
        ON f.time_key = t.time_key
    WHERE t.timestamp <= %s
    GROUP BY f.grid_key
    HAVING SUM(f.total_activity) > 0
) AS active
"""


GET_PEAK_HOUR = """
SELECT
    t.hour,
    SUM(f.total_activity) AS activity
FROM fact_network_activity AS f
INNER JOIN dim_time AS t
    ON f.time_key = t.time_key
WHERE t.timestamp <= %s
GROUP BY t.hour
ORDER BY activity DESC, t.hour ASC
LIMIT 1
"""


GET_TOP_GRID = """
SELECT
    g.grid_id,
    gt.activity
FROM (
    SELECT
        f.grid_key,
        SUM(f.total_activity) AS activity
    FROM fact_network_activity AS f
    INNER JOIN dim_time AS t
        ON f.time_key = t.time_key
    WHERE t.timestamp <= %s
    GROUP BY f.grid_key
    ORDER BY activity DESC
    LIMIT 1
) AS gt
INNER JOIN dim_grid AS g
    ON g.grid_key = gt.grid_key
ORDER BY g.grid_id ASC
LIMIT 1
"""

GET_MAX_TIMESTAMP = """
SELECT MAX(timestamp) AS as_of
FROM dim_time
"""

GET_GRID_ACTIVITY = """
SELECT
    t.timestamp,
    DATE(t.timestamp) AS date,
    t.hour,
    SUM(f.call_in + f.call_out) AS call_activity,
    SUM(f.sms_in + f.sms_out) AS sms_activity,
    SUM(f.internet) AS internet_activity,
    SUM(f.total_activity) AS total_activity
FROM fact_network_activity AS f
INNER JOIN dim_time AS t
    ON f.time_key = t.time_key
INNER JOIN dim_grid AS g
    ON f.grid_key = g.grid_key
WHERE g.grid_id = %s
  AND t.timestamp BETWEEN %s AND %s
GROUP BY
    t.timestamp,
    t.hour
ORDER BY t.timestamp ASC
"""

# ============================================================================
# API3 - HOTSPOTS
# ============================================================================
GET_HOTSPOTS = """
SELECT
    g.grid_id,
    t.timestamp,
    SUM(f.call_in + f.call_out) AS call_activity,
    SUM(f.sms_in + f.sms_out) AS sms_activity,
    SUM(f.internet) AS internet_activity,
    SUM(f.total_activity) AS total_activity,
    r.risk_score,
    r.risk_level,
    r.model_version
FROM fact_network_activity AS f
INNER JOIN dim_time AS t
    ON f.time_key = t.time_key
INNER JOIN dim_grid AS g
    ON f.grid_key = g.grid_key
LEFT JOIN network_risk_scores AS r
    ON r.grid_id = g.grid_id
    AND r.timestamp = t.timestamp
WHERE t.timestamp = %s
GROUP BY
    g.grid_id,
    t.timestamp,
    r.risk_score,
    r.risk_level,
    r.model_version
ORDER BY
    total_activity DESC,
    g.grid_id ASC
LIMIT %s
"""

# ============================================================================
# API3 - ALERTS
# ============================================================================
GET_ALERTS = """
SELECT
    grid_id,
    timestamp,
    'ACTIVE' AS status,
    risk_level AS severity,
    'ML risk score detected.' AS reason,
    risk_score,
    risk_level,
    model_version
FROM network_risk_scores
WHERE timestamp <= %s
ORDER BY risk_score DESC
LIMIT %s
"""

GET_GRID_FEATURES = """
SELECT
    grid_id,
    avg_activity,
    activity_growth,
    active_hours,
    peak_ratio,
    variability,
    internet_share,
    feature_timestamp
FROM ml2_grid_features
WHERE grid_id = %s
  AND feature_timestamp BETWEEN %s AND %s
ORDER BY feature_timestamp
"""

GET_LATEST_FEATURE_TIMESTAMP = """
SELECT MAX(feature_timestamp) AS latest_feature_timestamp
FROM ml2_grid_features
"""