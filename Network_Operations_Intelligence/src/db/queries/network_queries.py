"""
SQL queries for network analytics.
"""
GET_MAX_TIMESTAMP = """
SELECT MAX(timestamp) AS as_of
FROM dim_time
"""

GET_NETWORK_SUMMARY = """
WITH grid_hour AS (
    SELECT
        f.grid_key,
        g.grid_id,
        t.hour,
        SUM(f.total_activity) AS activity
    FROM fact_network_activity AS f
    INNER JOIN dim_time AS t
        ON f.time_key = t.time_key
    INNER JOIN dim_grid AS g
        ON f.grid_key = g.grid_key
    WHERE t.timestamp <= %s
    GROUP BY f.grid_key, g.grid_id, t.hour
),
grid_totals AS (
    SELECT
        grid_key,
        grid_id,
        SUM(activity) AS activity
    FROM grid_hour
    GROUP BY grid_key, grid_id
),
hour_totals AS (
    SELECT
        hour,
        SUM(activity) AS activity
    FROM grid_hour
    GROUP BY hour
)
SELECT
    (
        SELECT COALESCE(SUM(activity), 0)
        FROM grid_hour
    ) AS total_activity,
    (
        SELECT COUNT(*)
        FROM grid_totals
        WHERE activity > 0
    ) AS active_grids,
    (
        SELECT hour
        FROM hour_totals
        ORDER BY activity DESC, hour ASC
        LIMIT 1
    ) AS peak_hour,
    (
        SELECT grid_id
        FROM grid_totals
        ORDER BY activity DESC, grid_id ASC
        LIMIT 1
    ) AS top_grid
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
    SUM(f.total_activity) AS total_activity
FROM fact_network_activity AS f
INNER JOIN dim_time AS t
    ON f.time_key = t.time_key
INNER JOIN dim_grid AS g
    ON f.grid_key = g.grid_key
WHERE t.timestamp = %s
GROUP BY
    g.grid_id,
    t.timestamp
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
    g.grid_id,
    t.timestamp,
    SUM(f.call_in + f.call_out) AS call_activity,
    SUM(f.sms_in + f.sms_out) AS sms_activity,
    SUM(f.internet) AS internet_activity,
    SUM(f.total_activity) AS total_activity,
    'ACTIVE' AS status,
    'HIGH' AS severity,
    'Rule-based activity alert detected.' AS reason
FROM fact_network_activity AS f
INNER JOIN dim_time AS t
    ON f.time_key = t.time_key
INNER JOIN dim_grid AS g
    ON f.grid_key = g.grid_key
WHERE t.timestamp = %s
GROUP BY
    g.grid_id,
    t.timestamp
HAVING SUM(f.total_activity) > 0
ORDER BY
    total_activity DESC,
    g.grid_id ASC
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