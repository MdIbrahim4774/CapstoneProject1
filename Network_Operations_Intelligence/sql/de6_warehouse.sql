CREATE DATABASE IF NOT EXISTS network_operations;

USE network_operations;


-- ============================================================
-- DIMENSION: GRID
-- One row per Milan grid
-- ============================================================

CREATE TABLE IF NOT EXISTS dim_grid (
    grid_key INT AUTO_INCREMENT PRIMARY KEY,

    grid_id VARCHAR(50) NOT NULL UNIQUE,

    centroid_latitude DECIMAL(10, 7),
    centroid_longitude DECIMAL(10, 7),

    geometry_ref VARCHAR(255),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- ============================================================
-- DIMENSION: TIME
-- One row per unique hourly timestamp
-- ============================================================

CREATE TABLE IF NOT EXISTS dim_time (
    time_key BIGINT PRIMARY KEY,

    timestamp DATETIME NOT NULL UNIQUE,

    date DATE NOT NULL,
    hour TINYINT NOT NULL,

    day_of_week TINYINT NOT NULL,
    day_name VARCHAR(10) NOT NULL,

    month TINYINT NOT NULL,
    year SMALLINT NOT NULL
);


-- ============================================================
-- FACT: NETWORK ACTIVITY
--
-- Grain:
--     one grid + one hour
-- ============================================================

CREATE TABLE IF NOT EXISTS fact_network_activity (
    activity_key BIGINT AUTO_INCREMENT PRIMARY KEY,

    grid_key INT NOT NULL,
    time_key BIGINT NOT NULL,

    sms_in DOUBLE NOT NULL DEFAULT 0,
    sms_out DOUBLE NOT NULL DEFAULT 0,

    call_in DOUBLE NOT NULL DEFAULT 0,
    call_out DOUBLE NOT NULL DEFAULT 0,

    internet DOUBLE NOT NULL DEFAULT 0,

    total_activity DOUBLE NOT NULL DEFAULT 0,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_fact_grid
        FOREIGN KEY (grid_key)
        REFERENCES dim_grid(grid_key),

    CONSTRAINT fk_fact_time
        FOREIGN KEY (time_key)
        REFERENCES dim_time(time_key),

    CONSTRAINT uq_fact_grid_time
        UNIQUE (grid_key, time_key)
);


-- ============================================================
-- INDEXES
-- ============================================================

CREATE INDEX idx_fact_grid
    ON fact_network_activity(grid_key);

CREATE INDEX idx_fact_time
    ON fact_network_activity(time_key);

CREATE INDEX idx_fact_grid_time
    ON fact_network_activity(grid_key, time_key);

CREATE INDEX idx_time_date
    ON dim_time(date);

CREATE INDEX idx_time_hour
    ON dim_time(hour);

CREATE INDEX idx_grid_grid_id
    ON dim_grid(grid_id);