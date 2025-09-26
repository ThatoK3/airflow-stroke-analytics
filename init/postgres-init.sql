-- Analytics cache tables for stroke predictions
CREATE TABLE IF NOT EXISTS stroke_analytics_op_counts (
    op VARCHAR(10) PRIMARY KEY,
    count BIGINT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stroke_analytics_risk_distribution (
    risk_category VARCHAR(10) PRIMARY KEY,
    pct DECIMAL(5,2),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stroke_analytics_probability_bins (
    prob_bucket VARCHAR(20) PRIMARY KEY,
    total BIGINT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stroke_analytics_risk_by_gender (
    gender VARCHAR(10) PRIMARY KEY,
    high_risk BIGINT,
    medium_risk BIGINT,
    low_risk BIGINT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stroke_analytics_risk_by_age_group (
    age_group VARCHAR(20),
    risk_category VARCHAR(10),
    total BIGINT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (age_group, risk_category)
);

CREATE TABLE IF NOT EXISTS stroke_analytics_province_hotspots (
    province VARCHAR(50) PRIMARY KEY,
    high_risk BIGINT,
    medium_risk BIGINT,
    low_risk BIGINT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stroke_analytics_hypertension_heart_correlation (
    hypertension INT,
    heart_disease INT,
    low_risk BIGINT,
    low_pct DECIMAL(5,2),
    medium_risk BIGINT,
    medium_pct DECIMAL(5,2),
    high_risk BIGINT,
    high_pct DECIMAL(5,2),
    total BIGINT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (hypertension, heart_disease)
);

CREATE TABLE IF NOT EXISTS stroke_analytics_bmi_vs_risk (
    bmi_category VARCHAR(20),
    risk_category VARCHAR(10),
    total BIGINT,
    pct DECIMAL(5,2),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (bmi_category, risk_category)
);

CREATE TABLE IF NOT EXISTS stroke_analytics_glucose_risk_bands (
    glucose_category VARCHAR(20),
    risk_category VARCHAR(10),
    total BIGINT,
    pct DECIMAL(5,2),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (glucose_category, risk_category)
);