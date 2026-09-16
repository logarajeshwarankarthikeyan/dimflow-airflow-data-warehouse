DROP_TABLES = """
DROP TABLE IF EXISTS fact_episode;
DROP TABLE IF EXISTS dim_country;
DROP TABLE IF EXISTS dim_show;
"""



DIM_COUNTRY  = """ 
CREATE TABLE dim_country (
    country_id INTEGER AUTO_INCREMENT PRIMARY KEY,
    country_name VARCHAR(100) UNIQUE
);
"""


DIM_SHOW = """
CREATE TABLE dim_show (
    show_id INTEGER AUTO_INCREMENT PRIMARY KEY,
    tvmaze_id INTEGER UNIQUE,
    name VARCHAR(255)
);
""" 
STG_COUNTRY = """CREATE TEMPORARY TABLE stg_country (
    load_batch_id CHAR(36) NOT NULL,
    loaded_at_utc DATETIME(6) NOT NULL,
    country_name VARCHAR(100) NOT NULL,
    PRIMARY KEY (load_batch_id, country_name)
);"""


STG_SHOW = """CREATE TEMPORARY TABLE stg_show (
    load_batch_id CHAR(36) NOT NULL,
    loaded_at_utc DATETIME(6) NOT NULL,
    tvmaze_id INTEGER NOT NULL,
    name VARCHAR(255),
    PRIMARY KEY (load_batch_id, tvmaze_id)
);"""
