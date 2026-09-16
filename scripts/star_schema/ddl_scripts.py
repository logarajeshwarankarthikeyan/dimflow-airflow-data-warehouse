DROP_TABLES = """
DROP TABLE IF EXISTS fact_weather;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_city;
"""


DIM_CITY = """CREATE TABLE dim_city (
    city_id INT PRIMARY KEY,
    city_name VARCHAR(255),
    country_code VARCHAR(10),
    latitude FLOAT,
    longitude FLOAT
);"""


DIM_DATE = """ CREATE TABLE dim_date (
    date_id DATE PRIMARY KEY,
    year INT,
    month INT,
    day INT,
    day_of_week INT,
    is_weekend BOOLEAN
);"""


FACT_WEATHER = """CREATE TABLE fact_weather (
    weather_id INT AUTO_INCREMENT PRIMARY KEY,
    city_id INT,
    date_id DATE,
    temperature FLOAT,
    humidity FLOAT,
    FOREIGN KEY (city_id) REFERENCES dim_city(city_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
);"""


STG_CITY = """CREATE TEMPORARY TABLE stg_city (
    load_batch_id CHAR(36) NOT NULL,
    loaded_at_utc DATETIME(6) NOT NULL,
    city_id INT NOT NULL,
    city_name VARCHAR(255),
    country_code VARCHAR(10),
    latitude FLOAT,
    longitude FLOAT,
    PRIMARY KEY (load_batch_id, city_id)
);"""

STG_DATE = """CREATE TEMPORARY TABLE stg_date (
    load_batch_id CHAR(36) NOT NULL,
    loaded_at_utc DATETIME(6) NOT NULL,
    date_id DATE NOT NULL,
    year INT,
    month INT,
    day INT,
    day_of_week INT,
    is_weekend BOOLEAN,
    PRIMARY KEY (load_batch_id, date_id)
);"""

STG_WEATHER = """CREATE TEMPORARY TABLE stg_weather (
    load_batch_id CHAR(36) NOT NULL,
    loaded_at_utc DATETIME(6) NOT NULL,
    city_id INT NOT NULL,
    date_id DATE NOT NULL,
    temperature FLOAT,
    humidity FLOAT,
    PRIMARY KEY (load_batch_id, city_id, date_id)
);"""
