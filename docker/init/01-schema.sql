CREATE SCHEMA IF NOT EXISTS ingestion;

CREATE TABLE IF NOT EXISTS ingestion.taxi_trip_raw (
    id BIGSERIAL PRIMARY KEY,

    source_file VARCHAR(255) NOT NULL,
    line_number BIGINT NOT NULL,

    vendor_id INTEGER,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count INTEGER,
    trip_distance NUMERIC(10,3),

    pickup_longitude NUMERIC(9,6),
    pickup_latitude NUMERIC(9,6),
    dropoff_longitude NUMERIC(9,6),
    dropoff_latitude NUMERIC(9,6),

    rate_code_id INTEGER,
    store_and_fwd_flag CHAR(1),
    payment_type INTEGER,

    fare_amount NUMERIC(10,2),
    extra NUMERIC(10,2),
    mta_tax NUMERIC(10,2),
    tip_amount NUMERIC(10,2),
    tolls_amount NUMERIC(10,2),
    improvement_surcharge NUMERIC(10,2),
    total_amount NUMERIC(10,2),

    ingested_at TIMESTAMP DEFAULT now(),

    CONSTRAINT uk_source_line UNIQUE (source_file, line_number)
);