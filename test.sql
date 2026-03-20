--psql
CREATE TABLE public.test (
        trip_id BIGINT, 
        pickup_datetime TIMESTAMP WITHOUT TIME ZONE, 
        dropoff_datetime TIMESTAMP WITHOUT TIME ZONE, 
        pickup_longitude FLOAT(53), 
        pickup_latitude FLOAT(53), 
        dropoff_longitude FLOAT(53), 
        dropoff_latitude FLOAT(53), 
        passenger_count BIGINT, 
        trip_distance FLOAT(53), 
        fare_amount FLOAT(53), 
        extra FLOAT(53), 
        tip_amount FLOAT(53), 
        tolls_amount FLOAT(53), 
        total_amount FLOAT(53), 
        payment_type TEXT, 
        pickup_ntaname TEXT, 
        dropoff_ntaname TEXT
)

--clickhouse
CREATE TABLE default.test
(
    trip_id Int64,
    pickup_datetime DateTime,
    dropoff_datetime DateTime,
    pickup_longitude Float64,
    pickup_latitude Float64,
    dropoff_longitude Float64,
    dropoff_latitude Float64,
    passenger_count Int64,
    trip_distance Float64,
    fare_amount Float64,
    extra Float64,
    tip_amount Float64,
    tolls_amount Float64,
    total_amount Float64,
    payment_type String,
    pickup_ntaname String,
    dropoff_ntaname String
)
ENGINE = MergeTree()
ORDER BY trip_id;