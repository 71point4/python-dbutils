import polars as pl
from decouple import config
from dbutils import Query
import time
import logging


###########
# Logging #
###########
log = logging.getLogger(__name__)

def setup_logger():
    logger = logging.getLogger("dbutils")
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    ch.setFormatter(formatter)
    logger.addHandler(ch)


setup_logger()

###############
# Connections #
###############

# PostgreSQL connection 
psql = Query(
    db_type="psql",
    db="analytic",
    db_host=config("db_psql_host"),
    db_port=config("db_psql_port"),
    db_user=config("db_psql_user"),
    db_pass=config("db_psql_pass"),
)


# ClickHouse connection
clickhouse = Query(
    db_type="clickhouse",
    db="default",
    db_host=config("db_clickhouse_host"),
    db_port=config("db_clickhouse_port"),
    db_user=config("db_clickhouse_user"),
    db_pass=config("db_clickhouse_pass"),
)

#########
# Tests #
#########


# Test connection
psql.sql_query("SELECT CURRENT_DATE")

# Query examples
df = clickhouse.sql_query("SELECT * FROM nyc_taxi.trips_small LIMIT 3e6")

# Write test to PostgreSQL

# Example dataframe
excel = pl.read_excel("test.xlsx", sheet_id=2).rename(
    {"[ASISA-EQUB-FFAATA] Asset Allocation": "value"}
)

start = time.time()

psql.sql_write(
    excel,
    schema="public",
    table_name="excel",
    max_chunk=10000,
    max_workers=4,
)

end = time.time()
print("Postgres write:", end - start)
psql.sql_query("TRUNCATE excel")

# Test 2 
# * Recommend with parallel to create table manually

psql.sql_query("SELECT COUNT(*) FROM test")
start = time.time()
psql.sql_write(
    df,
    schema="public",
    table_name="test",
    max_chunk=100000,
    max_workers=5,
)
end = time.time()
print("PSQL write:", end - start)
psql.sql_query("TRUNCATE test")

# Write test to ClickHouse

clickhouse.sql_query("SELECT COUNT(*) FROM test")
clickhouse.sql_query("TRUNCATE test")
clickhouse.sql_query("SELECT COUNT(*) FROM test")

start = time.time()
clickhouse.sql_write(
    df,
    schema="default",
    table_name="test",
    max_chunk=100000,
    max_workers=5,
)
end = time.time()
print("ClickHouse write:", end - start)

################
# Taxi Example #
################

clickhouse.sql_query("SELECT COUNT(*) FROM nyc_taxi.trips_small")

clickhouse.sql_query(
    """
    SELECT
        trip_id,
        trip_distance,
        pickup_datetime,
        dropoff_datetime
    FROM nyc_taxi.trips_small
    ORDER BY trip_distance DESC
    LIMIT 10;
    """)

clickhouse.sql_query(
    """
    SELECT avg(trip_distance) AS avg_distance
    FROM nyc_taxi.trips_small;
    """)

clickhouse.sql_query(
    """
    SELECT
        round(trip_distance, 1) AS distance_bucket,
        count() AS trips
    FROM nyc_taxi.trips_small
    GROUP BY distance_bucket
    ORDER BY distance_bucket;
    """)

clickhouse.sql_query(
    """
    SELECT
        quantile(0.5)(trip_distance) AS median_trip_distance
    FROM nyc_taxi.trips_small;
    """)

# Time analysis

clickhouse.sql_query(
    """
   SELECT
        avg(dateDiff('minute', pickup_datetime, dropoff_datetime)) AS avg_minutes
   FROM nyc_taxi.trips_small;
   """)

clickhouse.sql_query(
    """
   SELECT
        trip_id,
        trip_distance,
        dateDiff('second', pickup_datetime, dropoff_datetime) AS duration_seconds,
        trip_distance / (duration_seconds / 3600) AS speed_mph
    FROM nyc_taxi.trips_small
    WHERE duration_seconds > 0
    ORDER BY speed_mph DESC
    LIMIT 10;
   """)

# Geospatial 

clickhouse.sql_query(
    """
   SELECT
        trip_id,
        geoDistance(
            pickup_longitude,
            pickup_latitude,
            dropoff_longitude,
            dropoff_latitude
        ) AS meters
    FROM nyc_taxi.trips_small
    ORDER BY meters DESC
    LIMIT 10;
   """)

# Geo lcustering
clickhouse.sql_query(
    """
    SELECT
        geohashEncode(pickup_longitude, pickup_latitude, 6) AS cell,
        count() AS trips
    FROM nyc_taxi.trips_small
    GROUP BY cell
    ORDER BY trips DESC
    LIMIT 20;
   """)

# Get cluster centers
clickhouse.sql_query(
    """
    SELECT
        geohashEncode(pickup_longitude, pickup_latitude, 6) AS cell,
        avg(pickup_latitude) AS lat,
        avg(pickup_longitude) AS lon,
        count() AS trips
    FROM nyc_taxi.trips_small
    GROUP BY cell
    ORDER BY trips DESC
    LIMIT 20;
   """)

# Distance to Time square
clickhouse.sql_query(
    """
    SELECT
        trip_id,
        geoDistance(
            pickup_longitude,
            pickup_latitude,
            -73.9855,
            40.7580
        ) AS meters_from_times_square
    FROM nyc_taxi.trips_small
    ORDER BY meters_from_times_square
    LIMIT 20;
   """)



# This finds trips that start near Times Square look like a $15 2.5-mile ride
clickhouse.sql_query(
    """
    SELECT
        trip_id,
        geoDistance(
            pickup_longitude,
            pickup_latitude,
            -73.9855,
            40.7580
        ) AS meters_from_times_square,
        cosineDistance(
            [trip_distance, total_amount],
            [2.5, 15]
        ) AS similarity
    FROM nyc_taxi.trips_small
    ORDER BY similarity
    LIMIT 20;
   """)

# This finds trips that:
# distance = 5 miles
# duration = 15 minutes
# 2 passengers
# $20 fare

clickhouse.sql_query(
    """
    SELECT
        trip_id,
        cosineDistance(
            [
                trip_distance,
                dateDiff('minute', pickup_datetime, dropoff_datetime),
                passenger_count,
                total_amount
            ],
            [5, 15, 2, 20]
        ) AS similarity
    FROM nyc_taxi.trips_small
    ORDER BY similarity ASC
    LIMIT 10;
   """)

# Nearest Neighbours:
clickhouse.sql_query(
    """
    SELECT
        trip_id,
        trip_distance,
        cosineDistance(
            [
                trip_distance,
                passenger_count,
                total_amount
            ],
            [3.0, 1, 12.0]
        ) AS distance
    FROM nyc_taxi.trips_small
    ORDER BY distance
    LIMIT 10;
   """)

# Find the most typical taxi ride in NYC using vector distance.

# The idea:
# - Compute the average ride characteristics
# - Treat each ride as a feature vector
# - Find the ride with the smallest cosine distance to the average



clickhouse.sql_query(
    """
    SELECT
        avg(trip_distance) AS avg_distance,
        avg(total_amount) AS avg_fare,
        avg(passenger_count) AS avg_passengers,
        avg(dateDiff('minute', pickup_datetime, dropoff_datetime)) AS avg_duration
    FROM nyc_taxi.trips_small;
   """)

# Most typical rides (vector comparison)

clickhouse.sql_query(
    """
    SELECT
        trip_id,
        trip_distance,
        total_amount,
        passenger_count,
        dateDiff('minute', pickup_datetime, dropoff_datetime) AS duration_minutes
    FROM nyc_taxi.trips_small
    ORDER BY cosineDistance(
        [
            toFloat64(trip_distance),
            toFloat64(total_amount),
            toFloat64(passenger_count),
            toFloat64(dateDiff('minute', pickup_datetime, dropoff_datetime))
        ],
        [
            toFloat64(avg(trip_distance) OVER ()),
            toFloat64(avg(total_amount) OVER ()),
            toFloat64(avg(passenger_count) OVER ()),
            toFloat64(avg(dateDiff('minute', pickup_datetime, dropoff_datetime)) OVER ())
        ]
    )
    LIMIT 10;
   """)


