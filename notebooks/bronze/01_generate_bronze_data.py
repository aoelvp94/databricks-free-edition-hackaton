# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze – Synthetic data generation

# MAGIC This notebook runs as part of the Bronze Lakeflow pipeline. It generates realistic synthetic data for
# MAGIC flight, hotel, and event quotes across Latin America to power the medallion architecture.

# COMMAND ----------

import random
from datetime import datetime, timedelta

import dlt
from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql import types as T

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters and catalogs

# MAGIC - `pipeline.defaultCatalog` / `pipeline.defaultSchema`: configured by the bundle.
# MAGIC - `bundle.synthetic_days`: number of future days to simulate (default 35).

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "1")

TARGET_CATALOG = spark.conf.get("pipeline.defaultCatalog", "hackathon")
TARGET_SCHEMA = spark.conf.get("pipeline.defaultSchema", "bronze_travel")
SYNTHETIC_DAYS = int(spark.conf.get("bundle.synthetic_days", "35"))
BASE_SEED = int(spark.conf.get("bundle.synthetic_seed", "202411"))
random.seed(BASE_SEED)

today = datetime.utcnow().date()
ingestion_ts = datetime.utcnow()
source_batch_id = f"bronze_{ingestion_ts.strftime('%Y%m%d%H%M')}"

ROUTES = [
    ("EZE", "SCL", "Buenos Aires", "Santiago", "Argentina", "Chile", 1140, "regional"),
    ("EZE", "GRU", "Buenos Aires", "Sao Paulo", "Argentina", "Brazil", 1680, "regional"),
    ("LIM", "BOG", "Lima", "Bogota", "Peru", "Colombia", 1885, "regional"),
    ("MEX", "BOG", "Mexico City", "Bogota", "Mexico", "Colombia", 3150, "international"),
    ("BOG", "MIA", "Bogota", "Miami", "Colombia", "United States", 3750, "international"),
    ("SCL", "LIM", "Santiago", "Lima", "Chile", "Peru", 2460, "regional"),
    ("MEX", "LIM", "Mexico City", "Lima", "Mexico", "Peru", 4230, "international"),
]

CARRIERS = [
    ("AR", "Aerolineas Argentinas", False, "SkyTeam"),
    ("LA", "LATAM Airlines", False, "Oneworld"),
    ("CM", "Copa Airlines", False, "Star Alliance"),
    ("AV", "Avianca", False, "Star Alliance"),
    ("H2", "Sky Airline", True, None),
    ("JA", "JetSMART", True, None),
    ("VB", "VivaAerobus", True, None),
]

PROPERTIES = [
    ("SCL-H001", "Bellavista Hotel", "Santiago", "Chile", 4),
    ("SCL-H002", "AeroSuites", "Santiago", "Chile", 3),
    ("LIM-H001", "Miraflores Deluxe", "Lima", "Peru", 5),
    ("LIM-H002", "Coastal Inn", "Lima", "Peru", 4),
    ("BOG-H001", "Andes Plaza", "Bogota", "Colombia", 4),
    ("MEX-H001", "Chapultepec Central", "Mexico City", "Mexico", 5),
    ("MEX-H002", "Historic Center Suites", "Mexico City", "Mexico", 3),
    ("GRU-H001", "Paulista Business", "Sao Paulo", "Brazil", 4),
]

EVENT_TYPES = [
    ("Festival", 0.18),
    ("Concert", 0.22),
    ("Marathon", 0.15),
    ("Soccer match", 0.25),
    ("National holiday", 0.35),
]

# COMMAND ----------

def _random_shock(prob: float, magnitude_range: tuple[float, float]) -> float:
    if random.random() <= prob:
        lower, upper = magnitude_range
        return random.uniform(lower, upper)
    return 0.0


def _sample_price(base: float, volatility: float, shock_prob: float, shock_range: tuple[float, float]) -> float:
    noise = random.gauss(0, volatility)
    shock = _random_shock(shock_prob, shock_range)
    return max(base + noise + shock, base * 0.4)


def _flight_rows() -> list[Row]:
    rows = []
    for day_delta in range(SYNTHETIC_DAYS):
        purchase_date = today + timedelta(days=day_delta)
        for route in ROUTES:
            for carrier in CARRIERS:
                departure_lead = random.choice([7, 14, 21, 30, 45, 60])
                departure_date = purchase_date + timedelta(days=departure_lead)
                route_id = f"{route[0]}-{route[1]}"
                base_price = 120 + (route[6] / 25) + random.uniform(10, 80)
                volatility = base_price * random.uniform(0.05, 0.15)
                shock_prob = 0.12 if carrier[2] else 0.05
                shock_range = (-base_price * 0.25, base_price * 0.45)
                price = round(_sample_price(base_price, volatility, shock_prob, shock_range), 2)
                baggage_fee = float(round(random.choice([0.0, 25.0, 35.0, 45.0]) if carrier[2] else random.choice([0.0, 20.0, 30.0]), 2))
                availability = round(random.uniform(0.2, 0.95), 2)
                promotion_flag = 1 if price < base_price * 0.8 else 0
                quote_id = f"{route_id}-{carrier[0]}-{purchase_date.strftime('%Y%m%d')}"
                rows.append(
                    Row(
                        quote_id=quote_id,
                        route_id=route_id,
                        origin_iata=route[0],
                        destination_iata=route[1],
                        origin_city=route[2],
                        destination_city=route[3],
                        origin_country=route[4],
                        destination_country=route[5],
                        distance_km=route[6],
                        route_category=route[7],
                        carrier_code=carrier[0],
                        carrier_name=carrier[1],
                        is_low_cost=carrier[2],
                        alliance=carrier[3],
                        purchase_date=purchase_date,
                        departure_date=departure_date,
                        lead_time_days=departure_lead,
                        base_price_usd=round(base_price, 2),
                        total_price_usd=price,
                        baggage_fee_usd=baggage_fee,
                        availability_ratio=availability,
                        promotion_flag=promotion_flag,
                        tax_rate=round(random.uniform(0.13, 0.19), 3),
                        fuel_surcharge_usd=round(random.uniform(8, 25), 2),
                        ingestion_ts=ingestion_ts,
                        source_batch_id=source_batch_id,
                        source_system="synthetic_generator_v1",
                    )
                )
    return rows


def _hotel_rows() -> list[Row]:
    rows = []
    for day_delta in range(SYNTHETIC_DAYS):
        search_date = today + timedelta(days=day_delta)
        stay_length = random.choice([2, 3, 4, 5, 7])
        checkin_date = search_date + timedelta(days=random.choice([10, 15, 30, 45]))
        checkout_date = checkin_date + timedelta(days=stay_length)
        for property_id, name, city, country, stars in PROPERTIES:
            base_rate = 80 + stars * 25 + random.uniform(0, 50)
            volatility = base_rate * random.uniform(0.05, 0.22)
            shock_prob = 0.08 if stars >= 4 else 0.14
            shock_range = (-base_rate * 0.2, base_rate * 0.35)
            nightly_price = round(_sample_price(base_rate, volatility, shock_prob, shock_range), 2)
            availability = round(random.uniform(0.15, 0.98), 2)
            total_price = round(nightly_price * stay_length, 2)
            quote_id = f"{property_id}-{search_date.strftime('%Y%m%d')}"
            rows.append(
                Row(
                    quote_id=quote_id,
                    property_id=property_id,
                    property_name=name,
                    city=city,
                    country=country,
                    stars=stars,
                    search_date=search_date,
                    checkin_date=checkin_date,
                    checkout_date=checkout_date,
                    stay_length_nights=stay_length,
                    nightly_price_usd=nightly_price,
                    total_price_usd=total_price,
                    availability_ratio=availability,
                    cancellation_policy=random.choice(["flexible", "moderate", "estricta"]),
                    review_score=round(random.uniform(3.5, 4.9), 2),
                    promotion_flag=1 if nightly_price < base_rate * 0.78 else 0,
                    ingestion_ts=ingestion_ts,
                    source_batch_id=source_batch_id,
                    source_system="synthetic_generator_v1",
                )
            )
    return rows


def _event_rows() -> list[Row]:
    rows = []
    for route in ROUTES:
        city = route[3]
        for _ in range(random.randint(1, 4)):
            start_offset = random.randint(3, SYNTHETIC_DAYS + 20)
            duration_days = random.choice([1, 2, 3, 7])
            event_date = today + timedelta(days=start_offset)
            event_type, uplift = random.choice(EVENT_TYPES)
            rows.append(
                Row(
                    event_id=f"{city[:3].upper()}-{event_date.strftime('%Y%m%d')}-{event_type[:2].upper()}",
                    city=city,
                    country=route[5],
                    event_type=event_type,
                    expected_uplift=round(uplift, 2),
                    start_date=event_date,
                    end_date=event_date + timedelta(days=duration_days),
                    description=f"{event_type} in {city}",
                    ingestion_ts=ingestion_ts,
                    source_batch_id=source_batch_id,
                    source_system="synthetic_generator_v1",
                )
            )
    return rows


def _create_df(rows: list[Row], schema: T.StructType) -> DataFrame:
    return spark.createDataFrame(rows, schema=schema)


# COMMAND ----------

flight_schema = T.StructType(
    [
        T.StructField("quote_id", T.StringType(), False),
        T.StructField("route_id", T.StringType(), False),
        T.StructField("origin_iata", T.StringType(), False),
        T.StructField("destination_iata", T.StringType(), False),
        T.StructField("origin_city", T.StringType(), False),
        T.StructField("destination_city", T.StringType(), False),
        T.StructField("origin_country", T.StringType(), False),
        T.StructField("destination_country", T.StringType(), False),
        T.StructField("distance_km", T.IntegerType(), False),
        T.StructField("route_category", T.StringType(), False),
        T.StructField("carrier_code", T.StringType(), False),
        T.StructField("carrier_name", T.StringType(), False),
        T.StructField("is_low_cost", T.BooleanType(), False),
        T.StructField("alliance", T.StringType(), True),
        T.StructField("purchase_date", T.DateType(), False),
        T.StructField("departure_date", T.DateType(), False),
        T.StructField("lead_time_days", T.IntegerType(), False),
        T.StructField("base_price_usd", T.DoubleType(), False),
        T.StructField("total_price_usd", T.DoubleType(), False),
        T.StructField("baggage_fee_usd", T.DoubleType(), False),
        T.StructField("availability_ratio", T.DoubleType(), False),
        T.StructField("promotion_flag", T.IntegerType(), False),
        T.StructField("tax_rate", T.DoubleType(), False),
        T.StructField("fuel_surcharge_usd", T.DoubleType(), False),
        T.StructField("ingestion_ts", T.TimestampType(), False),
        T.StructField("source_batch_id", T.StringType(), False),
        T.StructField("source_system", T.StringType(), False),
    ]
)

hotel_schema = T.StructType(
    [
        T.StructField("quote_id", T.StringType(), False),
        T.StructField("property_id", T.StringType(), False),
        T.StructField("property_name", T.StringType(), False),
        T.StructField("city", T.StringType(), False),
        T.StructField("country", T.StringType(), False),
        T.StructField("stars", T.IntegerType(), False),
        T.StructField("search_date", T.DateType(), False),
        T.StructField("checkin_date", T.DateType(), False),
        T.StructField("checkout_date", T.DateType(), False),
        T.StructField("stay_length_nights", T.IntegerType(), False),
        T.StructField("nightly_price_usd", T.DoubleType(), False),
        T.StructField("total_price_usd", T.DoubleType(), False),
        T.StructField("availability_ratio", T.DoubleType(), False),
        T.StructField("cancellation_policy", T.StringType(), False),
        T.StructField("review_score", T.DoubleType(), False),
        T.StructField("promotion_flag", T.IntegerType(), False),
        T.StructField("ingestion_ts", T.TimestampType(), False),
        T.StructField("source_batch_id", T.StringType(), False),
        T.StructField("source_system", T.StringType(), False),
    ]
)

event_schema = T.StructType(
    [
        T.StructField("event_id", T.StringType(), False),
        T.StructField("city", T.StringType(), False),
        T.StructField("country", T.StringType(), False),
        T.StructField("event_type", T.StringType(), False),
        T.StructField("expected_uplift", T.DoubleType(), False),
        T.StructField("start_date", T.DateType(), False),
        T.StructField("end_date", T.DateType(), False),
        T.StructField("description", T.StringType(), False),
        T.StructField("ingestion_ts", T.TimestampType(), False),
        T.StructField("source_batch_id", T.StringType(), False),
        T.StructField("source_system", T.StringType(), False),
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## DLT tables

# MAGIC Three Bronze tables are produced. Each table applies explicit deduplication and appends technical columns that already exist in the synthetic dataset.

# COMMAND ----------

@dlt.table(
    name="flight_quotes_raw",
    comment="Synthetic LATAM flight quotes with controlled noise and technical metadata.",
    table_properties={"quality": "bronze", "data_domain": "travel"},
)
def flight_quotes_raw() -> DataFrame:
    df = _create_df(_flight_rows(), flight_schema)
    return (
        df.dropDuplicates(["quote_id", "carrier_code", "purchase_date"])
        .withColumn("ingestion_date", F.to_date("ingestion_ts"))
        .withColumn("ingestion_file", F.lit("synthetic_generator"))
        .withColumn("baggage_fee_usd", F.col("baggage_fee_usd").cast("double"))
        .withColumn("total_price_usd", F.col("total_price_usd").cast("double"))
        .withColumn("base_price_usd", F.col("base_price_usd").cast("double"))
        .withColumn("fuel_surcharge_usd", F.col("fuel_surcharge_usd").cast("double"))
        .withColumn("availability_ratio", F.col("availability_ratio").cast("double"))
        .withColumn("tax_rate", F.col("tax_rate").cast("double"))
    )


@dlt.table(
    name="hotel_quotes_raw",
    comment="Synthetic LATAM hotel quotes with noise and estimated availability.",
    table_properties={"quality": "bronze", "data_domain": "travel"},
)
def hotel_quotes_raw() -> DataFrame:
    df = _create_df(_hotel_rows(), hotel_schema)
    return (
        df.dropDuplicates(["quote_id", "property_id", "search_date"])
        .withColumn("ingestion_date", F.to_date("ingestion_ts"))
        .withColumn("ingestion_file", F.lit("synthetic_generator"))
    )


@dlt.table(
    name="event_calendar_raw",
    comment="Synthetic event calendar that explains demand variations.",
    table_properties={"quality": "bronze", "data_domain": "travel"},
)
def event_calendar_raw() -> DataFrame:
    df = _create_df(_event_rows(), event_schema)
    return (
        df.dropDuplicates(["event_id"])
        .withColumn("ingestion_date", F.to_date("ingestion_ts"))
        .withColumn("ingestion_file", F.lit("synthetic_generator"))
    )


