# Databricks notebook source
# MAGIC %md
# MAGIC # Silver – Entity-relationship model

# MAGIC This notebook runs as a Lakeflow (DLT) pipeline on top of the Bronze layer. It normalizes,
# MAGIC validates, and models the data into dimensions and facts ready for analytics and ML.

# COMMAND ----------

from datetime import datetime

import dlt
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# MAGIC The pipeline expects:
# MAGIC - `pipeline.defaultCatalog` (set by the bundle).
# MAGIC - `source.bronze_schema` containing the Bronze schema name.

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "4")

TARGET_CATALOG = spark.conf.get("pipeline.defaultCatalog", "workspace")
BRONZE_SCHEMA = spark.conf.get("source.bronze_schema", "bronze_travel")


def read_bronze(table_name: str) -> DataFrame:
    full_name = f"{TARGET_CATALOG}.{BRONZE_SCHEMA}.{table_name}"
    return spark.read.table(full_name)


def surrogate_key(*cols) -> F.Column:
    return F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in cols]), 256)


LOAD_TS = F.current_timestamp()
LOAD_DATE = F.to_date(LOAD_TS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Staging and quality rules

# COMMAND ----------

@dlt.table(
    name="stg_flight_quotes",
    comment="Flight staging view with quality rules applied.",
    table_properties={"quality": "silver", "source_layer": "bronze"},
)
@dlt.expect_all_or_drop(
    {
        "total_price_positive": "total_price_usd > 0",
        "availability_bounds": "availability_ratio BETWEEN 0 AND 1",
        "valid_dates": "purchase_date <= departure_date",
    }
)
def stg_flight_quotes() -> DataFrame:
    df = read_bronze("flight_quotes_raw")
    return (
        df.withColumn("quote_sk", surrogate_key("quote_id", "carrier_code", "purchase_date"))
        .withColumn("record_valid_from", LOAD_TS)
        .withColumn("record_valid_to", F.lit(None).cast(T.TimestampType()))
    )


@dlt.table(
    name="stg_hotel_quotes",
    comment="Hotel staging view with quality controls and metadata.",
    table_properties={"quality": "silver", "source_layer": "bronze"},
)
@dlt.expect_all_or_drop(
    {
        "nightly_price_positive": "nightly_price_usd > 0",
        "availability_bounds": "availability_ratio BETWEEN 0 AND 1",
        "valid_dates": "search_date <= checkin_date",
    }
)
def stg_hotel_quotes() -> DataFrame:
    df = read_bronze("hotel_quotes_raw")
    return (
        df.withColumn("quote_sk", surrogate_key("quote_id", "property_id", "search_date"))
        .withColumn("record_valid_from", LOAD_TS)
        .withColumn("record_valid_to", F.lit(None).cast(T.TimestampType()))
    )


@dlt.table(
    name="stg_events",
    comment="External events staging view.",
    table_properties={"quality": "silver", "source_layer": "bronze"},
)
def stg_events() -> DataFrame:
    df = read_bronze("event_calendar_raw")
    return (
        df.withColumn("event_sk", surrogate_key("event_id", "city", "start_date"))
        .withColumn("record_valid_from", LOAD_TS)
        .withColumn("record_valid_to", F.lit(None).cast(T.TimestampType()))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensions

# COMMAND ----------

@dlt.table(
    name="dim_route",
    comment="LATAM routes with country, distance, and category attributes.",
    table_properties={"quality": "silver", "data_domain": "travel"},
)
def dim_route() -> DataFrame:
    df = dlt.read("stg_flight_quotes")
    return (
        df.select(
            surrogate_key("route_id").alias("route_sk"),
            "route_id",
            "origin_iata",
            "destination_iata",
            "origin_city",
            "destination_city",
            "origin_country",
            "destination_country",
            "distance_km",
            "route_category",
        )
        .dropDuplicates(["route_id"])
        .withColumn("record_valid_from", LOAD_TS)
        .withColumn("record_valid_to", F.lit(None).cast(T.TimestampType()))
    )


@dlt.table(
    name="dim_carrier",
    comment="Airline dimension with alliance details.",
    table_properties={"quality": "silver", "data_domain": "travel"},
)
def dim_carrier() -> DataFrame:
    df = dlt.read("stg_flight_quotes")
    return (
        df.select(
            surrogate_key("carrier_code").alias("carrier_sk"),
            "carrier_code",
            "carrier_name",
            "is_low_cost",
            "alliance",
        )
        .dropDuplicates(["carrier_code"])
        .withColumn("record_valid_from", LOAD_TS)
        .withColumn("record_valid_to", F.lit(None).cast(T.TimestampType()))
    )


@dlt.table(
    name="dim_property",
    comment="Hotel and property dimension with core attributes.",
    table_properties={"quality": "silver", "data_domain": "travel"},
)
def dim_property() -> DataFrame:
    df = dlt.read("stg_hotel_quotes")
    return (
        df.select(
            surrogate_key("property_id").alias("property_sk"),
            "property_id",
            "property_name",
            "city",
            "country",
            "stars",
        )
        .dropDuplicates(["property_id"])
        .withColumn("record_valid_from", LOAD_TS)
        .withColumn("record_valid_to", F.lit(None).cast(T.TimestampType()))
    )


@dlt.table(
    name="dim_time",
    comment="Time dimension with granularities and seasonality flags.",
    table_properties={"quality": "silver", "data_domain": "travel"},
)
def dim_time() -> DataFrame:
    flight_dates = dlt.read("stg_flight_quotes").select("purchase_date", "departure_date")
    hotel_dates = dlt.read("stg_hotel_quotes").select(
        F.col("search_date").alias("purchase_date"),
        F.col("checkin_date").alias("departure_date"),
    )

    calendar_df = (
        flight_dates.unionByName(hotel_dates)
        .select(
            F.explode(
                F.array(F.col("purchase_date"), F.col("departure_date"))
            ).alias("calendar_date")
        )
        .dropna()
        .dropDuplicates()
    )

    return (
        calendar_df.select(
            surrogate_key("calendar_date").alias("date_sk"),
            F.col("calendar_date").alias("date"),
            F.dayofweek("calendar_date").alias("day_of_week"),
            F.weekofyear("calendar_date").alias("week_of_year"),
            F.month("calendar_date").alias("month"),
            F.year("calendar_date").alias("year"),
            (F.dayofweek("calendar_date").isin(1, 7)).alias("is_weekend"),
            F.date_format("calendar_date", "MMMM").alias("month_name"),
            F.when(F.month("calendar_date").isin(12, 1, 2), "alta")
            .when(F.month("calendar_date").isin(6, 7), "media")
            .otherwise("baja")
            .alias("seasonality_band"),
        )
        .withColumn("record_valid_from", LOAD_TS)
        .withColumn("record_valid_to", F.lit(None).cast(T.TimestampType()))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Facts

# COMMAND ----------

@dlt.table(
    name="fact_flight_quote",
    comment="Flight quote fact with metrics and foreign keys.",
    table_properties={"quality": "silver", "data_domain": "travel", "dataset_purpose": "analytical_fact"},
    partition_cols=["departure_date"],
)
def fact_flight_quote() -> DataFrame:
    flights = dlt.read("stg_flight_quotes")
    route_dim = dlt.read("dim_route").select("route_sk", "route_id")
    carrier_dim = dlt.read("dim_carrier").select("carrier_sk", "carrier_code")
    time_dim = dlt.read("dim_time").select(
        F.col("date_sk"),
        F.col("date").alias("calendar_date"),
    )

    return (
        flights.join(route_dim, "route_id", "left")
        .join(carrier_dim, "carrier_code", "left")
        .join(
            time_dim.alias("purchase_time"),
            flights.purchase_date == F.col("purchase_time.calendar_date"),
            "left",
        )
        .withColumn("purchase_date_sk", F.col("purchase_time.date_sk"))
        .join(
            time_dim.alias("departure_time"),
            flights.departure_date == F.col("departure_time.calendar_date"),
            "left",
        )
        .withColumn("departure_date_sk", F.col("departure_time.date_sk"))
        .drop(
            F.col("purchase_time.calendar_date"),
            F.col("purchase_time.date_sk"),
            F.col("departure_time.calendar_date"),
            F.col("departure_time.date_sk"),
        )
        .withColumn("gross_price_usd", F.col("total_price_usd") + F.col("baggage_fee_usd"))
        .withColumn("yield_per_km", F.col("total_price_usd") / F.col("distance_km"))
        .withColumn("load_factor_signal", F.round(F.col("availability_ratio"), 2))
    )


@dlt.table(
    name="fact_hotel_quote",
    comment="Hotel quote fact with pricing and availability metrics.",
    table_properties={"quality": "silver", "data_domain": "travel", "dataset_purpose": "analytical_fact"},
    partition_cols=["checkin_date"],
)
def fact_hotel_quote() -> DataFrame:
    hotels = dlt.read("stg_hotel_quotes")
    property_dim = dlt.read("dim_property").select("property_sk", "property_id")
    time_dim = dlt.read("dim_time").select(
        F.col("date_sk"),
        F.col("date").alias("calendar_date"),
    )

    enriched = (
        hotels.join(property_dim, "property_id", "left")
        .join(
            time_dim.alias("search_time"),
            hotels.search_date == F.col("search_time.calendar_date"),
            "left",
        )
        .withColumn("search_date_sk", F.col("search_time.date_sk"))
        .join(
            time_dim.alias("checkin_time"),
            hotels.checkin_date == F.col("checkin_time.calendar_date"),
            "left",
        )
        .withColumn("checkin_date_sk", F.col("checkin_time.date_sk"))
        .drop(
            F.col("search_time.calendar_date"),
            F.col("search_time.date_sk"),
            F.col("checkin_time.calendar_date"),
            F.col("checkin_time.date_sk"),
        )
        .withColumn("avg_price_per_night", F.col("total_price_usd") / F.col("stay_length_nights"))
    )

    return enriched.select(
        "quote_id",
        "quote_sk",
        "property_id",
        "property_sk",
        "search_date",
        "checkin_date",
        "checkout_date",
        "search_date_sk",
        "checkin_date_sk",
        "stay_length_nights",
        "nightly_price_usd",
        "avg_price_per_night",
        "total_price_usd",
        "availability_ratio",
        "promotion_flag",
        "review_score",
        "cancellation_policy",
        "record_valid_from",
        "record_valid_to",
    )


@dlt.table(
    name="fact_combo_offer",
    comment="Flight + hotel bundles with the initial opportunity index.",
    table_properties={"quality": "silver", "data_domain": "travel", "dataset_purpose": "analytical_fact"},
)
def fact_combo_offer() -> DataFrame:
    flights = dlt.read("stg_flight_quotes")
    hotels = dlt.read("stg_hotel_quotes")
    events = dlt.read("stg_events")

    combos = (
        flights.alias("f")
        .join(
            hotels.alias("h"),
            (F.col("f.destination_city") == F.col("h.city"))
            & (F.abs(F.datediff("f.departure_date", "h.checkin_date")) <= 1),
            "inner",
        )
        .join(
            events.alias("e"),
            (F.col("e.city") == F.col("f.destination_city"))
            & (F.datediff("f.departure_date", "e.start_date") <= 7),
            "left",
        )
    )

    return (
        combos.select(
            surrogate_key("f.quote_id", "h.quote_id").alias("combo_sk"),
            F.col("f.quote_id").alias("flight_quote_id"),
            F.col("h.quote_id").alias("hotel_quote_id"),
            "f.route_id",
            "f.carrier_code",
            "h.property_id",
            F.col("f.departure_date").alias("travel_date"),
            F.col("h.checkin_date").alias("stay_start_date"),
            F.col("h.checkout_date").alias("stay_end_date"),
            (F.col("f.total_price_usd") + F.col("h.total_price_usd")).alias("bundle_price_usd"),
            F.round(
                100
                - (
                    F.col("f.total_price_usd") / F.col("f.base_price_usd")
                    + F.col("h.total_price_usd") / F.col("h.nightly_price_usd") * 10
                ),
                2,
            ).alias("opportunity_index_baseline"),
            F.coalesce(F.col("e.expected_uplift"), F.lit(0.05)).alias("event_uplift"),
            F.col("e.event_type").alias("event_type"),
        )
        .withColumn("record_valid_from", LOAD_TS)
        .withColumn("record_valid_to", F.lit(None).cast(T.TimestampType()))
    )


