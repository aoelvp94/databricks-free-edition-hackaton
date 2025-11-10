# Databricks notebook source
# MAGIC %md
# MAGIC # Batch scoring – OpportunityScorer

# MAGIC Compute scores for upcoming routes and persist them into `gold_predictions`.

# COMMAND ----------

from datetime import datetime, timedelta

import mlflow
import pandas as pd
from pyspark.sql import functions as F

# COMMAND ----------

try:
    dbutils  # type: ignore # noqa: F821
except NameError:
    class _DBUtilsShim:
        class _Widgets:
            def text(self, *args, **kwargs):
                pass

            def get(self, key: str) -> str:
                raise NameError("dbutils not available")

        def __init__(self):
            self.widgets = self._Widgets()

        def notebook(self):
            raise NameError("dbutils not available")

    dbutils = _DBUtilsShim()  # type: ignore


def notebook_exit(message: str):
    try:
        dbutils.notebook.exit(message)  # type: ignore
    except Exception:
        print(message)


try:
    dbutils.widgets.text("model_stage", "Staging", "Model stage")  # type: ignore
    dbutils.widgets.text("prediction_horizon_days", "21", "Prediction horizon (days)")  # type: ignore
    dbutils.widgets.text("min_opportunity_index", "50", "Filter baseline index ≥")  # type: ignore
    MODEL_STAGE = dbutils.widgets.get("model_stage")  # type: ignore
    PREDICTION_HORIZON_DAYS = int(dbutils.widgets.get("prediction_horizon_days"))  # type: ignore
    MIN_INDEX = float(dbutils.widgets.get("min_opportunity_index"))  # type: ignore
except Exception:
    MODEL_STAGE = "Staging"
    PREDICTION_HORIZON_DAYS = 21
    MIN_INDEX = 50.0

CATALOG = spark.conf.get("pipeline.defaultCatalog", "hackathon")
SILVER_SCHEMA = spark.conf.get("var.silver_schema_name", "silver_travel")
GOLD_SCHEMA = spark.conf.get("var.gold_schema_name", "gold_travel")
REGISTERED_MODEL = "travel_hacking_opportunity_scorer"

client = mlflow.MlflowClient()

stage_lookup = client.get_latest_versions(REGISTERED_MODEL, stages=[MODEL_STAGE]) if MODEL_STAGE.lower() != "none" else []
if stage_lookup:
    model_uri = f"models:/{REGISTERED_MODEL}/{MODEL_STAGE}"
    model_version = stage_lookup[0].version
else:
    latest_none = client.get_latest_versions(REGISTERED_MODEL, stages=["None"])
    if not latest_none:
        raise RuntimeError("No registered model versions found. Run the training job first.")
    model_version = latest_none[0].version
    model_uri = f"models:/{REGISTERED_MODEL}/{model_version}"

model = mlflow.pyfunc.load_model(model_uri)

# COMMAND ----------

today = datetime.utcnow().date()
horizon_date = today + timedelta(days=PREDICTION_HORIZON_DAYS)

feature_cols = [
    "bundle_price_usd",
    "total_price_usd",
    "avg_price_per_night",
    "stay_length_nights",
    "availability_ratio",
    "hotel_availability_ratio",
    "promotion_flag",
    "hotel_promotion_flag",
    "event_uplift_filled",
    "stay_vs_flight_ratio",
    "yield_per_km",
    "distance_km",
]

combo_df = (
    spark.table(f"{CATALOG}.{SILVER_SCHEMA}.fact_combo_offer")
    .where(F.col("opportunity_index_baseline") >= MIN_INDEX)
    .alias("c")
)
flight_df = (
    spark.table(f"{CATALOG}.{SILVER_SCHEMA}.fact_flight_quote")
    .select(
        "quote_id",
        "total_price_usd",
        "yield_per_km",
        "availability_ratio",
        "promotion_flag",
        F.col("departure_date").alias("travel_date"),
        "route_id",
    )
    .alias("f")
)
hotel_df = (
    spark.table(f"{CATALOG}.{SILVER_SCHEMA}.fact_hotel_quote")
    .select(
        F.col("quote_id").alias("hotel_quote_id"),
        "avg_price_per_night",
        "stay_length_nights",
        F.col("availability_ratio").alias("hotel_availability_ratio"),
        F.col("promotion_flag").alias("hotel_promotion_flag"),
    )
    .alias("h")
)
route_df = (
    spark.table(f"{CATALOG}.{SILVER_SCHEMA}.dim_route")
    .select("route_id", "distance_km")
    .dropDuplicates(["route_id"])
    .alias("r")
)

joined = (
    combo_df.join(flight_df, F.col("c.flight_quote_id") == F.col("f.quote_id"), "left")
    .join(hotel_df, F.col("c.hotel_quote_id") == F.col("h.hotel_quote_id"), "left")
    .join(route_df, "route_id", "left")
)

dataset = (
    joined.filter((F.col("travel_date") >= F.lit(today)) & (F.col("travel_date") <= F.lit(horizon_date)))
    .withColumn(
        "stay_vs_flight_ratio",
        F.col("bundle_price_usd")
        / F.when(
            (F.col("total_price_usd") + F.col("avg_price_per_night") * F.col("stay_length_nights")) <= 0,
            F.lit(1.0),
        ).otherwise(
            F.col("total_price_usd") + F.col("avg_price_per_night") * F.col("stay_length_nights")
        ),
    )
    .withColumn("event_uplift_filled", F.coalesce("event_uplift", F.lit(0.05)))
    .withColumn("promotion_flag", F.coalesce(F.col("promotion_flag"), F.lit(0)).cast("int"))
    .withColumn("hotel_promotion_flag", F.coalesce(F.col("hotel_promotion_flag"), F.lit(0)).cast("int"))
    .fillna(
        {
            "total_price_usd": 200.0,
            "stay_vs_flight_ratio": 1.0,
            "avg_price_per_night": 120.0,
            "distance_km": 1500,
            "hotel_availability_ratio": 0.6,
            "hotel_promotion_flag": 0,
            "promotion_flag": 0,
            "availability_ratio": 0.6,
            "stay_length_nights": 3,
        }
    )
)

if dataset.count() == 0:
    notebook_exit("No records to score: adjust parameters.")
    raise SystemExit

pandas_df = dataset.select(
    "route_id",
    "travel_date",
    "combo_sk",
    "opportunity_index_baseline",
    *feature_cols,
).toPandas()

scores = model.predict_proba(pandas_df[feature_cols])[:, 1]

pandas_df["opportunity_score"] = scores * 100
pandas_df["uplift"] = pandas_df["opportunity_score"] - pandas_df["opportunity_index_baseline"]
pandas_df["model_version"] = str(model_version)
pandas_df["scored_at"] = datetime.utcnow()

predictions_spark_df = spark.createDataFrame(
    pandas_df[["route_id", "travel_date", "opportunity_score", "uplift", "model_version", "scored_at"]]
)

predictions_spark_df.createOrReplaceTempView("predictions_batch")

spark.sql(
    f"""
MERGE INTO {CATALOG}.{GOLD_SCHEMA}.gold_predictions AS target
USING predictions_batch AS source
ON target.route_id = source.route_id AND target.travel_date = source.travel_date
WHEN MATCHED THEN UPDATE SET
  target.opportunity_score = source.opportunity_score,
  target.uplift = source.uplift,
  target.model_version = source.model_version,
  target.scored_at = source.scored_at
WHEN NOT MATCHED THEN INSERT *
"""
)

notebook_exit(f"Scoring complete – {len(scores)} records written with model v{model_version}")


