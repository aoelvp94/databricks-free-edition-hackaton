# Databricks notebook source
# MAGIC %md
# MAGIC # OpportunityScorer training

# MAGIC Train a model that predicts whether a flight + hotel combination represents a high-value
# MAGIC opportunity (high score) and register it in MLflow.

import mlflow
import numpy as np
import pandas as pd
from lightgbm import LGBMClassifier
from mlflow.models.signature import infer_signature
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import MinMaxScaler
from pyspark.sql import functions as F
from datetime import datetime

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "4")

CATALOG = spark.conf.get("pipeline.defaultCatalog", "hackathon")
SILVER_SCHEMA = spark.conf.get("var.silver_schema_name", "silver_travel")
GOLD_SCHEMA = spark.conf.get("var.gold_schema_name", "gold_travel")
EXPERIMENT_PATH = "/Shared/travel_hacking/experiments/opportunity_scorer"
REGISTERED_MODEL = "travel_hacking_opportunity_scorer"

mlflow.set_experiment(EXPERIMENT_PATH)
mlflow.lightgbm.autolog(disable=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data preparation

# COMMAND ----------

combo_df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.fact_combo_offer")
flight_df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.fact_flight_quote").select(
    "quote_id",
    "total_price_usd",
    "yield_per_km",
    "availability_ratio",
    "promotion_flag",
    "distance_km",
)
hotel_df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.fact_hotel_quote").select(
    F.col("quote_id").alias("hotel_quote_id"),
    "avg_price_per_night",
    "stay_length_nights",
    F.col("availability_ratio").alias("hotel_availability_ratio"),
    F.col("promotion_flag").alias("hotel_promotion_flag"),
)

# COMMAND ----------

dataset = (
    combo_df.alias("c")
    .join(flight_df.alias("f"), "quote_id", "left")
    .join(hotel_df.alias("h"), "hotel_quote_id", "left")
    .withColumn(
        "opportunity_label",
        F.when(F.col("opportunity_index_baseline") >= 65, 1).otherwise(0),
    )
    .withColumn(
        "stay_vs_flight_ratio",
        F.col("bundle_price_usd") / (F.col("total_price_usd") + F.col("avg_price_per_night") * F.col("stay_length_nights")),
    )
    .withColumn("event_uplift_filled", F.coalesce("event_uplift", F.lit(0.05)))
    .withColumn("promotion_flag", F.col("promotion_flag").cast("int"))
    .withColumn("hotel_promotion_flag", F.col("hotel_promotion_flag").cast("int"))
    .fillna(
        {
            "stay_vs_flight_ratio": 1.0,
            "avg_price_per_night": 120.0,
            "distance_km": 1500,
            "hotel_availability_ratio": 0.6,
        }
    )
)

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

pandas_df = dataset.select("combo_sk", *feature_cols, "opportunity_label").toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Training

# COMMAND ----------

X = pandas_df[feature_cols].astype(float)
y = pandas_df["opportunity_label"].astype(int)

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

pipeline = Pipeline(
    steps=[
        ("scaler", MinMaxScaler()),
        (
            "model",
            LGBMClassifier(
                n_estimators=250,
                learning_rate=0.08,
                max_depth=-1,
                subsample=0.9,
                colsample_bytree=0.8,
                random_state=42,
            ),
        ),
    ]
)

with mlflow.start_run(run_name="opportunity_scorer_training"):
    mlflow.set_tags(
        {
            "model_name": REGISTERED_MODEL,
            "data_catalog": CATALOG,
            "training_layer": "gold",
            "training_schema": SILVER_SCHEMA,
            "target_schema": GOLD_SCHEMA,
        }
    )
    pipeline.fit(X_train, y_train)

    y_pred_proba = pipeline.predict_proba(X_test)[:, 1]
    auc = roc_auc_score(y_test, y_pred_proba)
    ap = average_precision_score(y_test, y_pred_proba)

    mlflow.log_metric("roc_auc", float(auc))
    mlflow.log_metric("avg_precision", float(ap))
    mlflow.log_param("feature_count", len(feature_cols))
    mlflow.log_param("pipeline", "MinMaxScaler+LGBMClassifier")

    signature = infer_signature(X_train, pipeline.predict_proba(X_train)[:, 1])
    input_example = X_train.iloc[:5]

    mlflow.sklearn.log_model(
        sk_model=pipeline,
        artifact_path="model",
        registered_model_name=REGISTERED_MODEL,
        signature=signature,
        input_example=input_example,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metadata registration

# COMMAND ----------

latest_model = mlflow.MlflowClient().get_latest_versions(REGISTERED_MODEL, stages=["None"])
if latest_model:
    model_version = latest_model[0].version
    mlflow.MlflowClient().set_registered_model_tag(REGISTERED_MODEL, "business_domain", "travel_hacking")
    mlflow.MlflowClient().set_model_version_tag(REGISTERED_MODEL, model_version, "feature_list", ",".join(feature_cols))
    mlflow.MlflowClient().set_model_version_tag(REGISTERED_MODEL, model_version, "trained_at", datetime.utcnow().isoformat())

# COMMAND ----------

# MAGIC %md
# MAGIC The model is registered in MLflow. Promote it manually to `Staging` or `Production`
# MAGIC depending on the evaluation results.


