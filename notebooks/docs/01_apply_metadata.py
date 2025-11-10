# Databricks notebook source
# MAGIC %md
# MAGIC # Apply table and column metadata

# MAGIC This notebook records table comments, column comments, and properties in Unity Catalog for
# MAGIC every layer (Bronze, Silver, Gold). Run it after the pipelines materialize the tables so Genie and the catalog contain rich descriptions.

# COMMAND ----------

from typing import Dict

from pyspark.sql.utils import AnalysisException

# COMMAND ----------

# Basic configuration
CATALOG = "workspace"

# Dictionary with Unity Catalog table comments, column comments, and properties
TABLE_METADATA: Dict[str, Dict] = {
    # --------------------
    # Bronze
    # --------------------
    f"{CATALOG}.bronze_travel.flight_quotes_raw": {
        "comment": "Synthetic LATAM flight quotes with route, airline, pricing, and ingestion metadata attributes.",
        "columns": {
            "quote_id": "Unique identifier for the generated quote.",
            "route_id": "Concatenated origin-destination identifier (IATA).",
            "origin_iata": "IATA code for the departure airport.",
            "destination_iata": "IATA code for the arrival airport.",
            "origin_city": "Origin city for the flight.",
            "destination_city": "Destination city for the flight.",
            "origin_country": "Country of origin.",
            "destination_country": "Destination country.",
            "distance_km": "Approximate route distance in kilometers.",
            "route_category": "Route classification (regional or international).",
            "carrier_code": "Airline code (IATA/ICAO).",
            "carrier_name": "Commercial airline name.",
            "is_low_cost": "Indicator for low-cost carriers.",
            "alliance": "Airline alliance (if applicable).",
            "purchase_date": "Date the fare was queried.",
            "departure_date": "Scheduled flight departure date.",
            "lead_time_days": "Days between purchase and flight.",
            "base_price_usd": "Estimated base fare without extras.",
            "total_price_usd": "Total observed fare at query time.",
            "baggage_fee_usd": "Estimated baggage fee for the fare.",
            "availability_ratio": "Relative availability level (0-1).",
            "promotion_flag": "Indicator for promotional fares.",
            "tax_rate": "Tax rate applied to the fare.",
            "fuel_surcharge_usd": "Associated fuel surcharge.",
            "ingestion_ts": "Bronze-layer ingestion timestamp.",
            "source_batch_id": "Identifier of the synthetic batch generated.",
            "source_system": "Name of the synthetic generator or source system.",
            "ingestion_date": "Ingestion date in Bronze.",
            "ingestion_file": "Logical file identifier associated with the ingest (synthetic).",
        },
        "properties": {
            "quality_tier": "bronze",
            "data_domain": "travel",
        },
    },
    f"{CATALOG}.bronze_travel.hotel_quotes_raw": {
        "comment": "Synthetic hotel quotes for LATAM destinations with pricing, availability, and policy details.",
        "columns": {
            "quote_id": "Unique identifier for the hotel quote.",
            "property_id": "Property or hotel code.",
            "property_name": "Commercial name of the property.",
            "city": "City where the property is located.",
            "country": "Country where the property is located.",
            "stars": "Hotel star rating.",
            "search_date": "Date the stay was searched.",
            "checkin_date": "Quoted check-in date.",
            "checkout_date": "Quoted check-out date.",
            "stay_length_nights": "Length of stay in nights.",
            "nightly_price_usd": "Nightly price in USD.",
            "total_price_usd": "Estimated total stay cost.",
            "availability_ratio": "Relative availability level (0-1).",
            "cancellation_policy": "Applicable cancellation policy.",
            "review_score": "Average review score.",
            "promotion_flag": "Indicator for promotional rate.",
            "ingestion_ts": "Bronze-layer ingestion timestamp.",
            "source_batch_id": "Identifier of the synthetic batch generated.",
            "source_system": "Name of the generator or source system.",
            "ingestion_date": "Ingestion date in Bronze.",
            "ingestion_file": "Logical file identifier associated with the ingest (synthetic).",
        },
        "properties": {
            "quality_tier": "bronze",
            "data_domain": "travel",
        },
    },
    f"{CATALOG}.bronze_travel.event_calendar_raw": {
        "comment": "Synthetic calendar of events impacting travel demand (holidays, concerts, sports, etc.).",
        "columns": {
            "event_id": "Unique identifier for the event.",
            "city": "City where the event occurs.",
            "country": "Country where the event occurs.",
            "event_type": "Event type (festival, concert, sports, etc.).",
            "expected_uplift": "Expected demand uplift (0-1).",
            "start_date": "Event start date.",
            "end_date": "Event end date.",
            "description": "Short description of the event.",
            "ingestion_ts": "Bronze-layer ingestion timestamp.",
            "source_batch_id": "Identifier of the synthetic batch generated.",
            "source_system": "Name of the synthetic generator or source system.",
            "ingestion_date": "Ingestion date in Bronze.",
            "ingestion_file": "Logical file identifier associated with the ingest (synthetic).",
        },
        "properties": {
            "quality_tier": "bronze",
            "data_domain": "travel",
        },
    },
    # --------------------
    # Silver - Dimensions / staging
    # --------------------
    f"{CATALOG}.silver_travel.dim_route": {
        "comment": "Flight route dimension with origin, destination, distance, and category attributes.",
        "columns": {
            "route_sk": "Surrogate key hash for the route.",
            "route_id": "Original route identifier (origin-destination).",
            "origin_iata": "Origin airport IATA code.",
            "destination_iata": "Destination airport IATA code.",
            "origin_city": "Origin city name.",
            "destination_city": "Destination city name.",
            "origin_country": "Origin country.",
            "destination_country": "Destination country.",
            "distance_km": "Approximate distance in kilometers.",
            "route_category": "Route classification (regional or international).",
            "record_valid_from": "Start of the record validity window in Silver.",
            "record_valid_to": "End of the record validity window (null = active).",
        },
        "properties": {
            "quality_tier": "silver",
            "data_domain": "travel",
        },
    },
    f"{CATALOG}.silver_travel.dim_carrier": {
        "comment": "Airline dimension with alliance metadata.",
        "columns": {
            "carrier_sk": "Surrogate key for the airline.",
            "carrier_code": "Airline code.",
            "carrier_name": "Commercial airline name.",
            "is_low_cost": "Low-cost carrier indicator.",
            "alliance": "Associated global alliance.",
            "record_valid_from": "Start of the record validity window.",
            "record_valid_to": "End of the record validity window (null = active).",
        },
        "properties": {
            "quality_tier": "silver",
            "data_domain": "travel",
        },
    },
    f"{CATALOG}.silver_travel.dim_property": {
        "comment": "Hotel and property dimension with location and category attributes.",
        "columns": {
            "property_sk": "Surrogate key for the property.",
            "property_id": "Original property identifier.",
            "property_name": "Commercial property name.",
            "city": "City where the property is located.",
            "country": "Country where the property is located.",
            "stars": "Hotel rating in stars.",
            "record_valid_from": "Start of the record validity window.",
            "record_valid_to": "End of the record validity window (null = active).",
        },
        "properties": {
            "quality_tier": "silver",
            "data_domain": "travel",
        },
    },
    f"{CATALOG}.silver_travel.dim_time": {
        "comment": "Time dimension with daily granularity, ISO week, month, and season attributes.",
        "columns": {
            "date_sk": "Surrogate key for the calendar date.",
            "date": "Calendar date.",
            "day_of_week": "Day-of-week number (1=Sunday).",
            "week_of_year": "ISO week of year.",
            "month": "Calendar month number.",
            "year": "Calendar year.",
            "is_weekend": "Weekend indicator.",
            "month_name": "Month name.",
            "seasonality_band": "Seasonality band (high, medium, low).",
            "record_valid_from": "Start of the record validity window.",
            "record_valid_to": "End of the record validity window (null = active).",
        },
        "properties": {
            "quality_tier": "silver",
            "data_domain": "travel",
        },
    },
    f"{CATALOG}.silver_travel.fact_flight_quote": {
        "comment": "Flight quote fact table with pricing metrics, availability, and foreign keys to dimensions.",
        "columns": {
            "quote_id": "Original identifier for the flight quote.",
            "quote_sk": "Surrogate key for the quote in Silver.",
            "route_id": "Original route identifier.",
            "route_sk": "Foreign key to dim_route.",
            "carrier_code": "Airline code.",
            "carrier_sk": "Foreign key to dim_carrier.",
            "purchase_date": "Original quote date.",
            "purchase_date_sk": "Foreign key to dim_time (purchase date).",
            "departure_date": "Quoted departure date.",
            "departure_date_sk": "Foreign key to dim_time (flight date).",
            "lead_time_days": "Days between purchase and flight.",
            "base_price_usd": "Historic base fare.",
            "total_price_usd": "Total observed fare.",
            "baggage_fee_usd": "Associated baggage cost.",
            "availability_ratio": "Availability signal (0-1).",
            "promotion_flag": "Promotion indicator.",
            "tax_rate": "Tax rate applied.",
            "fuel_surcharge_usd": "Fuel surcharge amount.",
            "gross_price_usd": "Total price including baggage.",
            "yield_per_km": "Price-to-distance ratio.",
            "load_factor_signal": "Expected load factor proxy.",
            "record_valid_from": "Start of the record validity window.",
            "record_valid_to": "End of the record validity window (null = active).",
        },
        "properties": {
            "quality_tier": "silver",
            "data_domain": "travel",
            "dataset_purpose": "analytical_fact",
        },
    },
    f"{CATALOG}.silver_travel.fact_hotel_quote": {
        "comment": "Hotel quote fact table with pricing and availability metrics.",
        "columns": {
            "quote_id": "Original identifier for the hotel quote.",
            "quote_sk": "Surrogate key for the quote in Silver.",
            "property_id": "Property identifier.",
            "property_sk": "Foreign key to dim_property.",
            "search_date": "Stay search date.",
            "search_date_sk": "Foreign key to dim_time (search date).",
            "checkin_date": "Quoted check-in date.",
            "checkin_date_sk": "Foreign key to dim_time (check-in).",
            "checkout_date": "Quoted check-out date.",
            "stay_length_nights": "Length of stay in nights.",
            "nightly_price_usd": "Observed nightly price.",
            "avg_price_per_night": "Calculated average price per night.",
            "total_price_usd": "Estimated total cost.",
            "availability_ratio": "Relative availability (0-1).",
            "promotion_flag": "Promotion indicator.",
            "review_score": "Average review score.",
            "cancellation_policy": "Associated cancellation policy.",
            "record_valid_from": "Start of the record validity window.",
            "record_valid_to": "End of the record validity window (null = active).",
        },
        "properties": {
            "quality_tier": "silver",
            "data_domain": "travel",
            "dataset_purpose": "analytical_fact",
        },
    },
    f"{CATALOG}.silver_travel.fact_combo_offer": {
        "comment": "Flight + hotel bundles with the initial opportunity index.",
        "columns": {
            "combo_sk": "Surrogate key for the generated bundle.",
            "flight_quote_id": "Identifier for the associated flight quote.",
            "hotel_quote_id": "Identifier for the associated hotel quote.",
            "route_id": "Route for the associated flight.",
            "carrier_code": "Airline code for the flight.",
            "property_id": "Identifier for the associated property.",
            "travel_date": "Flight date (trip start).",
            "stay_start_date": "Check-in date.",
            "stay_end_date": "Check-out date.",
            "bundle_price_usd": "Combined price for the flight + hotel package.",
            "opportunity_index_baseline": "Baseline opportunity score before ML.",
            "event_uplift": "Expected uplift due to events (0-1).",
            "event_type": "Relevant event description.",
            "record_valid_from": "Start of the record validity window.",
            "record_valid_to": "End of the record validity window (null = active).",
        },
        "properties": {
            "quality_tier": "silver",
            "data_domain": "travel",
            "dataset_purpose": "analytical_fact",
        },
    },
    # --------------------
    # Gold
    # --------------------
    f"{CATALOG}.gold_travel.gold_price_volatility": {
        "comment": "Gold-layer table with weekly price volatility metrics by route.",
        "columns": {
            "route_sk": "Route key (dim_route).",
            "route_id": "Original route identifier.",
            "origin_city": "Origin city.",
            "destination_city": "Destination city.",
            "purchase_week_of_year": "ISO week of purchase being analyzed.",
            "purchase_year": "Year for the analyzed week.",
            "avg_price_usd": "Average price observed during the week.",
            "std_price_usd": "Standard deviation of prices.",
            "avg_price_ratio": "Average ratio of price to base fare.",
            "p10_price_usd": "10th percentile price.",
            "p90_price_usd": "90th percentile price.",
            "volatility_index": "Volatility index (std / avg * 100).",
            "run_date": "Execution date that produced the metric.",
        },
        "properties": {
            "quality_tier": "gold",
            "data_domain": "travel",
            "dataset_purpose": "monitoring",
        },
    },
    f"{CATALOG}.gold_travel.gold_opportunity_index": {
        "comment": "Gold-layer table with the final opportunity score for flight + hotel bundles.",
        "columns": {
            "combo_sk": "Unique identifier for the bundle.",
            "flight_quote_id": "Identifier for the associated flight quote.",
            "hotel_quote_id": "Identifier for the associated hotel quote.",
            "route_id": "Route for the associated flight.",
            "carrier_code": "Airline code.",
            "travel_date": "Date of travel.",
            "bundle_price_usd": "Total bundle price.",
            "event_uplift": "Expected uplift due to events (0-1).",
            "event_type": "Type of event considered.",
            "flight_total_price_usd": "Flight price used in the bundle.",
            "hotel_avg_price_per_night": "Average nightly price for the associated hotel.",
            "flight_availability_ratio": "Estimated flight availability.",
            "hotel_availability_ratio": "Estimated hotel availability.",
            "opportunity_index": "Normalized 0-100 opportunity score for prioritization.",
            "computed_at": "Timestamp when the score was calculated.",
        },
        "properties": {
            "quality_tier": "gold",
            "data_domain": "travel",
            "dataset_purpose": "recommendation",
        },
    },
    f"{CATALOG}.gold_travel.gold_demand_forecast": {
        "comment": "Daily demand forecast with moving averages by route.",
        "columns": {
            "route_id": "Route identifier.",
            "travel_date": "Forecast travel date.",
            "quotes_count": "Observed quote count for the day.",
            "rolling7_quotes_avg": "7-day moving average of quotes.",
            "rolling7_price_avg": "7-day moving average of price in USD.",
            "forecast_quotes": "Simple projection of future quotes.",
            "forecast_generated_at": "Timestamp when the forecast was generated.",
        },
        "properties": {
            "quality_tier": "gold",
            "data_domain": "travel",
            "dataset_purpose": "forecasting",
        },
    },
    f"{CATALOG}.gold_travel.gold_predictions": {
        "comment": "Batch scoring results for the OpportunityScorer ML model (populated by the ML job).",
        "columns": {
            "route_id": "Route identifier.",
            "travel_date": "Assessed travel date.",
            "opportunity_score": "Opportunity score generated by the model (0-100).",
            "uplift": "Difference between final score and baseline.",
            "model_version": "Model version registered in MLflow.",
            "scored_at": "Scoring timestamp.",
        },
        "properties": {
            "quality_tier": "gold",
            "data_domain": "travel",
            "dataset_purpose": "ml_predictions",
        },
    },
}

# COMMAND ----------

def escape_comment(text: str) -> str:
    """Escape apostrophes for COMMENT statements."""
    return text.replace("'", "''")


def comment_on_table(full_name: str, comment: str) -> None:
    sql = f"COMMENT ON TABLE {full_name} IS '{escape_comment(comment)}'"
    spark.sql(sql)


def comment_on_column(full_name: str, column: str, comment: str) -> None:
    sql = f"COMMENT ON COLUMN {full_name}.{column} IS '{escape_comment(comment)}'"
    spark.sql(sql)


def set_table_properties(full_name: str, properties: Dict[str, str]) -> None:
    assignments = ", ".join(f"'{k}'='{escape_comment(v)}'" for k, v in properties.items())
    sql = f"ALTER TABLE {full_name} SET TBLPROPERTIES ({assignments})"
    spark.sql(sql)


results = []
for table_name, metadata in TABLE_METADATA.items():
    status = {"table": table_name, "applied": True, "messages": []}
    try:
        if "comment" in metadata:
            comment_on_table(table_name, metadata["comment"])
            status["messages"].append("Table comment applied")
        if "columns" in metadata:
            for column, description in metadata["columns"].items():
                try:
                    comment_on_column(table_name, column, description)
                except AnalysisException as col_exc:
                    status["messages"].append(f"Column {column} skipped: {col_exc.desc}")
        if "properties" in metadata:
            try:
                set_table_properties(table_name, metadata["properties"])
                status["messages"].append("Table properties applied")
            except AnalysisException as prop_exc:
                status["messages"].append(f"Properties skipped: {prop_exc.desc}")
    except AnalysisException as exc:
        status["applied"] = False
        status["messages"].append(f"Table skipped: {exc.desc}")
    results.append(status)

# COMMAND ----------

display(spark.createDataFrame(results))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Next steps

# MAGIC - Re-run this notebook whenever new columns are created in Bronze/Silver/Gold.
# MAGIC - Genie and Unity Catalog will use these comments and properties to answer natural-language questions.
# MAGIC - Keep these metadata definitions synchronized with the dictionary in `docs/data_dictionary.md`.


