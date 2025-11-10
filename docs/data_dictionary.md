# Travel Hacking LATAM – Data Dictionary

## Catalog and Schemas

- **Catalog:** `hackathon`
- **Schemas:**
  - `bronze_travel`: raw, deduplicated data with technical metadata.
  - `silver_travel`: entity-relationship model ready for analytics and ML.
  - `gold_travel`: KPIs, predictions, and business-serving views.

## Bronze (`bronze_travel`)

### `flight_quotes_raw`
- **Description:** synthetic LATAM flight quotes with origin/destination, carrier, and pricing attributes.
- **Key columns:** `quote_id`, `route_id`, `origin_iata`, `destination_iata`, `carrier_code`, `purchase_date`, `departure_date`, `total_price_usd`, `availability_ratio`, `promotion_flag`.
- **Technical metadata:** `ingestion_ts`, `source_batch_id`, `source_system`, `ingestion_date`, `ingestion_file`.

### `hotel_quotes_raw`
- **Description:** generated lodging quotes for major destinations.
- **Main columns:** `quote_id`, `property_id`, `property_name`, `city`, `country`, `stars`, `checkin_date`, `checkout_date`, `nightly_price_usd`, `total_price_usd`, `availability_ratio`, `promotion_flag`, `review_score`, `cancellation_policy`, ingestion metadata.

### `event_calendar_raw`
- **Description:** synthetic calendar of events (holidays, festivals, sports) that affect travel demand.
- **Columns:** `event_id`, `city`, `country`, `event_type`, `expected_uplift`, `start_date`, `end_date`, `description`, ingestion metadata.

---

## Silver (`silver_travel`)

### Dimensions
- **`dim_route`** – flight routes (`route_sk`, `route_id`, `origin_*`, `destination_*`, `distance_km`, `route_category`, validity range).
- **`dim_carrier`** – airlines (`carrier_sk`, `carrier_code`, `carrier_name`, `is_low_cost`, `alliance`, validity range).
- **`dim_property`** – properties/hotels (`property_sk`, `property_id`, `property_name`, `city`, `country`, `stars`, validity range).
- **`dim_time`** – calendar (`date_sk`, `date`, `day_of_week`, `week_of_year`, `month`, `year`, `is_weekend`, `seasonality_band`, validity range).

### Facts
- **`fact_flight_quote`** – flight quotes enriched with foreign keys and metrics (`quote_sk`, `route_sk`, `carrier_sk`, `purchase_date_sk`, `departure_date_sk`, `total_price_usd`, `gross_price_usd`, `yield_per_km`, `load_factor_signal`, etc.).
- **`fact_hotel_quote`** – enriched hotel quotes (`quote_sk`, `property_sk`, `search_date_sk`, `checkin_date_sk`, `avg_price_per_night`, `availability_ratio`, `promotion_flag`, etc.).
- **`fact_combo_offer`** – flight + hotel bundles (`combo_sk`, `flight_quote_id`, `hotel_quote_id`, `bundle_price_usd`, `opportunity_index_baseline`, `event_uplift`, `event_type`).

DLT expectations applied: prices > 0, availability within [0,1], consistent dates, and surrogate-key joins.

---

## Gold (`gold_travel`)

- **`gold_price_volatility`** – weekly price volatility metrics by route (`avg_price_usd`, `std_price_usd`, `volatility_index`, percentiles, `purchase_week_of_year`, `purchase_year`).
- **`gold_opportunity_index`** – final 0-100 opportunity score for flight + hotel bundles (`opportunity_index`, `event_uplift`, `price_pressure`, availability metrics, etc.).
- **`gold_demand_forecast`** – simple demand forecast based on 7-day moving averages (`quotes_count`, `rolling7_quotes_avg`, `forecast_quotes`, `forecast_generated_at`).
- **`gold_predictions`** – ML batch-scoring output (`opportunity_score`, `uplift`, `model_version`, `scored_at`).

All tables have comments and properties (`quality_tier`, `data_domain`, `dataset_purpose`) applied via `notebooks/docs/01_apply_metadata.py`. Genie relies on this metadata to answer questions with greater precision.

## MLflow

- Experiment: `/Shared/travel_hacking/experiments/opportunity_scorer`.
- Registered model: `travel_hacking_opportunity_scorer` with stages `None`, `Staging`, `Production`.
- Artifacts: training parameters, metrics (AUC, RMSE), feature importance/explanations.

## Dashboards and Agents

- AI/BI dashboard: `travel_hacking_insights` (core KPIs and narrative).
- Lakehouse AI agent: `travel_hacking_genie` configured with a knowledge base pointing to Gold tables and this data dictionary.

> Keep this document up to date whenever you add new columns, rules, or metrics so Genie always exposes accurate metadata.


