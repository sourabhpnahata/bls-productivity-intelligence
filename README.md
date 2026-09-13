# bls-productivity-intelligence

````markdown
# BLS Productivity Intelligence Platform

A production-style **Databricks data engineering project** built using U.S. Bureau of Labor Statistics (BLS) Productivity data and U.S. population data.

The project started from the **Rearc Data Quest** and was extended into an end-to-end analytics pipeline with dynamic ingestion, Lakeflow pipelines, medallion architecture, statistical analysis, anomaly detection, validation, and dashboard-ready datasets.

## Architecture

```text
BLS + Population API
        ↓
Python Ingestion
        ↓
Raw Files / Databricks Volume
        ↓
     BRONZE
        ↓
     SILVER
        ↓
      GOLD
        ↓
Statistical / Anomaly Layer
        ↓
Validation + Serving Tables
        ↓
Dashboard
````

## Tech Stack

* Databricks
* PySpark / Spark SQL
* Lakeflow / Spark Declarative Pipelines
* Delta Lake
* Python / Requests
* Databricks Volumes
* Statistical anomaly detection
* Git
* Declarative Automation Bundles

## Data Sources

**BLS Productivity**

`https://download.bls.gov/pub/time.series/pr/`

Key datasets include `pr.data.0.Current`, `pr.series`, `pr.measure`, `pr.sector`, `pr.class`, and related metadata files.

**Population**

Annual U.S. population data is retrieved from the Data USA API.

## Source Ingestion

BLS files are dynamically discovered from the source directory.

Existing files are downloaded only when the BLS modification timestamp changes.

Ingestion metadata tracks:

```text
file_name
source_url
bls_modified_at
downloaded_at_utc
http_status
status
change_type
error_message
ingestion_run_id
```

Population data is refreshed from the API on each run.

## Medallion Architecture

### Bronze

Raw structured source data:

```text
bronze.pr_data_current
bronze.pr_series
bronze.pr_measure
bronze.pr_sector
bronze.pr_class
bronze.pr_period
bronze.pr_duration
bronze.pr_footnote
bronze.pr_seasonal
bronze.population
bronze.ingestion_metadata
```

### Silver

Cleaned, typed and enriched data:

```text
silver.productivity_observations
silver.productivity_series
silver.population_clean
silver.productivity_enriched
```

BLS annual and quarterly observations are explicitly classified so annual records are not incorrectly treated as quarters.

### Gold

Analytics-ready datasets:

```text
gold.dim_series
gold.fact_productivity
gold.series_annual_metrics
gold.series_quarterly_metrics
gold.series_statistics
gold.population_context
gold.series_movement
```

Includes QoQ/YoY metrics, rolling statistics, annual metrics, series profiles and movement features.

## Statistical & Anomaly Detection

A separate statistical layer calculates series-specific baselines and anomaly scores.

```text
gold.series_movement
        ↓
series_baselines
        ↓
anomaly_scores
        ↓
anomaly_events
```

Severity levels:
```text
NORMAL
MEDIUM
HIGH
CRITICAL
INSUFFICIENT_HISTORY
```

Anomaly events also include an explanation/reason for the detected movement.

## Data Quality

A dedicated validation notebook checks:

* completeness and nulls
* duplicates
* valid periods and dates
* annual/quarterly classification
* Gold table grain
* population quality
* analytical metrics
* anomaly scores and severity
* cross-layer consistency

## Project Structure

```text
bls-productivity-intelligence/
├── README.md
├── databricks.yml
├── src/
│   ├── ingestion/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── statistical/
│   └── serving/
├── notebooks/
│   ├── challenge/
├── resources/
└── docs/
```

## Future Enhancements

* Databricks Genie
* Automated alerts
* Advanced anomaly detection
* Forecasting
* CI/CD
* Production orchestration
* Additional BLS datasets

## Summary

This project demonstrates an end-to-end data engineering workflow covering:

**Dynamic ingestion → PySpark → Lakeflow → Delta Lake → Medallion Architecture → Statistical Analysis → Anomaly Detection → Dashboard → Deployment as Code**

```
```
