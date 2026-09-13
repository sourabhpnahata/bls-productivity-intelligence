# Engineering Thought Process

## 1. Architecture

### Why Bronze / Silver / Gold?
I chose a medallion architecture because the project has two distinct goals: reliable source ingestion and reusable analytics.

* **Bronze:** Preserves raw source inputs. BLS tab-delimited observations and API population payload data are kept separate for full traceability and to allow downstream logic rebuilds without re-downloading source files.
* **Silver:** Standardizes raw data into high-quality analytical tables. Standardizes headers, trims whitespace, casts types, deduplicates keys, classifies annual vs. quarterly records, and joins BLS reference tables.
* **Gold:** Delivers analytics-ready datasets, including a series dimension, productivity fact table, annual/quarterly metrics, population context, and statistical movement features.
* **Statistical / Anomaly Layer:** Positioned after Gold to separate baseline calculations, anomaly scores, and event classifications from structural transformations.

### Key Architectural Decisions
* **Lakeflow / Spark Declarative Pipelines:** Used for end-to-end lineage, explicit dependency tracking across layers, and version-controllable transformation code suitable for production.
* **PySpark Primary Transformation Engine:** Handles complex joins, aggregations, window functions, and period classifications efficiently in Python source files while leaving SQL for ad-hoc analysis.
* **Idempotent Source Ingestion:** BLS files are downloaded only when remote directory timestamps change. Ingestion runs log detailed metadata (timestamps, HTTP status, run ID) to maintain an audit trail.
* **Explicit Period Classification:** Distinguishes quarterly (`Q01`–`Q04`) from annual records. This prevents annual values from contaminating quarterly rolling calculations or causing incorrect quarter conversions.

---

## 2. Trade-offs and Production Readiness

| Feature Area | Current Implementation | Production Recommendation |
| :--- | :--- | :--- |
| **Schema Drift** | Handles known BLS schema quirks. | Add automatic column drift detection, schema versioning, file quarantining, and schema alert notifications. |
| **Data Volume & Scale** | Full processing tailored for manageable BLS volumes. | Implement incremental pipeline processing, Delta file compaction, and cluster tuning based on workload patterns. |
| **Access Control & Security** | Single-environment pipeline demonstration. | Implement RBAC, secret managers for credentials, separate DEV/STAGE/PROD environments, and row/column security. |
| **Observability** | Basic data validation checks and run metadata. | Integrate SLA tracking, pipeline failure alerts, automated volume/freshness anomaly alerts, and data quality dashboards. |
| **Ingestion Resilience** | Source-timestamp comparison logic. | Add exponential backoff retries, content MD5 checksums, rate-limiting handlers, and failed-file quarantine queues. |
| **Anomaly Modeling** | Rolling statistics and movement metrics. | Evaluate against historical incident labels, add seasonality-aware baselines, and monitor false-positive metrics. |

---

## 3. Retrospective

### Primary Challenges
1. **Source Semantics & Mixed Periods:** Understanding the nuances of BLS data—specifically distinguishing annual aggregate records from quarterly data—required implementing explicit `period_type` tracking to prevent skewed rolling aggregations.
2. **Repeatable Ingestion:** Designing an idempotent control mechanism based on BLS directory modification timestamps while navigating local file-system limits within Databricks Free Edition.
3. **Decoupling Business Logic:** Separating broad platform analytics (Gold layer) from single-purpose downstream consumer requirements (such as specific challenge responses).

### Key Takeaways
Production-quality data engineering relies heavily on explicit assumptions: clearly defining record grain, safeguarding idempotence, handling source variations gracefully, and prioritizing structural data quality before downstream consumption.
