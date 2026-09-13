from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="dashboard_overview",
    comment="Dashboard-ready productivity and anomaly summary metrics."
)
def dashboard_overview():

    fact = spark.read.table(
        "bls_dataquest.gold.fact_productivity"
    )

    anomalies = spark.read.table(
        "bls_dataquest.statistical.anomaly_events"
    )

    series = spark.read.table(
        "bls_dataquest.gold.dim_series"
    )

    latest_year = fact.agg(
        F.max("year").alias("latest_year")
    )

    total_series = series.select(
        F.countDistinct("series_id").alias("total_series")
    )

    total_observations = fact.agg(
        F.count("*").alias("total_observations")
    )

    anomaly_summary = anomalies.agg(
        F.count("*").alias("total_anomalies"),
        F.sum(
            F.when(
                F.col("severity").isin("HIGH", "CRITICAL"),
                1
            ).otherwise(0)
        ).alias("high_critical_anomalies"),
        F.sum(
            F.when(
                F.col("severity") == "CRITICAL",
                1
            ).otherwise(0)
        ).alias("critical_anomalies")
    )

    return (
        latest_year
        .crossJoin(total_series)
        .crossJoin(total_observations)
        .crossJoin(anomaly_summary)
    )


@dp.table(
    name="dashboard_series_trend",
    comment="Dashboard-ready quarterly productivity trends and movement metrics."
)
def dashboard_series_trend():

    return (
        spark.read
        .table("bls_dataquest.gold.series_quarterly_metrics")
        .select(
            "series_id",
            "series_label",
            "sector_name",
            "class_text",
            "measure_text",
            "year",
            "period",
            "quarter",
            "observation_date",
            "value",
            "rolling_4_period_avg",
            "rolling_8_period_avg",
            "rolling_12_period_avg",
            "qoq_change",
            "qoq_change_pct",
            "yoy_change",
            "yoy_change_pct"
        )
    )

@dp.table(
    name="dashboard_anomalies",
    comment="Dashboard-ready anomaly events with productivity movement context."
)
def dashboard_anomalies():

    anomalies = spark.read.table(
        "bls_dataquest.statistical.anomaly_events"
    )

    scores = spark.read.table(
        "bls_dataquest.statistical.anomaly_scores"
    )

    movement = spark.read.table(
        "bls_dataquest.gold.series_movement"
    )

    return (
        anomalies.alias("a")
        .join(
            scores.alias("s"),
            [
                F.col("a.series_id") == F.col("s.series_id"),
                F.col("a.year") == F.col("s.year"),
                F.col("a.period") == F.col("s.period")
            ],
            "left"
        )
        .join(
            movement.alias("m"),
            [
                F.col("a.series_id") == F.col("m.series_id"),
                F.col("a.year") == F.col("m.year"),
                F.col("a.period") == F.col("m.period")
            ],
            "left"
        )
        .select(
            F.col("a.series_id"),
            F.col("a.series_label"),
            F.col("a.year"),
            F.col("a.period"),
            F.col("a.value"),

            F.col("m.observation_date"),
            F.col("m.rolling_4_period_avg"),
            F.col("m.baseline_deviation"),
            F.col("m.baseline_deviation_pct"),
            F.col("m.z_score"),
            F.col("m.movement_direction"),
            F.col("m.movement_magnitude"),

            F.col("s.anomaly_score"),

            F.col("a.severity"),
            F.col("a.anomaly_reason")
        )
    )

@dp.table(
    name="dashboard_best_years",
    comment="Dashboard-ready best-year result based on quarterly annual sums."
)
def dashboard_best_years():

    return (
        spark.read
        .table("bls_dataquest.gold.series_annual_metrics")
        .filter(F.col("is_best_year") == True)
        .select(
            "series_id",
            "series_label",
            "year",
            "annual_value",
            "avg_quarterly_value",
            "min_quarterly_value",
            "max_quarterly_value"
        )
    )


