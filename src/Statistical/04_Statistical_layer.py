from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.table(
    name="series_baselines",
    comment="Historical statistical baselines for each BLS productivity series."
)
def series_baselines():

    df = spark.read.table(
        "gold.series_quarterly_metrics"
    )

    historical_window_8 = (
        Window
        .partitionBy("series_id")
        .orderBy("observation_date")
        .rowsBetween(-8, -1)
    )

    historical_window_12 = (
        Window
        .partitionBy("series_id")
        .orderBy("observation_date")
        .rowsBetween(-12, -1)
    )

    return (
        df

        # -------------------------------
        # 8-quarter historical baseline
        # -------------------------------

        .withColumn(
            "baseline_mean_8",
            F.avg("value").over(
                historical_window_8
            )
        )

        .withColumn(
            "baseline_stddev_8",
            F.stddev("value").over(
                historical_window_8
            )
        )

        # -------------------------------
        # 12-quarter historical baseline
        # -------------------------------

        .withColumn(
            "baseline_mean_12",
            F.avg("value").over(
                historical_window_12
            )
        )

        .withColumn(
            "baseline_stddev_12",
            F.stddev("value").over(
                historical_window_12
            )
        )

        # -------------------------------
        # Robust statistics
        # -------------------------------

        .withColumn(
            "baseline_median_8",
            F.expr(
                "percentile_approx(value, 0.5)"
            ).over(historical_window_8)
        )

        .withColumn(
            "baseline_q25_8",
            F.expr(
                "percentile_approx(value, 0.25)"
            ).over(historical_window_8)
        )

        .withColumn(
            "baseline_q75_8",
            F.expr(
                "percentile_approx(value, 0.75)"
            ).over(historical_window_8)
        )

        # -------------------------------
        # IQR
        # -------------------------------

        .withColumn(
            "baseline_iqr_8",
            F.col("baseline_q75_8")
            - F.col("baseline_q25_8")
        )

        # -------------------------------
        # Number of historical observations
        # -------------------------------

        .withColumn(
            "baseline_observation_count",
            F.count("value").over(
                historical_window_12
            )
        )

        # -------------------------------
        # Deviation from baseline
        # -------------------------------

        .withColumn(
            "deviation_from_mean_8",
            F.col("value")
            - F.col("baseline_mean_8")
        )

        .withColumn(
            "deviation_pct_from_mean_8",
            F.when(
                F.col("baseline_mean_8") != 0,

                (
                    F.col("value")
                    - F.col("baseline_mean_8")
                )
                / F.abs(F.col("baseline_mean_8"))
                * 100
            )
        )
    )

@dp.table(
    name="anomaly_scores",
    comment="Multi-signal statistical anomaly scores for BLS productivity observations."
)
def anomaly_scores():

    df = dp.read("series_baselines")

    return (
        df

        # ==========================================
        # Standard z-score
        # ==========================================

        .withColumn(
            "z_score_8",
            F.when(
                F.col("baseline_stddev_8") > 0,

                (
                    F.col("value")
                    - F.col("baseline_mean_8")
                )
                / F.col("baseline_stddev_8")
            )
        )

        # ==========================================
        # IQR-based robust score
        # ==========================================

        .withColumn(
            "iqr_score",
            F.when(
                F.col("baseline_iqr_8") > 0,

                F.when(
                    F.col("value")
                    > F.col("baseline_q75_8"),

                    (
                        F.col("value")
                        - F.col("baseline_q75_8")
                    )
                    / F.col("baseline_iqr_8")
                )

                .when(
                    F.col("value")
                    < F.col("baseline_q25_8"),

                    (
                        F.col("baseline_q25_8")
                        - F.col("value")
                    )
                    / F.col("baseline_iqr_8")
                )

                .otherwise(
                    F.lit(0.0)
                )
            )
        )

        # ==========================================
        # Movement magnitude
        # ==========================================

        .withColumn(
            "movement_magnitude_pct",
            F.abs(
                F.col("qoq_change_pct")
            )
        )

        # ==========================================
        # Direction
        # ==========================================

        .withColumn(
            "movement_direction",
            F.when(
                F.col("qoq_change") > 0,
                "INCREASE"
            )
            .when(
                F.col("qoq_change") < 0,
                "DECREASE"
            )
            .otherwise(
                "NO_CHANGE"
            )
        )

        # ==========================================
        # Is there enough history?
        # ==========================================

        .withColumn(
            "baseline_ready",
            F.col("baseline_observation_count") >= 4
        )

        # ==========================================
        # Individual anomaly signals
        # ==========================================

        .withColumn(
            "z_anomaly",
            F.when(
                F.abs(F.col("z_score_8")) >= 3,
                True
            )
            .otherwise(False)
        )

        .withColumn(
            "iqr_anomaly",
            F.when(
                F.col("iqr_score") >= 1.5,
                True
            )
            .otherwise(False)
        )

        # ==========================================
        # Combined anomaly score
        # ==========================================

        .withColumn(
            "anomaly_score",

            (
                F.when(
                    F.col("z_anomaly"),
                    F.lit(2.0)
                )
                .otherwise(F.lit(0.0))

                +

                F.when(
                    F.col("iqr_anomaly"),
                    F.lit(2.0)
                )
                .otherwise(F.lit(0.0))

                +

                F.when(
                    F.abs(
                        F.col("qoq_change_pct")
                    ) >= 5,
                    F.lit(1.0)
                )
                .otherwise(F.lit(0.0))

                +

                F.when(
                    F.abs(
                        F.col("yoy_change_pct")
                    ) >= 10,
                    F.lit(1.0)
                )
                .otherwise(F.lit(0.0))
            )
        )

        # ==========================================
        # Severity
        # ==========================================

        .withColumn(
            "severity",

            F.when(
                ~F.col("baseline_ready"),
                "INSUFFICIENT_HISTORY"
            )

            .when(
                F.col("anomaly_score") >= 5,
                "CRITICAL"
            )

            .when(
                F.col("anomaly_score") >= 3,
                "HIGH"
            )

            .when(
                F.col("anomaly_score") >= 2,
                "MEDIUM"
            )

            .otherwise(
                "NORMAL"
            )
        )
    )

@dp.table(
    name="anomaly_events",
    comment="Detected BLS productivity anomalies with severity and explainable reason codes."
)
def anomaly_events():

    df = dp.read("anomaly_scores")

    return (
        df

        .filter(
            F.col("severity").isin(
                "MEDIUM",
                "HIGH",
                "CRITICAL"
            )
        )

        .withColumn(
            "anomaly_reason",

            F.concat_ws(
                "; ",

                F.when(
                    F.col("z_anomaly"),
                    F.lit(
                        "Historical z-score exceeded threshold"
                    )
                ),

                F.when(
                    F.col("iqr_anomaly"),
                    F.lit(
                        "Value deviated materially from historical IQR"
                    )
                ),

                F.when(
                    F.abs(
                        F.col("qoq_change_pct")
                    ) >= 5,

                    F.lit(
                        "Large quarter-over-quarter movement"
                    )
                ),

                F.when(
                    F.abs(
                        F.col("yoy_change_pct")
                    ) >= 10,

                    F.lit(
                        "Large year-over-year movement"
                    )
                )
            )
        )

        .select(
            "series_id",
            "series_label",

            "sector_name",
            "class_text",
            "measure_text",

            "year",
            "period",
            "observation_date",

            "value",

            "previous_value",
            "qoq_change",
            "qoq_change_pct",

            "previous_year_same_quarter",
            "yoy_change",
            "yoy_change_pct",

            "baseline_mean_8",
            "baseline_stddev_8",

            "z_score_8",
            "iqr_score",

            "anomaly_score",
            "severity",

            "movement_direction",
            "anomaly_reason",

            "population"
        )
    )

