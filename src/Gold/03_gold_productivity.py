from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# 1. SERIES DIMENSION
# ============================================================

@dp.table(
    name="dim_series",
    comment="Analytics-ready dimension containing one record per BLS productivity series."
)
def dim_series():

    series = spark.read.table("silver.productivity_series")

    return (
        series
        .select(
            "series_id",
            "sector_code",
            "sector_name",
            "class_code",
            "class_text",
            "measure_code",
            "measure_text",
            "duration_code",
            "seasonal",
            "base_year",
            "begin_year",
            "begin_period",
            "end_year",
            "end_period"
        )
        .withColumn(
            "series_label",
            F.concat_ws(
                ": ",
                F.col("sector_name"),
                F.col("class_text"),
                F.col("measure_text")
            )
        )
        .withColumn(
            "is_manufacturing",
            F.when(
                F.lower(F.col("sector_name")).contains("manufactur"),
                F.lit(True)
            ).otherwise(F.lit(False))
        )
        .withColumn(
            "is_seasonally_adjusted",
            F.when(
                F.upper(F.trim(F.col("seasonal"))) == "S",
                F.lit(True)
            )
            .when(
                F.upper(F.trim(F.col("seasonal"))) == "Y",
                F.lit(True)
            )
            .otherwise(F.lit(False))
        )
        .withColumn(
            "series_start_year",
            F.col("begin_year")
        )
        .withColumn(
            "series_end_year",
            F.col("end_year")
        )
        .dropDuplicates(["series_id"])
    )


# ============================================================
# 2. PRODUCTIVITY FACT
# ============================================================

@dp.table(
    name="fact_productivity",
    comment="Analytics-ready BLS productivity observations enriched with series metadata and population."
)
def fact_productivity():

    observations = spark.read.table(
        "silver.productivity_observations"
    )

    series = spark.read.table(
        "silver.productivity_series"
    )

    population = spark.read.table(
        "silver.population_clean"
    )

    df = (
        observations.alias("o")
        .join(
            series.alias("s"),
            F.col("o.series_id") == F.col("s.series_id"),
            "left"
        )
        .join(
            population.alias("p"),
            F.col("o.year") == F.col("p.year"),
            "left"
        )
        .select(
            F.col("o.series_id"),
            F.col("o.year"),
            F.col("o.period"),
            F.col("o.period_type"),
            F.col("o.value"),
            F.col("o.footnote_codes"),

            F.col("s.sector_code"),
            F.col("s.sector_name"),

            F.col("s.class_code"),
            F.col("s.class_text"),

            F.col("s.measure_code"),
            F.col("s.measure_text"),

            F.col("s.duration_code"),
            F.col("s.seasonal"),
            F.col("s.base_year"),

            F.col("p.population")
        )
    )

    return (
        df

        # Extract quarter only for quarterly observations
        .withColumn(
            "quarter",
            F.when(
                F.col("period_type") == "QUARTERLY",
                F.regexp_extract(
                    F.col("period"),
                    r"Q([0-9]+)",
                    1
                ).cast("int")
            )
        )

        # Annual observations get month = January.
        # This gives them a valid chronological date while
        # preserving period_type = ANNUAL.
        .withColumn(
            "observation_date",
            F.when(
                F.col("period_type") == "QUARTERLY",
                F.make_date(
                    F.col("year"),
                    ((F.col("quarter") - 1) * 3 + 1),
                    F.lit(1)
                )
            )
            .when(
                F.col("period_type") == "ANNUAL",
                F.make_date(
                    F.col("year"),
                    F.lit(1),
                    F.lit(1)
                )
            )
        )

        .withColumn(
            "series_label",
            F.concat_ws(
                ": ",
                F.col("sector_name"),
                F.col("class_text"),
                F.col("measure_text")
            )
        )

        .withColumn(
            "is_manufacturing",
            F.when(
                F.lower(
                    F.col("sector_name")
                ).contains("manufactur"),
                F.lit(True)
            )
            .otherwise(F.lit(False))
        )
    )

# ============================================================
# 3. ANNUAL SERIES METRICS
# ============================================================

@dp.table(
    name="series_annual_metrics",
    comment="Annual analytical metrics for every BLS productivity series."
)
def series_annual_metrics():

    df = spark.read.table("fact_productivity").filter(F.col("period_type") == "QUARTERLY")

    annual = (
        df
        .groupBy(
            "series_id",
            "year"
        )
        .agg(
            F.sum("value").alias("annual_value"),

            F.avg("value").alias(
                "avg_quarterly_value"
            ),

            F.min("value").alias(
                "min_quarterly_value"
            ),

            F.max("value").alias(
                "max_quarterly_value"
            ),

            F.stddev("value").alias(
                "quarterly_stddev"
            ),

            F.count("value").alias(
                "quarter_count"
            ),

            F.first(
                "population",
                ignorenulls=True
            ).alias("population"),

            F.first(
                "series_label",
                ignorenulls=True
            ).alias("series_label"),

            F.first(
                "sector_name",
                ignorenulls=True
            ).alias("sector_name"),

            F.first(
                "class_text",
                ignorenulls=True
            ).alias("class_text"),

            F.first(
                "measure_text",
                ignorenulls=True
            ).alias("measure_text")
        )
    )

    window_spec = (
        Window
        .partitionBy("series_id")
        .orderBy("year")
    )

    return (
        annual

        .withColumn(
            "previous_year_value",
            F.lag("annual_value").over(window_spec)
        )

        .withColumn(
            "yoy_change",
            F.col("annual_value")
            - F.col("previous_year_value")
        )

        .withColumn(
            "yoy_change_pct",
            F.when(
                F.col("previous_year_value") != 0,
                (
                    F.col("annual_value")
                    - F.col("previous_year_value")
                )
                / F.abs(F.col("previous_year_value"))
                * 100
            )
        )

        .withColumn(
            "year_rank",
            F.row_number().over(
                Window
                .partitionBy("series_id")
                .orderBy(
                    F.col("annual_value").desc(),
                    F.col("year").desc()
                )
            )
        )

        .withColumn(
            "is_best_year",
            F.col("year_rank") == 1
        )
    )


# ============================================================
# 4. QUARTERLY SERIES METRICS
# ============================================================

@dp.table(
    name="series_quarterly_metrics",
    comment="Quarterly movement and rolling metrics for every BLS productivity series."
)
def series_quarterly_metrics():

    df = spark.read.table("fact_productivity").filter(F.col("period_type") == "QUARTERLY")

    window_spec = (
        Window
        .partitionBy("series_id")
        .orderBy("observation_date")
    )

    return (
        df

        # Previous quarter
        .withColumn(
            "previous_value",
            F.lag("value").over(window_spec)
        )

        .withColumn(
            "qoq_change",
            F.col("value")
            - F.col("previous_value")
        )

        .withColumn(
            "qoq_change_pct",
            F.when(
                F.col("previous_value") != 0,
                (
                    F.col("value")
                    - F.col("previous_value")
                )
                / F.abs(F.col("previous_value"))
                * 100
            )
        )

        # Same quarter previous year
        .withColumn(
            "previous_year_same_quarter",
            F.lag("value", 4).over(window_spec)
        )

        .withColumn(
            "yoy_change",
            F.col("value")
            - F.col("previous_year_same_quarter")
        )

        .withColumn(
            "yoy_change_pct",
            F.when(
                F.col("previous_year_same_quarter") != 0,
                (
                    F.col("value")
                    - F.col("previous_year_same_quarter")
                )
                / F.abs(F.col("previous_year_same_quarter"))
                * 100
            )
        )

        # Rolling 4-quarter statistics
        .withColumn(
            "rolling_4_period_avg",
            F.avg("value").over(
                Window
                .partitionBy("series_id")
                .orderBy("observation_date")
                .rowsBetween(-4, -1)
            )
        )

        .withColumn(
            "rolling_4_period_stddev",
            F.stddev("value").over(
                Window
                .partitionBy("series_id")
                .orderBy("observation_date")
                .rowsBetween(-4, -1)
            )
        )

        # Rolling 8-quarter statistics
        .withColumn(
            "rolling_8_period_avg",
            F.avg("value").over(
                Window
                .partitionBy("series_id")
                .orderBy("observation_date")
                .rowsBetween(-8, -1)
            )
        )

        .withColumn(
            "rolling_8_period_stddev",
            F.stddev("value").over(
                Window
                .partitionBy("series_id")
                .orderBy("observation_date")
                .rowsBetween(-8, -1)
            )
        )

        # Rolling 12-quarter statistics
        .withColumn(
            "rolling_12_period_avg",
            F.avg("value").over(
                Window
                .partitionBy("series_id")
                .orderBy("observation_date")
                .rowsBetween(-12, -1)
            )
        )

        .withColumn(
            "rolling_12_period_stddev",
            F.stddev("value").over(
                Window
                .partitionBy("series_id")
                .orderBy("observation_date")
                .rowsBetween(-12, -1)
            )
        )
    )


# ============================================================
# 5. SERIES STATISTICAL PROFILE
# ============================================================

@dp.table(
    name="series_statistics",
    comment="Statistical profile of every BLS productivity series."
)
def series_statistics():

    df = spark.read.table("fact_productivity")

    return (
        df
        .groupBy("series_id")
        .agg(
            F.first(
                "series_label",
                ignorenulls=True
            ).alias("series_label"),

            F.first(
                "sector_name",
                ignorenulls=True
            ).alias("sector_name"),

            F.first(
                "class_text",
                ignorenulls=True
            ).alias("class_text"),

            F.first(
                "measure_text",
                ignorenulls=True
            ).alias("measure_text"),

            F.count("value").alias(
                "observation_count"
            ),

            F.min("year").alias(
                "first_year"
            ),

            F.max("year").alias(
                "last_year"
            ),

            F.avg("value").alias(
                "mean_value"
            ),

            F.expr(
                "percentile_approx(value, 0.5)"
            ).alias(
                "median_value"
            ),

            F.stddev("value").alias(
                "stddev_value"
            ),

            F.min("value").alias(
                "min_value"
            ),

            F.max("value").alias(
                "max_value"
            ),

            F.expr(
                "percentile_approx(value, 0.25)"
            ).alias(
                "q25"
            ),

            F.expr(
                "percentile_approx(value, 0.75)"
            ).alias(
                "q75"
            )
        )
        .withColumn(
            "coefficient_of_variation",
            F.when(
                F.col("mean_value") != 0,
                F.col("stddev_value")
                / F.abs(F.col("mean_value"))
            )
        )
    )


# ============================================================
# 6. POPULATION CONTEXT
# ============================================================

@dp.table(
    name="population_context",
    comment="Annual US population with year-over-year and rolling population metrics."
)
def population_context():

    population = spark.read.table(
        "silver.population_clean"
    )

    window_spec = (
        Window
        .orderBy("year")
    )

    return (
        population

        .withColumn(
            "previous_year_population",
            F.lag("population").over(window_spec)
        )

        .withColumn(
            "population_yoy_change",
            F.col("population")
            - F.col("previous_year_population")
        )

        .withColumn(
            "population_yoy_change_pct",
            F.when(
                F.col("previous_year_population") != 0,
                (
                    F.col("population")
                    - F.col("previous_year_population")
                )
                / F.col("previous_year_population")
                * 100
            )
        )

        .withColumn(
            "population_rolling_3_year_avg",
            F.avg("population").over(
                Window
                .orderBy("year")
                .rowsBetween(-3, -1)
            )
        )
    )


# ============================================================
# 7. SERIES MOVEMENT
# ============================================================

@dp.table(
    name="series_movement",
    comment="Standardized productivity movement features used by downstream statistical anomaly detection."
)
def series_movement():

    df = spark.read.table(
        "series_quarterly_metrics"
    )

    return (
        df

        # Change direction
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
            .otherwise("NO_CHANGE")
        )

        # Deviation from historical 4-quarter baseline
        .withColumn(
            "baseline_deviation",
            F.col("value")
            - F.col("rolling_4_period_avg")
        )

        .withColumn(
            "baseline_deviation_pct",
            F.when(
                F.col("rolling_4_period_avg") != 0,
                (
                    F.col("value")
                    - F.col("rolling_4_period_avg")
                )
                / F.abs(F.col("rolling_4_period_avg"))
                * 100
            )
        )

        # Z-score using only previous observations
        .withColumn(
            "z_score",
            F.when(
                F.col("rolling_4_period_stddev") > 0,
                (
                    F.col("value")
                    - F.col("rolling_4_period_avg")
                )
                / F.col("rolling_4_period_stddev")
            )
        )

        # Broad movement classification
        .withColumn(
            "movement_magnitude",
            F.when(
                F.col("qoq_change_pct").isNull(),
                "UNKNOWN"
            )
            .when(
                F.abs(F.col("qoq_change_pct")) < 1,
                "STABLE"
            )
            .when(
                F.abs(F.col("qoq_change_pct")) < 5,
                "MODERATE"
            )
            .otherwise(
                "LARGE"
            )
        )
    )

