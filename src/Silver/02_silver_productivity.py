from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ============================================================
# SILVER 1 — CLEAN PRODUCTIVITY OBSERVATIONS
# ============================================================

@dp.table(
    name="productivity_observations",
    comment="Cleaned and typed BLS productivity observations."
)
def productivity_observations():

    df = spark.read.table("bronze.pr_data_current")

    return (
        df
        .select(
            F.trim(F.col("series_id")).alias("series_id"),

            F.trim(F.col("year"))
            .cast("int")
            .alias("year"),

            F.upper(
                F.trim(F.col("period"))
            ).alias("period"),

            F.trim(F.col("value"))
            .cast("double")
            .alias("value"),

            F.trim(F.col("footnote_codes"))
            .alias("footnote_codes")
        )
        .filter(
            F.col("series_id").isNotNull()
            & F.col("year").isNotNull()
            & F.col("period").isNotNull()
            & F.col("value").isNotNull()
        )
        .withColumn(
            "period_type",
            F.when(
                F.col("period").rlike("^Q0[1-4]$"),
                F.lit("QUARTERLY")
            )
            .when(
                F.col("period").rlike("^A[0-9]+$"),
                F.lit("ANNUAL")
            )
            .otherwise(
                F.lit("OTHER")
            )
        )
        .dropDuplicates(
            ["series_id", "year", "period"]
        )
    )

# ============================================================
# SILVER 2 — ENRICHED SERIES METADATA
# ============================================================

@dp.table(
    name="productivity_series",
    comment="Enriched BLS productivity series metadata."
)
def productivity_series():

    series = spark.read.table("bronze.pr_series")
    sector = spark.read.table("bronze.pr_sector")
    class_df = spark.read.table("bronze.pr_class")
    measure = spark.read.table("bronze.pr_measure")

    return (
        series.alias("s")

        # ----------------------------------------------------
        # Sector
        # ----------------------------------------------------
        .join(
            sector.alias("sec"),
            F.trim(F.col("s.sector_code"))
            == F.trim(F.col("sec.sector_code")),
            "left"
        )

        # ----------------------------------------------------
        # Class
        # ----------------------------------------------------
        .join(
            class_df.alias("cls"),
            F.trim(F.col("s.class_code"))
            == F.trim(F.col("cls.class_code")),
            "left"
        )

        # ----------------------------------------------------
        # Measure
        # ----------------------------------------------------
        .join(
            measure.alias("m"),
            F.trim(F.col("s.measure_code"))
            == F.trim(F.col("m.measure_code")),
            "left"
        )

        .select(
            F.trim(F.col("s.series_id"))
                .alias("series_id"),

            F.trim(F.col("s.sector_code"))
                .alias("sector_code"),

            F.trim(F.col("sec.sector_name"))
                .alias("sector_name"),

            F.trim(F.col("s.class_code"))
                .alias("class_code"),

            F.trim(F.col("cls.class_text"))
                .alias("class_text"),

            F.trim(F.col("s.measure_code"))
                .alias("measure_code"),

            F.trim(F.col("m.measure_text"))
                .alias("measure_text"),

            F.trim(F.col("s.duration_code"))
                .alias("duration_code"),

            F.trim(F.col("s.seasonal"))
                .alias("seasonal"),

            F.trim(F.col("s.base_year"))
                .alias("base_year"),

            F.trim(F.col("s.begin_year"))
                .cast("int")
                .alias("begin_year"),

            F.trim(F.col("s.begin_period"))
                .alias("begin_period"),

            F.trim(F.col("s.end_year"))
                .cast("int")
                .alias("end_year"),

            F.trim(F.col("s.end_period"))
                .alias("end_period")
        )

        .dropDuplicates(["series_id"])
    )


# ============================================================
# SILVER 3 — CLEAN POPULATION
# ============================================================

@dp.table(
    name="population_clean",
    comment="Cleaned annual US population data."
)
def population_clean():

    df = spark.read.table("bronze.population")

    return (
        df
        .select(
            F.trim(F.col("nation_id"))
                .alias("nation_id"),

            F.trim(F.col("nation"))
                .alias("nation"),

            F.col("year")
                .cast("int")
                .alias("year"),

            F.col("population")
                .cast("long")
                .alias("population")
        )

        .filter(
            F.col("year").isNotNull()
            & F.col("population").isNotNull()
        )

        .dropDuplicates(
            ["nation_id", "year"]
        )
    )


# ============================================================
# SILVER 4 — ENRICHED PRODUCTIVITY DATA
# ============================================================

@dp.table(
    name="productivity_enriched",
    comment="BLS productivity observations enriched with metadata and population."
)
def productivity_enriched():

    observations = spark.read.table(
        "silver.productivity_observations"
    )

    series = spark.read.table(
        "silver.productivity_series"
    )

    population = spark.read.table(
        "silver.population_clean"
    )

    return (
        observations.alias("o")

        # ----------------------------------------------------
        # Productivity → Series Metadata
        # ----------------------------------------------------
        .join(
            series.alias("s"),
            F.col("o.series_id")
            == F.col("s.series_id"),
            "left"
        )

        # ----------------------------------------------------
        # Productivity → Population
        # ----------------------------------------------------
        .join(
            population.alias("p"),
            F.col("o.year")
            == F.col("p.year"),
            "left"
        )

        .select(

            # Productivity observation
            F.col("o.series_id"),
            F.col("o.year"),
            F.col("o.period"),
            F.col("o.value"),
            F.col("o.footnote_codes"),

            # Series metadata
            F.col("s.sector_code"),
            F.col("s.sector_name"),

            F.col("s.class_code"),
            F.col("s.class_text"),

            F.col("s.measure_code"),
            F.col("s.measure_text"),

            F.col("s.duration_code"),
            F.col("s.seasonal"),
            F.col("s.base_year"),

            # Population
            F.col("p.population")
        )
    )