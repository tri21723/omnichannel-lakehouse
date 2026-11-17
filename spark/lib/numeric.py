"""Checked narrowing for nonnegative count and quantity aggregates."""


def checked_int(value):
    """Check the wide aggregate before casting, independently of Spark ANSI mode."""
    from pyspark.sql import functions as F

    return F.when(
        value.isNotNull() & (value >= 0) & (value <= 2147483647), value.cast("int")
    ).otherwise(F.raise_error("Count/quantity exceeds nonnegative INT range").cast("int"))
