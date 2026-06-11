from pyspark.sql import functions as F


def safe_double(col):
    """Cast a string column to double, turning NULL/empty strings into NULL
    instead of raising CAST_INVALID_INPUT."""
    return (
        F.when(col.isNull() | (col == ""), F.lit(None).cast("double"))
        .otherwise(col.cast("double"))
    )
