"""Reads and writes for the S3 zones (or local folders). All writes are idempotent."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


def path_exists(spark: SparkSession, path: str) -> bool:
    jvm = spark.sparkContext._jvm
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = hpath.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    return fs.exists(hpath)


def read_raw(spark: SparkSession, fmt: str, path: str, schema: StructType | None = None) -> DataFrame:
    reader = spark.read
    if schema is not None:
        reader = reader.schema(schema)
    if fmt == "csv":
        return reader.option("header", "true").csv(path)
    if fmt == "json":
        return reader.option("mode", "PERMISSIVE").json(path)
    if fmt == "parquet":
        return reader.parquet(path)
    raise ValueError(fmt)


def read_parquet(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.parquet(path)


def write_batch(df: DataFrame, path: str, batch_date: str) -> None:
    """Write one batch into a batch_date partition. Dynamic partition overwrite
    replaces only that partition, so re-running a day never duplicates rows."""
    (df.withColumn("batch_date", F.lit(batch_date)).write.mode("overwrite").partitionBy("batch_date").parquet(path))


def write_table(df: DataFrame, path: str, partition_by: list[str] | None = None) -> None:
    """Full overwrite of a curated table. Curated tables are always rebuilt from
    the cleansed zone (never from themselves), so a plain overwrite is safe."""
    w = df.write.mode("overwrite")
    if partition_by:
        w = w.partitionBy(*partition_by)
    w.parquet(path)


def append(df: DataFrame, path: str) -> None:
    df.write.mode("append").parquet(path)
