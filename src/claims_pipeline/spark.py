"""SparkSession factory: AWS Glue when running inside a Glue job, plain local Spark otherwise."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def _configure(spark: SparkSession) -> SparkSession:
    conf = spark.conf
    # Overwrite only the partitions present in the DataFrame -> idempotent batch re-runs.
    conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    conf.set("spark.sql.session.timeZone", "UTC")
    conf.set("spark.sql.adaptive.enabled", "true")
    conf.set("spark.sql.parquet.compression.codec", "snappy")
    return spark


def get_spark(app_name: str = "claims-pipeline") -> SparkSession:
    try:  # AWS Glue runtime
        from awsglue.context import GlueContext  # type: ignore
        from pyspark.context import SparkContext

        glue = GlueContext(SparkContext.getOrCreate())
        return _configure(glue.spark_session)
    except ImportError:
        pass

    spark = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return _configure(spark)
