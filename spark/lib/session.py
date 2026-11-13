"""Shared Spark session for local Lakekeeper/Iceberg integration."""

import json
import os
from pathlib import Path


def get_spark():
    from pyspark.sql import SparkSession

    warehouse = json.loads(Path("/runtime/catalog.json").read_text())["warehouse_name"]
    settings = {
        "spark.master": os.environ["SPARK_MASTER_URL"],
        "spark.driver.host": "omnichannel-spark-driver",
        "spark.driver.bindAddress": "0.0.0.0",
        "spark.executor.memory": "1g",
        "spark.cores.max": "2",
        "spark.executor.cores": "1",
        "spark.sql.shuffle.partitions": os.environ["SPARK_SHUFFLE_PARTITIONS"],
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.lh": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.lh.type": "rest",
        "spark.sql.catalog.lh.uri": os.environ["LAKEKEEPER_URI"] + "/catalog",
        "spark.sql.catalog.lh.warehouse": warehouse,
        "spark.sql.catalog.lh.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "spark.sql.catalog.lh.s3.endpoint": os.environ["S3_ENDPOINT"],
        "spark.sql.catalog.lh.s3.path-style-access": "true",
        "spark.sql.catalog.lh.s3.access-key-id": os.environ["SPARK_S3_ACCESS_KEY"],
        "spark.sql.catalog.lh.s3.secret-access-key": os.environ["SPARK_S3_SECRET_KEY"],
        "spark.sql.catalog.lh.client.region": os.environ["AWS_DEFAULT_REGION"],
        "spark.redaction.regex": "(?i)secret|password|token|access.key",
    }
    builder = SparkSession.builder.appName("lakehouse-smoke")
    for key, value in settings.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
