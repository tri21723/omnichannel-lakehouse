"""Inspect the pinned JVM/runtime classes without opening any data source."""

import json

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("runtime-check").getOrCreate()
try:
    loader = spark._jvm.java.lang.Thread.currentThread().getContextClassLoader()
    for name in [
        "org.apache.iceberg.spark.SparkCatalog",
        "org.apache.iceberg.aws.s3.S3FileIO",
        "org.postgresql.Driver",
    ]:
        loader.loadClass(name)
    assert spark.version == "3.5.5"
    print(
        json.dumps(
            {
                "spark": spark.version,
                "scala": spark._jvm.scala.util.Properties.versionNumberString(),
                "hadoop": spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion(),
                "event": "runtime_classes_verified",
            }
        )
    )
finally:
    spark.stop()
