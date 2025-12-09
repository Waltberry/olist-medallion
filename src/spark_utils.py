# src/spark_utils.py
import os
from pathlib import Path

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def _setup_hadoop_on_windows() -> None:
    """
    Ensure HADOOP_HOME / hadoop_home_dir / PATH are set on Windows so that
    Spark's Hadoop layer finds winutils.exe and does not crash.
    """
    if os.name != "nt":
        return

    default_hadoop_home = r"C:\hadoop"
    hadoop_home = os.environ.get("HADOOP_HOME", default_hadoop_home)

    os.environ.setdefault("HADOOP_HOME", hadoop_home)
    os.environ.setdefault("hadoop_home_dir", hadoop_home)

    bin_path = str(Path(hadoop_home) / "bin")
    path_parts = os.environ.get("PATH", "").split(";")
    if bin_path not in path_parts:
        os.environ["PATH"] = os.environ.get("PATH", "") + ";" + bin_path


def get_spark(app_name: str = "olist-medallion") -> SparkSession:
    """
    Creates a local SparkSession configured with Delta Lake support.
    Uses a Hive-backed catalog so databases/tables persist across jobs.
    """
    _setup_hadoop_on_windows()

    warehouse_dir = str(Path("spark-warehouse").resolve())

    # Base builder
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", warehouse_dir)
    )

    # Let delta-spark add its own bits
    builder = configure_spark_with_delta_pip(builder)

    # 🔑 KEY: enable Hive support so catalog (DBs/tables) is persistent
    builder = builder.enableHiveSupport()

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
