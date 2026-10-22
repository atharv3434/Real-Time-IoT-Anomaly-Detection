from pyspark.sql import SparkSession


def get_spark(app_name: str = "iot-anomaly", master: str = "local[*]") -> SparkSession:
    spark = (SparkSession.builder.appName(app_name).master(master)
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark
