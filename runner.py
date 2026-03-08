from pyspark.sql.types import NumericType, StringType, BooleanType
import os
from pyspark.sql import SparkSession
from functools import reduce
from pyspark.sql.window import Window
from pyspark.sql import functions as f
from pyspark.sql.functions import to_date, avg, col, to_timestamp, try_divide, when, count, max

flag_cols = [
            "shared_request_flag",
            "shared_match_flag",
            "access_a_ride_flag",
            "wav_request_flag",
            "wav_match_flag"]
datetime_cols = [
    "request_datetime",
    "on_scene_datetime",
    "pickup_datetime",
    "dropoff_datetime"
]

def utils(dataframe):
    numOfPartitions = dataframe.rdd.getNumPartitions()
    numOfCols = len(dataframe.columns)
    return numOfCols, numOfPartitions

spark = SparkSession.builder \
    .appName("MyApp") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "1g") \
    .config("spark.executor.cores", "1") \
    .config("spark.driver.memory", "512m") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print(f"Spark version: {spark.version}")
print(f"Connected to master: {spark.sparkContext.master}")

data_path = "/data/tripdata_2025-06.parquet"

try:
    dataframe = spark.read.parquet(data_path)
    dataframe.printSchema()
    print(f"Number of rows: {dataframe.count()}")

    cols, part = utils(dataframe)

    df = dataframe.dropna().dropDuplicates()
    for c in datetime_cols:
        df = df.withColumn(c, to_timestamp(col(c)))

    for c in flag_cols:
        df = df.withColumn(c, when(col(c) == "Y", 1).otherwise(0))

    num_col, cat_col = [], []
    for field in df.schema.fields:

        if isinstance(field.dataType, NumericType):
            num_col.append(field.name)
        elif isinstance(field.dataType, StringType):
            cat_col.append(field.name)

    num_col = [i for i in num_col if i not in flag_cols]
    print(f"numerical columns are: {num_col}")
    print(f"categorical columns are {cat_col}")

    df = df.withColumn(
        "speed",
        try_divide(col("trip_miles"), col("trip_time")/3600)
    )
    conditions = []

    for c in num_col:
        q = df.approxQuantile(c, [0.25, 0.75], 0.01)
        q1, q3 = q[0], q[1]
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        conditions.append((col(c) >= lower) & (col(c) <= upper))

    df_clean = df.filter(reduce(lambda a, b: a & b, conditions))

    df_clean = df_clean.withColumn("requestDate", to_date(col("request_datetime")))
    windowPartition = Window.partitionBy(col("requestDate"))
    df_clean = df_clean.withColumn("avgDayPay", avg(col("driver_pay")).over(windowPartition))

    result = df_clean.groupBy("requestDate").agg(count("*").alias("trip_count"))
    result.show()

    df_clean.show()


finally:
    spark.stop()