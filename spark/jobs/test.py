from pyspark.sql.functions import col
from pyspark.sql import SparkSession

def run_spark_job():
    spark = SparkSession.builder.appName("FakeDataFrameExample").getOrCreate()
    data = [("Alice", 34), ("Bob", 45), ("Charlie", 50)]
    columns = ["name", "age"]
    df = spark.createDataFrame(data, columns)
    print("Schema of the DataFrame:")
    df.printSchema()
    print("Content of the DataFrame:")
    df.show()
    filtered_df = df.filter(col("age") > 40)
    print("Filtered DataFrame:")
    filtered_df.show()
    spark.stop()
