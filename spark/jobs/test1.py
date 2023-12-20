from pyspark.sql.functions import col
from pyspark.sql import SparkSession

import logging
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
    #save to parquet
    logging.info("Saving filtered dataframe to parquet")
    filtered_df.write.parquet("filtered.parquet")
    #save  to ok.txt
    with open("ok.txt", "w") as f:
        f.write("ok")
    spark.stop()
