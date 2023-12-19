import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,FloatType,ArrayType,ShortType,LongType

def transform_data(spark_df):
    
    return spark_df

def create_Schema():
    schema = StructType([
        StructField("_id", StringType(), True),
        StructField("url", StringType(), True),
        StructField("agent", StructType([
            StructField("address", StringType(), True),
            StructField("agent_type", StringType(), True),
            StructField("email", StringType(), True),
            StructField("name", StringType(), True),
            StructField("phone_number", StringType(), True),
            StructField("profile", StringType(), True),
        ]), True),
        StructField("attr", StructType([
            StructField("area", FloatType(), True),
            StructField("bathroom", ShortType(), True),
            StructField("bedroom", ShortType(), True),
            StructField("built_year", ShortType(), True),
            StructField("certificate", StringType(), True),
            StructField("condition", StringType(), True),
            StructField("direction", StringType(), True),
            StructField("feature", StringType(), True),
            StructField("floor", FloatType(), True),
            StructField("floor_num", FloatType(), True),
            StructField("height", FloatType(), True),
            StructField("interior", StringType(), True),
            StructField("length", FloatType(), True),
            StructField("site_id", StringType(), True),
            StructField("total_area", FloatType(), True),
            StructField("total_room", ShortType(), True),
            StructField("type_detail", StringType(), True),
            StructField("width", FloatType(), True),
        ]), True),
        StructField("description", StringType(), True),
        StructField("id", StringType(), True),
        StructField("images", ArrayType(StringType(), True), True),
        StructField("initial_at", StringType(), True),
        StructField("location", StructType([
            StructField("address", StringType(), True),
            StructField("city", StringType(), True),
            StructField("description", StringType(), True),
            StructField("dist", StringType(), True),
            StructField("lat", FloatType(), True),
            StructField("long", FloatType(), True),
            StructField("street", StringType(), True),
            StructField("ward", StringType(), True),
        ]), True),
        StructField("price", LongType(), True),
        StructField("price_currency", StringType(), True),
        StructField("price_string", StringType(), True),
        StructField("project", StructType([
            StructField("name", StringType(), True),
            StructField("profile", StringType(), True),
        ]), True),
        StructField("property_type", StringType(), True),
        StructField("publish_at", StringType(), True),
        StructField("site", StringType(), True),
        StructField("thumbnail", StringType(), True),
        StructField("title", StringType(), True),
        StructField("update_at", StringType(), True),
        StructField("initial_date", StringType(), True)
    ])
    return schema

def create_spark_connection():
    minio_access_key = 'd4l74vAq5BauTUcl3at6'
    minio_secret_key='DCzBKp50W47uhAPA98wrx2mfI2OgHNWpIhyU3rXf'
    minio_endpoint='http://192.168.1.6:9000/'

    s_conn = None

    try:
        s_conn = SparkSession.builder \
                .appName("MinIOExtractFile") \
                .master("local[*]") \
                .config("fs.s3a.endpoint", minio_endpoint)\
                .config("fs.s3a.access.key", minio_access_key)\
                .config("fs.s3a.secret.key", minio_secret_key )\
                .config("spark.hadoop.fs.s3a.path.style.access", "true")\
                .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.3")\
                .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")\
                .config("spark.sql.parquet.enableVectorizedReader","false")\
                .config("spark.sql.parquet.writeLegacyFormat","true")\
                .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn

def connect_to_minIO(spark_conn):
    spark_df = None
    s3_path = "s3a://bronze/2023-12-04/"
    try:
        schema = create_Schema()
        spark_df = spark_conn.read\
                    .schema(schema)\
                    .parquet(s3_path)
        logging.info("minIO dataframe created successfully")
    except Exception as e:
        logging.warning(f"minIO dataframe could not be created because: {e}")
    
    return spark_df
    
# log4jLogger = spark.sparkContext._jvm.org.apache.log4j
# LOGGER = log4jLogger.LogManager.getLogger(__name__)


if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()
    print("Testing start .....")
    if spark_conn is not None:
        spark_df = connect_to_minIO(spark_conn)
        print(spark_df.select('*').show(3))
        transformed_df = transform_data(spark_df)
        print(transformed_df.collect())
        #streaming
        # s3_dest="s3a://silver/2023-12-04/"
        # streaming_query = spark_df.coalesce(1).write \
        #     .mode("overwrite")\
        #     .parquet(s3_dest)
    spark_conn.stop()
