export $(grep -v '^#' .env | xargs)

# Run Spark
docker exec -it spark_test-spark-master-1 bin/spark-submit file/test.py 