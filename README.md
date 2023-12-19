export $(grep -v '^#' .env | xargs)

# Run Spark
docker exec -it pipeline-spark-master-1 bin/spark-submit code/connectMinio.py