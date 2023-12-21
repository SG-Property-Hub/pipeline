export $(grep -v '^#' dev.env | xargs)
sudo chown 50000:0 dags logs spark
docker-compose up --build


spark://spark-master:7077