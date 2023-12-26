export $(grep -v '^#' .env | xargs)
chmod -R 777 dags/ logs/ spark/
docker-compose up --build


spark://spark-master:7077