### Export environment variables

$ export $(grep -v '^#' .env | xargs)
export $(grep -v '^#' dev.env | xargs)

echo -e "AIRFLOW_UID=$(id -u)" > .env

docker compose up airflow-init



scrapy crawl spider_name