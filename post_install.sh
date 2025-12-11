docker compose up -d --build

docker compose run --rm apiserver airflow db migrate

docker compose up -d

docker compose ps

docker compose logs apiserver