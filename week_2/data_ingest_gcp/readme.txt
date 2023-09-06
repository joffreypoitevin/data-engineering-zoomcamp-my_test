IF YOU MODIFY THE SCRIPT OR DOCKERFILE, DONT FORGET TO BUILD THE IMAGE AGAIN!

#postgresql/ingest_script/pg_admin containers without docker compose
docker network create ingest_network
docker run -p 5432:5432 --network=ingest_network -e POSTGRES_PASSWORD="root" -e POSTGRES_USER="root" -e POSTGRES_DB="week_2" -v C://Users//joffr//Desktop//data-engineering-zoomcamp-my_test//week_2//data_ingest_gcp//postgres_db//:/var/lib/postgresql/data --name postgres postgres:15.3-bullseye
docker run --network=ingest_network ingest_script
docker run -p 80:80 --network=ingest_network -e PGADMIN_DEFAULT_EMAIL="joffreympoitevin@gmail.com" -e PGADMIN_DEFAULT_PASSWORD="root" --name pgadmin dpage/pgadmin4 

#run the data_ingest.py locally
docker run -p 5432:5432 -e POSTGRES_PASSWORD="root" -e POSTGRES_USER="root" -e POSTGRES_DB="week_2" -v C://Users//joffr//Desktop//data-engineering-zoomcamp-my_test//week_2//data_ingest_gcp//postgres_db//:/var/lib/postgresql/data --name postgres postgres:15.3-bullseye
python data_ingest.py --user root --password root --host localhost --port 5432 --name_db week_2 --name_table football_premier_league --url https://datahub.io/sports-data/english-premier-league/r/season-1819.csv --sql_query "SELECT * FROM football_premier_league;" --if_exists "replace"
#NOTE locally host = localhost !!!

#run container from dockerfile for data_ingest.py. postgresql container must be running. host:name of te postgresql container
docker build -t ingest_script .
docker run --network=ingest_network -e name_table="football_premier_league" -e url="https://datahub.io/sports-data/english-premier-league/r/season-1819.csv" -e sql_query="SELECT * FROM football_premier_league;" ingest_script 

#docker compose to run the 3 container at the time
#-postgresql
#-pgadmin
#-data_ingest.py (dockerfile)
docker compose up -d 