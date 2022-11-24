#postgresql credentials
docker run -p 5432:5432 --network=ingest_network -e POSTGRES_PASSWORD="root" -e POSTGRES_USER="root" -e POSTGRES_DB="my_db" -v C://Users//joffr//Desktop//data-engineering-zoomcamp//week_1_basics_n_setup//ny_taxi_postgres_data:/var/lib/postgresql/data --name pg_container postgres:13
docker run --network=ingest_network ingest_script
#only in git bash

#pgadmin credential
docker run -p 80:80 --network=ingest_network -e PGADMIN_DEFAULT_EMAIL="joffreympoitevin@gmail.com" -e PGADMIN_DEFAULT_PASSWORD="root" --name pgadmin dpage/pgadmin4 

#run the data_ingest.py locally  
python data_ingest.py --user root --password root --host localhost --port 5432 --name_db my_db --name_table football_premier_league --url https://datahub.io/sports-data/english-premier-league/r/season-1819.csv --sql_query "SELECT * FROM football_premier_league;" --if_exists "replace"

#run container from dockerfile for data_ingest.py. postgresql container must be running. host:name of te postgresql container
docker build -t ingest_script .
docker run --network=ingest_network -e name_table="football_premier_league" -e url="https://datahub.io/sports-data/english-premier-league/r/season-1819.csv" -e sql_query="SELECT * FROM football_premier_league;" ingest_script 

#docker compose to run the 3 container at the time
#-postgresql
#-pgadmin
#-data_ingest.py (dockerfile)
docker compose up -d 