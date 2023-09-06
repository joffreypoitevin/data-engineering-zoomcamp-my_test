FROM python:3.9.1

WORKDIR /app
COPY . .
RUN pip install -r requirements.txt

ARG user="root" 
ARG password="root" 
ARG host="postgres" 
ARG port="5432" 
ARG name_db="week_2" 
ARG name_table="ny_taxi" 
ARG if_exists="replace"

ENV user=${user} 
ENV password=${password} 
ENV host=${host}
ENV port=${port} 
ENV name_db=${name_db} 
ENV name_table=${name_table} 
ENV url=${url} 
ENV sql_query=${sql_query}
ENV if_exists=${if_exists}

ENTRYPOINT ["python", "data_ingest.py"]