FROM apache/airflow:2.7.3
RUN pip install --upgrade pip
RUN pip install google-cloud-storage
RUN pip install pandas sqlalchemy psycopg2
#WORKDIR /usr/src/app
#COPY ./pipeline.py ./
#COPY ./db/load_data.sql ./
#COPY ./db/sql_queries.sql ./
#COPY ./shelter1000.csv ./
#COPY ./mammal_output.csv ./
#ENTRYPOINT python pipeline.py -i https://data.austintexas.gov/api/views/9t4d-g238/rows.csv
#ENTRYPOINT python pipeline.py -i https://data.austintexas.gov/api/views/9t4d-g238/rows.csv -o ./mammal_output.csv
#ENTRYPOINT python pipeline.py -i ./shelter1000.csv -o ./mammal_output.csv
#ENTRYPOINT [ “bash” ]