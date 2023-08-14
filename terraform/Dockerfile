FROM python:3.9

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2-binary pyarrow

WORKDIR /2_docker_sql
COPY upload-data.py upload-data.py

ENTRYPOINT [ "python", "upload-data.py" ]