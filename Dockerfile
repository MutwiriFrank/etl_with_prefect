FROM python:3.9

RUN apt-get install wget
RUN pip install --no-cache-dir  pandas == 1.5.2 \
    prefect == 2.7.7  \
    prefect-sqlalchemy==0.2.2  \
    prefect-gcp[cloud_storage]==0.2.3\
    protobuf==4.21.11\
    pyarrow==10.0\
    pandas-gbq==0.18.1\
    psycopg2-binary==2.9.5\
    sqlalchemy==1.4.46

WORKDIR /prefect_gcp_etl
COPY upload-data.py upload-data.py

ENTRYPOINT [ "python", "upload-data.py" ]