# docker image [gcp]
# https://console.cloud.google.com/gcr/images/spark-operator

FROM gcr.io/spark-operator/spark-py:v3.1.1-hadoop3

# using root user
USER root:root

# create directory for apps
RUN mkdir -p /app

# pip install
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir delta-spark && \
    pip install koalas

# copy spark program
COPY data-extraction.py /app/

# copy jar files
COPY ./jars /opt/spark/jars

# set work directory
WORKDIR /app

# user
USER 1001