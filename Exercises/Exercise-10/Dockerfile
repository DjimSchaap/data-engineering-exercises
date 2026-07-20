FROM python:3.11

RUN apt-get update && apt-get install -y default-jre-headless && rm -rf /var/lib/apt/lists/*

RUN wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz && \
tar xvf spark-3.5.0-bin-hadoop3.tgz && \
mv spark-3.5.0-bin-hadoop3/ /usr/local/spark && \
ln -s /usr/local/spark spark

WORKDIR app
COPY . /app
RUN python3 -m pip install -r requirements.txt --break-system-packages

ENV PYSPARK_PYTHON=python3
