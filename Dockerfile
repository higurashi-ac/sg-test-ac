FROM apache/airflow:2.7.3-python3.9

USER root
RUN apt-get update \
  && apt-get install -y openjdk-11-jdk procps wget tar \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

RUN wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3-scala2.13.tgz \
  && tar xzf spark-3.5.0-bin-hadoop3-scala2.13.tgz -C /opt \
  && mv /opt/spark-3.5.0-bin-hadoop3-scala2.13 /opt/spark \
  && rm spark-3.5.0-bin-hadoop3-scala2.13.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

RUN mkdir -p /tmp/spark-events && chmod -R 777 /tmp/spark-events

RUN java -version && javac -version
RUN spark-submit --version

USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
  && pip install --no-cache-dir pyspark==3.5.0 \
    apache-airflow-providers-amazon==8.7.0 \
    apache-airflow-providers-postgres==5.6.0 \
    psycopg2-binary==2.9.6 \
    pandas==2.0.3
