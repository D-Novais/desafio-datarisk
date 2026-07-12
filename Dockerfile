FROM apache/airflow:2.7.3

USER root

# 1. Instalar o Java (Motor do Spark) e o wget
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-11-jre-headless wget \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# 2. Baixar o Driver JDBC do PostgreSQL (A ponte que permite o Spark salvar no banco)
RUN wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar -O /opt/airflow/postgresql-42.6.0.jar

USER airflow

# 3. Instalar o PySpark e as dependências
RUN pip install --no-cache-dir pyspark==3.4.1 pandas sqlalchemy