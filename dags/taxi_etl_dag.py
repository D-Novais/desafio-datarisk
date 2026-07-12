from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

# Argumentos padrão da DAG
default_args = {
    'owner': 'datarisk_candidate',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Função de Extração e Carga na RAW com PySpark
def extract_and_load_raw(**kwargs):
    # 1. Pega as credenciais de forma segura do Airflow
    conn = BaseHook.get_connection('postgres_dw_conn')
    db_url = f"jdbc:postgresql://{conn.host}:{conn.port}/{conn.schema}"
    db_properties = {
        "user": conn.login,
        "password": conn.password,
        "driver": "org.postgresql.Driver"
    }

    # 2. Inicia o cluster Spark Local (Alocando 4GB de RAM para o processamento)
    spark = SparkSession.builder \
        .appName("TaxiETL_Datarisk") \
        .config("spark.jars", "/opt/airflow/postgresql-42.6.0.jar") \
        .config("spark.driver.memory", "4g") \
        .master("local[*]") \
        .getOrCreate()

    # 3. Leitura distribuída de todos os arquivos de uma só vez
    data_dir = '/opt/airflow/data'
    print("Iniciando a leitura dos arquivos .parquet.gz com PySpark...")
    
    # O Spark lê o diretório inteiro nativamente
    df_spark = spark.read.parquet(f"{data_dir}/yellow_tripdata_*.parquet*")
    
    linhas = df_spark.count()
    print(f"Total de {linhas} registros carregados no cluster Spark.")

    # 4. Gravação massiva via JDBC
    print("Iniciando a gravação distribuída no PostgreSQL...")
    df_spark.write.jdbc(
        url=db_url,
        table="raw.taxi_trips",
        mode="overwrite", # O overwrite garante a idempotência (limpa e insere)
        properties=db_properties
    )
    
    print("Carga na camada RAW finalizada com sucesso via Apache Spark!")
    spark.stop()

# Definição da DAG
with DAG(
    'taxi_trips_etl',
    default_args=default_args,
    description='ETL de viagens de taxi de NY em 3 camadas',
    schedule_interval=None, # Roda sob demanda
    catchup=False,
    template_searchpath=['/opt/airflow/sql'] # Diz para o Airflow onde estão os arquivos .sql
) as dag:

    # Task 1: Ingestão RAW com PySpark
    task_load_raw = PythonOperator(
        task_id='extract_load_raw',
        python_callable=extract_and_load_raw
    )

    # Task 2: Transformação TRUSTED (Limpeza e Tipagem)
    task_transform_trusted = SQLExecuteQueryOperator(
        task_id='transform_trusted',
        conn_id='postgres_dw_conn',
        sql='insert_trusted.sql' 
    )

    # Task 3: Transformação REFINED (Agregações)
    task_transform_refined = SQLExecuteQueryOperator(
        task_id='transform_refined',
        conn_id='postgres_dw_conn',
        sql='insert_refined.sql' 
    )

    # Ordem de execução do fluxo
    task_load_raw >> task_transform_trusted >> task_transform_refined