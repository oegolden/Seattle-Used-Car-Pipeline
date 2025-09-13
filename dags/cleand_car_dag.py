from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def map_to_same_day(execution_date: datetime):
    """
    Always map to the start of the same calendar day,
    so both DAGs align even if they start at different hours.
    """
    return execution_date.replace(hour=0, minute=0, second=0, microsecond=0)


@dag(
    dag_id="clean_car_dag",
    start_date=datetime(2025, 9, 5),
    schedule="@daily",
    catchup=False,
    tags=['used_car'],
)
def mssql_to_spark_etl():
    @task
    def get_mssql_conn_info():
        """
        Extract MSSQL connection info from Airflow Connection securely.
        """
        hook = MsSqlHook(mssql_conn_id="car_db")
        conn = hook.get_connection("car_db")

        jdbc_url = f"jdbc:sqlserver://{conn.host}:{conn.port};databaseName={conn.schema}"
        return {
            "jdbc_url": jdbc_url,
            "user": conn.login,
            "password": conn.password,
        }

    conn_info = get_mssql_conn_info()

    load_data = SparkSubmitOperator(
        task_id="load_data_from_mssql",
        application="/usr/local/airflow/include/scripts/mssql_to_spark.py",
        conn_id="car_db_spark",
        application_args=[
            "{{ ti.xcom_pull(task_ids='get_mssql_conn_info')['jdbc_url'] }}",
            "{{ ti.xcom_pull(task_ids='get_mssql_conn_info')['user'] }}",
            "{{ ti.xcom_pull(task_ids='get_mssql_conn_info')['password'] }}",
            "dbo.used_cars"
        ],
        jars="/opt/spark/jars/mssql-jdbc-12.2.0.jre8.jar",
        conf={
            "spark.local.dir": "/tmp/spark-work",
        }
    )

    conn_info >> load_data


mssql_to_spark_etl()
