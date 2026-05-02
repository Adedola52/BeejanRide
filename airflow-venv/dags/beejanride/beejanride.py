from airflow.sdk import DAG, chain, task, Variable
from pendulum import datetime, duration

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.bash import BashOperator

from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.standard.operators.python import PythonOperator
import sys
sys.path.append('/home/taibat/airflow/airflow-venv/dags/')
sys.path.append('/home/taibat/airflow/airflow-venv/include/')
from include.failure_alerts import failure_alert
from include.success_alert import success_alert

conn_id = Variable.get("connection_id", default=None)
arg = {
    "on_failure_callback": failure_alert,
    "owner": "Taibat",
    "params":{
        "dag_owner": "Taibat"
    },
    "retries": 3,
    "retry_delay": duration(seconds=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": duration(hours=2)
}

with DAG (
    dag_id = "beejanride",
    start_date = datetime(2026,5,1),
    schedule="@daily",
    tags=["beejan","Engennering Team"],
    default_args=arg

) as dag:

    airbyte_ingestion_sync = AirbyteTriggerSyncOperator(
        task_id="airbyte_ingestion_sync",
        airbyte_conn_id="beejan_airbyte",
        connection_id=conn_id,
        asynchronous=False,
        retries=2,
        retry_delay=duration(seconds=4)
    )
    
    airbyte_ingestion_monitor = AirbyteJobSensor(
      task_id= "airbyte_ingestion_sync_monitor",
      airbyte_conn_id="beejan_airbyte",
      airbyte_job_id=airbyte_ingestion_sync.output

    )

    transformation = BashOperator(
        task_id="transformation",
        bash_command="""
        /home/taibat/airflow/dbt-venv/bin/dbt run \
        --project-dir /home/taibat/airflow/beejanride/ \
        --profiles-dir /home/taibat/.dbt
        """
    )

    transformation_test = BashOperator(
        task_id = "transformation_test",
        bash_command = """
        /home/taibat/airflow/dbt-venv/bin/dbt test \
        --project-dir /home/taibat/airflow/beejanride/ \
        --profiles-dir /home/taibat/.dbt
        """

    )

    snapshot = BashOperator(
        task_id ="snapshot",
        bash_command = """
        /home/taibat/airflow/dbt-venv/bin/dbt snapshot \
        --project-dir /home/taibat/airflow/beejanride/ \
        --profiles-dir /home/taibat/.dbt 
        """
    )

    alert = PythonOperator(
        task_id="alert",
        python_callable=success_alert
    )


airbyte_ingestion_sync >> airbyte_ingestion_monitor 
airbyte_ingestion_monitor >> transformation >> [transformation_test, snapshot] >> alert


    


