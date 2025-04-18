from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="nfe_trigger",
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["example"],
) as dag:
    trigger_nfe_processing = TriggerDagRunOperator(
        task_id="trigger_nfe_processing",
        trigger_dag_id="nfe_processing",
        conf={"message": "This is a message from the trigger DAG"},
    )