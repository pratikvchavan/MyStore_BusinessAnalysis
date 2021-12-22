from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'hdfs',
    'depends_on_past': False,
    'start_date': datetime(2021,12,22),
    'email': ['airflow@example.com'],
    'email_on_failure': False,,
    'email_on_retry': False,,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
with DAG(
    'Mystoreproject'
    default_args=default_args,
    description='Myproject DAG',
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id='DailyDataIngestAndRefine',
        bash_command='spark-submit --master -yarn --deploy -mode cluster --jars config-1.2.1.jar --class DailyDataIngestAndRefine MyStoreproject-1.0-SNAPSHOT.jar'                     
        )

    t2 = BashOperator(
        task_id='EnrichProductReference',
        depends_on_past=False,
        bash_command='spark-submit --master -yarn --deploy -mode cluster --jars config-1.2.1.jar --class EnrichProductReference MyStoreproject-1.0-SNAPSHOT.jar',
        retries=3,
    )
   

    t3 = BashOperator(
        task_id='VendorEnrichment',
        depends_on_past=False,
        bash_command='spark-submit --master -yarn --deploy -mode cluster --jars config-1.2.1.jar --class VendorEnrichment MyStoreproject-1.0-SNAPSHOT.jar',
        params={'my_param': 'Parameter I passed in'},
    )

    t1 >> t2 >> t3