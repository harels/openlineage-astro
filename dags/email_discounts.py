from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'datascience',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['datascience@example.com']
}

dag = DAG(
    'email_discounts',
    schedule_interval='@hourly',
    catchup=False,
    default_args=default_args,
    description='Email discounts to customers that have experienced order delays daily.'
)

t1 = BigQueryOperator(
    task_id='insert',
    sql='''
    SELECT * FROM food_delivery.discounts
    ''',
    use_legacy_sql=False,
    dag=dag
)
