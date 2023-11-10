from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'datascience',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['datascience@datakin.com']
}

dag = DAG(
    'modify_table_schemas',
    schedule_interval='@hourly',
    catchup=False,
    default_args=default_args,
    description='Modifies table schemas to force dataset version creation in Marquez.'
)

t1 = BigQueryOperator(
    task_id='modify_raw_orders',
    sql='''
    ALTER TABLE food_delivery.raw_orders
      ADD COLUMN IF NOT EXISTS order_day_of_week STRING
    ''',
    use_legacy_sql=False,
    dag=dag
)
