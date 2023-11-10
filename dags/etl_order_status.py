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
    'etl_order_status',
    schedule_interval='@hourly',
    catchup=False,
    default_args=default_args,
    description='Loads order statues updates daily.'
)

t1 = BigQueryOperator(
    task_id='if_not_exists',
    sql='''
    CREATE TABLE IF NOT EXISTS food_delivery.order_status (
      id              INT64,
      transitioned_at TIME,
      status          STRING,
      order_id        INT64,
      customer_id     INT64,
      restaurant_id   INT64,
      driver_id       INT64
    )
    ''',
    use_legacy_sql=False,
    dag=dag
)

t2 = BigQueryOperator(
    task_id='insert',
    sql='''
    SELECT id, transitioned_at, status, order_id, customer_id, driver_id, restaurant_id
      FROM food_delivery.tmp_order_status
    ''',
    destination_dataset_table='openlineage-404715.food_delivery.order_status',
    use_legacy_sql=False,
    dag=dag
)

t1 >> t2
