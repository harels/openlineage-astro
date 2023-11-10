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
    'delivery_times_7_days',
    schedule_interval='@hourly',
    catchup=False,
    default_args=default_args,
    description='Determine weekly top delivery times by restaurant.'
)

t1 = BigQueryOperator(
    task_id='if_not_exists',
    sql='''
    CREATE TABLE IF NOT EXISTS food_delivery.top_delivery_times (
      order_id            INT64,
      order_placed_on     TIME,
      order_dispatched_on TIME,
      order_delivered_on  TIME,
      order_delivery_time INT64,
      customer_email      STRING,
      restaurant_id       INT64,
      driver_id           INT64
    )
    ''',
    use_legacy_sql=False,
    dag=dag
)

t2 = BigQueryOperator(
    task_id='insert',
    sql='''
    SELECT order_id,
           order_placed_on,
           order_dispatched_on,
           order_delivered_on,
           EXTRACT(MINUTE FROM order_delivered_on) - EXTRACT(MINUTE FROM order_placed_on) AS order_delivery_time,
           customer_email,
           restaurant_id,
           driver_id
      FROM food_delivery.delivery_7_days
     ORDER BY order_delivery_time DESC
     LIMIT 1
    ''',
    destination_dataset_table='openlineage-404715.food_delivery.top_delivery_times',
    use_legacy_sql=False,
    dag=dag
)

t1 >> t2
