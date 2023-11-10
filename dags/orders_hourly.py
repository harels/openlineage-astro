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
    'orders_hourly',
    schedule_interval='@hourly',
    catchup=False,
    default_args=default_args,
    description='Determine hourly orders by customer.'
)

t1 = BigQueryOperator(
    task_id='if_not_exists_raw_orders',
    sql='''
    CREATE TABLE IF NOT EXISTS food_delivery.raw_orders (
      customer_email   STRING,
      orders_placed_on TIMESTAMP,
      order_id         STRING
    )
    ''',
    use_legacy_sql=False,
    dag=dag
)

t2 = BigQueryOperator(
    task_id='if_not_exists_orders_by_hour',
    sql='''
    CREATE TABLE IF NOT EXISTS food_delivery.orders_by_hour (
      customer_email STRING,
      hour_of_day    INT64,
      orders_placed  INT64
    )
    ''',
    use_legacy_sql=False,
    dag=dag
)

t3 = BigQueryOperator(
    task_id='select_then_insert_count',
    sql='''
    SELECT customer_email, 
           EXTRACT(HOUR FROM orders_placed_on) AS hour_of_day,
           COUNT(*) AS orders_placed
      FROM food_delivery.raw_orders
     GROUP BY customer_email, hour_of_day
     ORDER BY COUNT(*) DESC
    ''',
    destination_dataset_table='openlineage-404715.food_delivery.orders_by_hour',
    use_legacy_sql=False,
    dag=dag
)

t1 >> t3
t2 >> t3
