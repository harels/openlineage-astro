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
    'orders_popular_day_of_week',
    schedule_interval='@hourly',
    catchup=False,
    default_args=default_args,
    description='Determines the popular day of week orders are placed.'
)

t1 = BigQueryOperator(
    task_id='if_not_exists',
    sql='''
    CREATE TABLE IF NOT EXISTS food_delivery.popular_orders_day_of_week (
      order_day_of_week STRING,
      order_placed_on   TIME,
      orders_placed     TIME
    )
    ''',
    use_legacy_sql=False,
    dag=dag
)

t2 = BigQueryOperator(
    task_id='insert',
    sql='''
    SELECT FORMAT_TIME("%R", order_placed_on) AS order_day_of_week,
           order_placed_on,
           COUNT(*) AS orders_placed
      FROM food_delivery.top_delivery_times
     GROUP BY order_placed_on
    ''',
    destination_dataset_table='openlineage-404715.food_delivery.popular_orders_day_of_week',
    use_legacy_sql=False,
    dag=dag
)

t1 >> t2
