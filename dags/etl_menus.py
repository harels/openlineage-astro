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
    'etl_menus',
    schedule_interval='@hourly',
    catchup=False,
    default_args=default_args,
    description='Loads newly added restaurant menus daily.'
)

t1 = BigQueryOperator(
    task_id='if_not_exists',
    sql='''
    CREATE TABLE IF NOT EXISTS food_delivery.menus (
      id            INT64,
      name          STRING,
      restaurant_id INT64,
      description   STRING
    )
    ''',
    use_legacy_sql=False,
    dag=dag
)

t2 = BigQueryOperator(
    task_id='insert',
    sql='''
    SELECT id, name, restaurant_id, description
      FROM food_delivery.tmp_menus
    ''',
    destination_dataset_table='openlineage-404715.food_delivery.menus',
    use_legacy_sql=False,
    dag=dag
)

t1 >> t2
