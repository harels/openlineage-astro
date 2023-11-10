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
    'etl_categories',
    schedule_interval='@hourly',
    catchup=False,
    default_args=default_args,
    description='Loads newly added menus categories daily.'
)

t1 = BigQueryOperator(
    task_id='if_not_exists',
    sql='''
    CREATE TABLE IF NOT EXISTS food_delivery.categories (
      id          INT64,
      name        STRING,
      menu_id     INT64,
      description STRING
    )
    ''',
    use_legacy_sql=False,
    dag=dag
)

t2 = BigQueryOperator(
    task_id='insert',
    sql='''
    SELECT id, name, menu_id, description
      FROM food_delivery.tmp_categories
    ''',
    destination_dataset_table='openlineage-404715.food_delivery.categories',
    use_legacy_sql=False,
    dag=dag
)

t1 >> t2
