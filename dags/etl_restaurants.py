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
    'etl_restaurants',
    schedule_interval='@hourly',
    catchup=False,
    default_args=default_args,
    description='Loads newly registered restaurants daily.'
)

t1 = BigQueryOperator(
    task_id='if_not_exists',
    sql='''
    CREATE TABLE IF NOT EXISTS food_delivery.restaurants (
      id                INT64,
      created_at        TIME,
      updated_at        TIME,
      name              STRING,
      email             STRING,
      address           STRING,
      phone             STRING ,
      city_id           INT64,
      business_hours_id INT64,
      description       STRING
    )
    ''',
    use_legacy_sql=False,
    dag=dag
)

t2 = BigQueryOperator(
    task_id='etl',
    sql='''
    SELECT id, created_at, updated_at, name, email, address, phone, city_id, business_hours_id, description
      FROM food_delivery.tmp_restaurants;
    ''',
    destination_dataset_table='openlineage-404715.food_delivery.restaurants',
    use_legacy_sql=False,
    dag=dag
)

t1 >> t2
