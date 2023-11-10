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
    'etl_drivers',
    schedule_interval='@hourly',
    catchup=False,
    default_args=default_args,
    description='Loads newly registered drivers daily.'
)

t1 = BigQueryOperator(
    task_id='if_not_exists',
    sql='''
    CREATE TABLE IF NOT EXISTS food_delivery.drivers (
      id                INT64,
      created_at        TIME,
      updated_at        TIME,
      name              STRING,
      email             STRING,
      phone             STRING,
      car_make          STRING,
      car_model         STRING,
      car_year          STRING,
      car_color         STRING,
      car_license_plate STRING
    )
    ''',
    use_legacy_sql=False,
    dag=dag
)

t2 = BigQueryOperator(
    task_id='insert',
    sql='''
    SELECT id, created_at, updated_at, name, email, phone, car_make, car_model, car_year, car_color, car_license_plate
      FROM food_delivery.tmp_drivers
    ''',
    destination_dataset_table='openlineage-404715.food_delivery.drivers',
    use_legacy_sql=False,
    dag=dag
)

t1 >> t2
