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
    'new_food_deliveries',
    schedule_interval='@hourly',
    catchup=False,
    default_args=default_args,
    description='Add new food delivery data.'
)

t1 = BigQueryOperator(
    task_id='if_not_exists_cities',
    sql='''
    CREATE TABLE IF NOT EXISTS food_delivery.cities (
      id       INT64,
      name     STRING,
      state    STRING,
      zip_code STRING
    )
    ''',
    use_legacy_sql=False,
    dag=dag
)

t2 = BigQueryOperator(
    task_id='if_not_exists_business_hours',
    sql='''
    CREATE TABLE IF NOT EXISTS food_delivery.business_hours (
      id          INT64,
      day_of_week STRING,
      opens_at    TIME,
      closes_at   TIME
    )
    ''',
    use_legacy_sql=False,
    dag=dag
)

t3 = BigQueryOperator(
    task_id='if_not_exists_discounts',
    sql='''
    CREATE TABLE IF NOT EXISTS food_delivery.discounts (
      id           INT64,
      amount_off   INT64,
      customers_id INT64,
      starts_at    TIME,
      ends_at      TIME
    )
    ''',
    use_legacy_sql=False,
    dag=dag
)

t4 = BigQueryOperator(
    task_id='if_not_exists_tmp_restaurants',
    sql='''
    CREATE TABLE IF NOT EXISTS food_delivery.tmp_restaurants (
      id                INT64,
      created_at        TIME,
      updated_at        TIME,
      name              STRING,
      email             STRING,
      address           STRING,
      phone             STRING,
      city_id           INT64,
      business_hours_id INT64,
      description       STRING
    )
    ''',
    use_legacy_sql=False,
    dag=dag
)

t5 = BigQueryOperator(
    task_id='if_not_exists_tmp_menus',
    sql='''
    CREATE TABLE IF NOT EXISTS food_delivery.tmp_menus (
      id            INT64,
      name          STRING,
      restaurant_id INT64,
      description   STRING
    )
    ''',
    use_legacy_sql=False,
    dag=dag
)

t6 = BigQueryOperator(
    task_id='if_not_exists_tmp_menu_items',
    sql='''
    CREATE TABLE IF NOT EXISTS food_delivery.tmp_menu_items (
      id          INT64,
      name        STRING,
      price       STRING,
      category_id INT64,
      description STRING
    )
    ''',
    use_legacy_sql=False,
    dag=dag
)

t7 = BigQueryOperator(
    task_id='if_not_exists_tmp_categories',
    sql='''
    CREATE TABLE IF NOT EXISTS food_delivery.tmp_categories (
      id          INT64,
      name        STRING,
      menu_id     INT64,
      description STRING
    )
    ''',
    use_legacy_sql=False,
    dag=dag
)

t8 = BigQueryOperator(
    task_id='if_not_exists_tmp_drivers',
    sql='''
    CREATE TABLE IF NOT EXISTS food_delivery.tmp_drivers (
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

t9 = BigQueryOperator(
    task_id='if_not_exists_tmp_customers',
    sql='''
    CREATE TABLE IF NOT EXISTS food_delivery.tmp_customers (
      id         INT64,
      created_at TIME,
      updated_at TIME,
      name       STRING,
      email      STRING,
      address    STRING,
      phone      STRING,
      city_id    INT64
    )
    ''',
    use_legacy_sql=False,
    dag=dag
)

t10 = BigQueryOperator(
    task_id='if_not_exists_tmp_orders',
    sql='''
    CREATE TABLE IF NOT EXISTS food_delivery.tmp_orders (
      id           INT64,
      placed_on    TIME,
      menu_item_id INT64,
      quantity     INT64,
      discount_id  INT64,
      comment      STRING
    )
    ''',
    use_legacy_sql=False,
    dag=dag
)

t11 = BigQueryOperator(
    task_id='if_not_exists_tmp_order_status',
    sql='''
    CREATE TABLE IF NOT EXISTS food_delivery.tmp_order_status (
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
