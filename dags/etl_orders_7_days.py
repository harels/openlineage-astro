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
    'etl_orders_7_days',
    schedule_interval='@hourly',
    catchup=False,
    default_args=default_args,
    description='Loads newly placed orders weekly.'
)

t1 = BigQueryOperator(
    task_id='if_not_exists',
    sql='''
    CREATE TABLE IF NOT EXISTS food_delivery.orders_7_days (
      order_id      INT64,
      placed_on     TIME,
      discount_id   INT64,
      menu_id       INT64,
      restaurant_id INT64,
      menu_item_id  INT64,
      category_id   INT64
    )
    ''',
    use_legacy_sql=False,
    dag=dag
)

t2 = BigQueryOperator(
    task_id='truncate',
    sql='TRUNCATE TABLE food_delivery.orders_7_days',
    use_legacy_sql=False,
    dag=dag
)

t3 = BigQueryOperator(
    task_id='insert',
    sql='''
    SELECT o.id AS order_id, o.placed_on, o.discount_id, m.id AS menu_id, m.restaurant_id, mi.id AS menu_item_id, c.id AS category_id
      FROM food_delivery.orders AS o
     INNER JOIN food_delivery.menu_items AS mi
        ON mi.id = o.menu_item_id
     INNER JOIN food_delivery.categories AS c
        ON c.id = mi.category_id
     INNER JOIN food_delivery.menus AS m
        ON m.id = c.menu_id
     WHERE o.placed_on >= TIME_SUB(CURRENT_TIME(), INTERVAL 168 HOUR)
    ''',
    destination_dataset_table='openlineage-404715.food_delivery.orders_7_days',
    use_legacy_sql=False,
    dag=dag
)

t1 >> t2 >> t3
