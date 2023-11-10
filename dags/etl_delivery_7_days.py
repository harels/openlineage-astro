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
    'etl_delivery_7_days',
    schedule_interval='@hourly',
    catchup=False,
    default_args=default_args,
    description='Loads new deliveries for the week.'
)

t1 = BigQueryOperator(
    task_id='if_not_exists',
    sql='''
    CREATE TABLE IF NOT EXISTS food_delivery.delivery_7_days (
      order_id            INT64,
      order_placed_on     TIME,
      order_dispatched_on TIME,
      order_delivered_on  TIME,
      customer_email      STRING,
      customer_address    STRING,
      discount_id         INT64,
      menu_id             INT64,
      restaurant_id       INT64,
      restaurant_address  STRING,
      menu_item_id        INT64,
      category_id         INT64,
      driver_id           INT64
    )
    ''',
    use_legacy_sql=False,
    dag=dag
)

t2 = BigQueryOperator(
    task_id='truncate',
    sql='TRUNCATE TABLE food_delivery.delivery_7_days',
    use_legacy_sql=False,
    dag=dag
)

t3 = BigQueryOperator(
    task_id='insert',
    sql='''
    SELECT o.order_id, o.placed_on AS order_placed_on,
      (SELECT transitioned_at FROM food_delivery.order_status WHERE order_id = o.order_id AND status = 'DISPATCHED') AS order_dispatched_on,
      (SELECT transitioned_at FROM food_delivery.order_status WHERE order_id = o.order_id AND status = 'DELIVERED') AS order_delivered_on,
      c.email AS customer_email, c.address AS customer_address, o.discount_id, o.menu_id, o.restaurant_id, r.address AS restaurant_address, o.menu_item_id, o.category_id, d.id AS driver_id
      FROM food_delivery.orders_7_days AS o
     INNER JOIN food_delivery.order_status AS os
        ON os.order_id = o.order_id
     INNER JOIN food_delivery.customers AS c
        ON c.id = os.customer_id
     INNER JOIN food_delivery.restaurants AS r
        ON r.id = os.restaurant_id
     INNER JOIN food_delivery.drivers AS d
        ON d.id = os.driver_id
     WHERE os.transitioned_at >= TIME_SUB(CURRENT_TIME(), INTERVAL 168 HOUR)
    ''',
    destination_dataset_table='openlineage-404715.food_delivery.delivery_7_days',
    use_legacy_sql=False,
    dag=dag
)

t1 >> t2 >> t3
