# from airflow.contrib.operators.bigquery_operator import BigQueryOperator
# from airflow.utils.dates import days_ago
# from great_expectations.data_context import BaseDataContext
# from great_expectations.data_context.types.base import DataContextConfig, \
#     GCSStoreBackendDefaults
# from great_expectations_provider.operators.great_expectations import \
#     GreatExpectationsOperator
# from airflow import DAG

# project_config = DataContextConfig(
#     datasources={
#         "food_delivery_db": {
#             "data_asset_type": {
#                 "module_name": "great_expectations.dataset",
#                 "class_name": "SqlAlchemyDataset"
#             },
#             "class_name": "SqlAlchemyDatasource",
#             "module_name": "great_expectations.datasource",
#             "credentials": {
#                 "url": "bigquery://openlineage-404715/food_delivery"
#             },
#         }
#     },
#     store_backend_defaults=GCSStoreBackendDefaults(
#         default_bucket_name="us-central1-datakin-staging-135fd46b-bucket",
#         default_project_name="openlineage-404715",
#         expectations_store_prefix="great_expectations/expectations",
#     )
# )

# context = BaseDataContext(project_config=project_config)

# default_args = {
#     'owner': 'datascience',
#     'depends_on_past': False,
#     'start_date': days_ago(1),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'email': ['datascience@example.com']
# }

# dag = DAG(
#     'etl_customers',
#     schedule_interval='@hourly',
#     catchup=False,
#     default_args=default_args,
#     description='Loads newly registered customers daily.'
# )

# t1 = BigQueryOperator(
#     task_id='if_not_exists',
#     sql='''
#     CREATE TABLE IF NOT EXISTS food_delivery.customers (
#       id         INT64,
#       created_at TIME,
#       updated_at TIME,
#       name       STRING,
#       email      STRING,
#       address    STRING,
#       phone      STRING,
#       city_id    INT64
#     )
#     ''',
#     use_legacy_sql=False,
#     dag=dag
# )

# t2 = BigQueryOperator(
#     task_id='etl',
#     sql='''
#     SELECT id, created_at, updated_at, name, email, address, phone, city_id
#       FROM food_delivery.tmp_customers
#     ''',
#     destination_dataset_table='openlineage-404715.food_delivery.customers',
#     use_legacy_sql=False,
#     dag=dag
# )

# t3 = GreatExpectationsOperator(
#     expectation_suite_name='customers_suite',
#     batch_kwargs={
#         'table': 'customers',
#         'datasource': 'food_delivery_db'
#     },
#     data_context=context,
#     dag=dag,
#     task_id='customers_expectation',
# )

# t1 >> t2 >> t3
