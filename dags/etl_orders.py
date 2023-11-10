# from airflow.contrib.operators.bigquery_operator import BigQueryOperator
# from airflow.utils.dates import days_ago
# from great_expectations.data_context import BaseDataContext
# from great_expectations.data_context.types.base import DataContextConfig, \
#     GCSStoreBackendDefaults
# from great_expectations_provider.operators.great_expectations import \
#     GreatExpectationsOperator
# from airflow import DAG
# import os

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
#     validation_operators={
#       'action_list_operator': {
#         'class_name': "ActionListValidationOperator",
#         'action_list': [{
#           "name": "openlineage",
#           "action": {
#             "class_name": "OpenLineageValidationAction",
#             "module_name": "openlineage.common.provider.great_expectations",
#             "openlineage_host": os.getenv('OPENLINEAGE_URL'),
#             "openlineage_apiKey": os.getenv('OPENLINEAGE_API_KEY'),
#             "openlineage_namespace": os.getenv('OPENLINEAGE_NAMESPACE', 'default'),
#             "job_name": "validate_orders"
#           }
#         }]
#       }
#     },
#     anonymous_usage_statistics={'enabled': False},
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
#     'etl_orders',
#     schedule_interval='@hourly',
#     catchup=False,
#     default_args=default_args,
#     description='Loads newly placed orders daily.'
# )

# t1 = BigQueryOperator(
#     task_id='if_not_exists',
#     sql='''
#     CREATE TABLE IF NOT EXISTS food_delivery.orders (
#       id           INT64,
#       placed_on    TIME,
#       menu_item_id INT64,
#       quantity     INT64,
#       discount_id  INT64,
#       comment      STRING
#     )
#     ''',
#     use_legacy_sql=False,
#     dag=dag
# )

# t2 = BigQueryOperator(
#     task_id='insert',
#     sql='''
#     SELECT id, placed_on, menu_item_id, quantity, discount_id, comment
#       FROM food_delivery.tmp_orders;
#     ''',
#     destination_dataset_table='openlineage-404715.food_delivery.orders',
#     use_legacy_sql=False,
#     dag=dag
# )

# t3 = GreatExpectationsOperator(
#     expectation_suite_name='orders_suite',
#     batch_kwargs={
#         'table': 'orders',
#         'datasource': 'food_delivery_db'
#     },
#     data_context=context,
#     dag=dag,
#     task_id='customers_expectation',
# )

# t1 >> t2 >> t3
