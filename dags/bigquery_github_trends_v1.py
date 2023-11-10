from datetime import timedelta, datetime
import json
from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2017, 6, 02),
    'email': [airflow@airflow.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

schedule_interval = "00 21 * * *"

dag = DAG('bigquery_github_trends_v1', default_args=default_args, schedule_interval=schedule_interval)

t1 = BigQueryCheckOperator(
    task_id='bq_check_githubarchive_day',
    sql='''
    #legacySql
    SELECT table_id
    FROM [githubarchive:day.__TABLES__]
    WHERE table_id = "{{ yesterday_ds_nodash }}"
    ''',
    dag=dag)

t2 = BigQueryCheckOperator(
    task_id='bq_check_hackernews_full',
    sql='''
    #legacySql
    SELECT
    STRFTIME_UTC_USEC(timestamp, "%Y%m%d") as date
    FROM
      [bigquery-public-data:hacker_news.full]
    WHERE
    type = 'story'
    AND STRFTIME_UTC_USEC(timestamp, "%Y%m%d") = "{{ yesterday_ds_nodash }}"
    LIMIT 1
    ''',
    dag=dag)

t3 = BigQueryOperator(
    task_id='bq_write_to_github_daily_metrics',
    use_legacy_sql=False,
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    bql='''
    #standardSQL
    SELECT
      date,
      repo,
      SUM(IF(type='WatchEvent', 1, NULL)) AS stars,
      SUM(IF(type='ForkEvent',  1, NULL)) AS forks
    FROM (
      SELECT
        FORMAT_TIMESTAMP("%Y%m%d", created_at) AS date,
        actor.id as actor_id,
        repo.name as repo,
        type
      FROM
        `githubarchive.day.{{ yesterday_ds_nodash }}`
      WHERE type IN ('WatchEvent','ForkEvent')
    )
    GROUP BY
      date,
      repo
    ''',    destination_dataset_table='airflow-cloud-public-datasets.github_trends.github_daily_metrics${{ yesterday_ds_nodash }}',
    dag=dag)

t4 = BigQueryOperator(
    task_id='bq_write_to_github_agg',
    use_legacy_sql=False,
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    bql='''
    #standardSQL
    SELECT
      "{{ yesterday_ds_nodash }}" as date,
      repo,
      SUM(stars) as stars_last_28_days,
      SUM(IF(_PARTITIONTIME BETWEEN TIMESTAMP("{{ macros.ds_add(ds, -6) }}")
        AND TIMESTAMP("{{ yesterday_ds }}") ,
        stars, null)) as stars_last_7_days,
      SUM(IF(_PARTITIONTIME BETWEEN TIMESTAMP("{{ yesterday_ds }}")
        AND TIMESTAMP("{{ yesterday_ds }}") ,
        stars, null)) as stars_last_1_day,
      SUM(forks) as forks_last_28_days,
      SUM(IF(_PARTITIONTIME BETWEEN TIMESTAMP("{{ macros.ds_add(ds, -6) }}")
        AND TIMESTAMP("{{ yesterday_ds }}") ,
        forks, null)) as forks_last_7_days,
      SUM(IF(_PARTITIONTIME BETWEEN TIMESTAMP("{{ yesterday_ds }}")
        AND TIMESTAMP("{{ yesterday_ds }}") ,
        forks, null)) as forks_last_1_day
    FROM
      `airflow-cloud-public-datasets.github_trends.github_daily_metrics`
    WHERE _PARTITIONTIME BETWEEN TIMESTAMP("{{ macros.ds_add(ds, -27) }}")
    AND TIMESTAMP("{{ yesterday_ds }}")
    GROUP BY
      date,
      repo
    ''',
    destination_dataset_table='airflow-cloud-public-datasets.github_trends.github_agg${{ yesterday_ds_nodash }}',
    dag=dag)

t5 = BigQueryOperator(
    task_id='bq_write_to_hackernews_agg',
    use_legacy_sql=False,
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    bql='''
    #standardSQL
    SELECT
      FORMAT_TIMESTAMP("%Y%m%d", timestamp) AS date,
      `by` AS submitter,
      id as story_id,
      REGEXP_EXTRACT(url, "(https?://github.com/[^/]*/[^/#?]*)") as url,
      SUM(score) as score
    FROM
      `bigquery-public-data.hacker_news.full`
    WHERE
      type = 'story'
      AND timestamp>'{{ yesterday_ds }}'
      AND timestamp<'{{ ds }}'
      AND url LIKE '%https://github.com%'
      AND url NOT LIKE '%github.com/blog/%'
    GROUP BY
      date,
      submitter,
      story_id,
      url
    ''',
    destination_dataset_table='airflow-cloud-public-datasets.github_trends.hackernews_agg${{ yesterday_ds_nodash }}',
    dag=dag)

t6 = BigQueryOperator(
    task_id='bq_write_to_hackernews_github_agg',
    use_legacy_sql=False,
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    bql='''
    #standardSQL
    SELECT
    a.date as date,
    a.url as github_url,
    b.repo as github_repo,
    a.score as hn_score,
    a.story_id as hn_story_id,
    b.stars_last_28_days as stars_last_28_days,
    b.stars_last_7_days as stars_last_7_days,
    b.stars_last_1_day as stars_last_1_day,
    b.forks_last_28_days as forks_last_28_days,
    b.forks_last_7_days as forks_last_7_days,
    b.forks_last_1_day as forks_last_1_day
    FROM
    (SELECT
      *
    FROM
      `airflow-cloud-public-datasets.github_trends.hackernews_agg`
      WHERE _PARTITIONTIME BETWEEN TIMESTAMP("{{ yesterday_ds }}") AND TIMESTAMP("{{ yesterday_ds }}")
      )as a
    LEFT JOIN
      (
      SELECT
      repo,
      CONCAT('https://github.com/', repo) as url,
      stars_last_28_days,
      stars_last_7_days,
      stars_last_1_day,
      forks_last_28_days,
      forks_last_7_days,
      forks_last_1_day
      FROM
      `airflow-cloud-public-datasets.github_trends.github_agg`
      WHERE _PARTITIONTIME BETWEEN TIMESTAMP("{{ yesterday_ds }}") AND TIMESTAMP("{{ yesterday_ds }}")
      ) as b
    ON a.url = b.url
    ''',
    destination_dataset_table='airflow-cloud-public-datasets.github_trends.hackernews_github_agg${{ yesterday_ds_nodash }}',
    dag=dag)

t7 = BigQueryCheckOperator(
    task_id='bq_check_hackernews_github_agg',
    sql='''
    #legacySql
    SELECT
    partition_id
    FROM
    [airflow-cloud-public-datasets:github_trends.hackernews_github_agg$__PARTITIONS_SUMMARY__]
    WHERE partition_id = "{{ yesterday_ds_nodash }}"
    ''',
    dag=dag)

t3.set_upstream(t1)
t4.set_upstream(t3)
t5.set_upstream(t2)
t6.set_upstream(t4)
t6.set_upstream(t5)
t7.set_upstream(t6)

