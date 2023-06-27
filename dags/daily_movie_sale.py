from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from datetime import timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

import logging
from plugins import slack

dag = DAG(
    dag_id = 'S3_to_Redshift',
    start_date = datetime(2023,6,23), 
    schedule = '0 9 * * *',  
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
        'on_failure_callback': slack.on_failure_callback,
    }
)

schema = "adhoc"
table = "prediction_result"
s3_bucket = "de-project3-bucket"
s3_key = schema+"/"+table

s3_to_redshift = S3ToRedshiftOperator(
    task_id = 's3_to_redshift',
    s3_bucket = s3_bucket,
    s3_key= s3_key,
    schema = schema, 
    table = table,
    copy_options=['csv'],
    method = 'REPLACE', 
    upsert_keys = ['movieCd'],
    redshift_conn_id = "redshift_dev_db",
    aws_conn_id = "aws_conn_id",
    dag = dag
)

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

@task
def add_showingdays():
    logging.info("load started")
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        cur.execute("ALTER TABLE raw_data.movie_sale_temp ADD COLUMN showingDays FLOAT;")
        cur.execute("UPDATE raw_data.movie_sale_temp SET showingDays = DATEDIFF('day', openDt, date) + 1 WHERE DATEDIFF('day', openDt, date) > 0;")
        cur.execute("DELETE FROM raw_data.movie_sale_temp WHERE DATEDIFF('day', openDt, date) <= 0 OR showingDays IS NULL;")
        cur.execute("END;")
    except Exception as error:
        cur.execute("ROLLBACK;")
        raise
    logging.info("load done")

@task
def update():
    logging.info("load started")
    cur = get_Redshift_connection()
    cur.execute("""INSERT INTO raw_data.movie_sale
SELECT DISTINCT t.*
FROM raw_data.movie_sale_temp t
LEFT JOIN raw_data.movie_sale s ON t.movieCd = s.movieCd AND t.date = s.date
WHERE s.movieCd IS NULL AND s.date IS NULL;""")
    cur.execute("ALTER TABLE raw_data.movie_sale_temp DROP COLUMN showingDays;")


s3_to_redshift >> add_showingdays >> update