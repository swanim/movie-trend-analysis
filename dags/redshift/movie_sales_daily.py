from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.decorators import task
from datetime import datetime
from datetime import timedelta
import psycopg2

schema = "raw_data"
table = "movie_sale_temp"
s3_bucket = "team3-project3-bucket"
s3_key = "daily/movie_sales"   

copy_task = S3ToRedshiftOperator(
    task_id = 's3_to_redshift_temp_table',
    s3_bucket = s3_bucket,
    s3_key = s3_key,
    schema = schema,
    table = table,
    copy_options=['csv', 'IGNOREHEADER 1', 'dateformat \'auto\''],
    redshift_conn_id = "redshift_dev_db",
    aws_conn_id = "aws_s3_conn_id",    
    method = "REPLACE"
)

# Redshift connection 함수
def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()
# Trnasform task

@task
def transform(**context):
    sql = """

    -- 컬럼 추가
    ALTER TABLE raw_data.movie_sale_temp
    ADD COLUMN showingDays FLOAT;
    -- showingDays 값 처리
    BEGIN;
    UPDATE raw_data.movie_sale_temp
    SET showingDays = DATEDIFF('day', openDt, date) + 1
    WHERE DATEDIFF('day', openDt, date) > 0;
    DELETE FROM raw_data.movie_sale_temp
    WHERE DATEDIFF('day', openDt, date) <= 0 OR showingDays IS NULL;
    COMMIT;
    -- temp 값을 sale과 LEFT JOIN 해서 중복값 제거
    INSERT INTO raw_data.movie_sale
    SELECT DISTINCT t.*
    FROM raw_data.movie_sale_temp t
    LEFT JOIN raw_data.movie_sale s ON t.movieCd = s.movieCd AND t.date = s.date
    WHERE s.movieCd IS NULL AND s.date IS NULL;
    -- temp 컬럼 삭제
    ALTER TABLE raw_data.movie_sale_temp
    DROP COLUMN showingDays;
    """
    cur = get_Redshift_connection()
    try :
        cur.execute("BEGIN;")
        cur.execute(sql)
        cur.execute("COMMIT;")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        #cur.execute("ROLLBACK;") 
        raise

with DAG(
    dag_id='movie_sale_temp',
    start_date=datetime(2023, 6, 24),
    schedule_interval='0 11 * * *',
    max_active_runs=1,
    catchup=False,
    default_args={
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    }
) as dag:
    copy_task >> transform()