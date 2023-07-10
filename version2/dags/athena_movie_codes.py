from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 6, 27),
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

with DAG("athena_movie_codes", default_args=default_args, schedule_interval= '@once') as dag:

    insert = AthenaOperator(
        task_id='insert',
        database='athena_db',
        query='''INSERT INTO "athena_db"."movie_codes"
SELECT DISTINCT t.*
FROM "athena_db"."movie_codes_temp" t
LEFT JOIN "athena_db"."movie_codes" s ON t.movieCd = s.movieCd
WHERE s.movieCd IS NULL;''',
        output_location='s3://team3-athena-results/accumulated_data/movie_codes/query_results/',
        aws_conn_id='aws_conn_id'
    )

    
    save = AthenaOperator(
        task_id='save_table_to_s3',
        database='athana_db',
        query='SELECT * FROM  "athena_db"."movie_codes";',
        output_location='s3://team3-athena-results/accumulated_data/movie_codes/movie_codes_result/',
        aws_conn_id='aws_conn_id'
    ) 
    
    insert >> save 

