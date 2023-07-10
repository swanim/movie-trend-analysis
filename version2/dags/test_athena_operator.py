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

with DAG("athena_query_test", default_args=default_args, schedule_interval= '@once') as dag:

    t1 = BashOperator(
        task_id='bash_test',
        bash_command='echo "Starting AthenaOperator TEST"'
    )

    run_query = AthenaOperator(
        task_id='run_query_test',
        database='athena_db',
        query='select * FROM "athena_db"."movie_sales"',
        output_location='s3://',
        aws_conn_id='aws_s3_conn_id'
    )

    
    t1 >> run_query