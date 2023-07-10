from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.models import Variable

with DAG(dag_id="movie_director_daily",
    start_date=datetime(2023, 6, 23),
    schedule='@daily',
    catchup=True) as dag:

    schema = "raw_data"
    table = "movie_director"
    s3_bucket = "team3-project3-bucket"
    s3_key = "daily"+"/"+table 

    s3_to_redshift= S3ToRedshiftOperator(
    task_id = 's3_to_redshift',
    s3_bucket = s3_bucket,
    s3_key = s3_key,
    schema = schema, 
    table = table,
    copy_options=['csv', 'IGNOREHEADER 1','dateformat \'auto\'','removequotes'],
    method = 'UPSERT', 
    upsert_keys = ['movieCd'],
    redshift_conn_id = "redshift_dev_db",
    aws_conn_id = "aws_conn_id",
    dag = dag
)