from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator


DAG_ID = 'Insert_and_Upload_Acc_Data_to_S3'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retry_delay': timedelta(minutes=5)
}


def append_data():
    athena_query = "INSERT INTO movie_sales_pred SELECT * FROM movie_sales_pred_00f8fd0f4f6feb0efbaebc50c4180664;"
    athena_operator = AthenaOperator(
        task_id='run_athena_query',
        query=athena_query,
        database='athena_db',
        output_location='s3://team3-athena-results/accumulated_data/movie_sales_pred/',
        aws_conn_id='aws_s3_conn_id'
    )
    athena_operator.execute(context={})


with DAG(
    DAG_ID,
    schedule_interval=None,
    start_date=datetime(2022, 6, 22),
    catchup=False,
    tags=["medium"],
    default_args=default_args
) as dag:

    append_daily_data_task = PythonOperator(
        task_id='append_daily_to_acc_data',
        python_callable=append_data
    )


append_daily_data_task
