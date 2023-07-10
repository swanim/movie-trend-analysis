from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from plugins import s3, get_movie_data

DAG_ID = 'Fetch_and_Upload_Data_to_S3'


default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    DAG_ID,
    schedule_interval=None,
    start_date=datetime(2022, 3, 17),
    catchup=False,
    tags=["medium", "backup"],
    default_args=default_args
) as dag:

    get_movie_data_task = PythonOperator(
        task_id='fetch_movie_data',
        python_callable=get_movie_data.get_data
    )

    upload_to_s3_task = PythonOperator(
        task_id='copy_to_s3',
        python_callable=s3.copy_to_s3,
        op_args=[get_movie_data_task.output]
    )

    get_movie_data_task >> upload_to_s3_task
