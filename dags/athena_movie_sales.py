from airflow import DAG
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from datetime import datetime, timedelta
from airflow.models import Variable

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 6, 29),
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

with DAG("athena_movie_sales", default_args=default_args, schedule_interval= '@once') as dag:
    # ctas로 새로운 showingDays 추가해 새로운 테이블 작성
    ctas = AthenaOperator(
        task_id='ctas',
        database='athena_db',
        query="""
        CREATE TABLE movie_sales_ctas
        WITH (
            format = 'PARQUET',
            parquet_compression = 'SNAPPY',
            external_location = '{ctas_location}'
            ) AS
        SELECT rank, rankInten, rankOldAndNew, movieCd, movieNm, openDt, salesAmt, salesShare, salesInten, salesChange, salesAcc, audiCnt, audiInten, audiChange, audiAcc, scrnCnt, showCnt, date, date_diff('day', openDt, date) + 1 AS showingDays
        FROM movie_sales_temp
        WHERE date_diff('day', openDt, date) > 0;
        """.format(ctas_location=Variable.get("ctas_location")),
        output_location=Variable.get("query_results"),
        aws_conn_id='aws_s3_conn_id'
    )
    # 원본 테이블에 ctas 내용 insert
    insert_original = AthenaOperator(
        task_id='insert_original_table',
        database='athena_db',
        query="""
        INSERT INTO movie_sales
        SELECT DISTINCT t.*
        FROM movie_sales_ctas t
        LEFT JOIN movie_sales s ON t.movieCd = s.movieCd AND t.date = s.date
        WHERE s.movieCd IS NULL AND s.date IS NULL;
        """,
        output_location=Variable.get("query_results"),
        aws_conn_id='aws_s3_conn_id'
    )
    #ctas 테이블 삭제
    drop_ctas = AthenaOperator(
        task_id='drop_ctas_table',
        database='athena_db',
        query="""
        DROP TABLE athena_db.movie_sales_ctas;
        """,
        output_location=Variable.get("query_results"),
        aws_conn_id='aws_s3_conn_id'
    )
    # 원본 테이블 내용 s3에다 저장
    result = AthenaOperator(
        task_id='save_elt_results',
        database='athena_db',
        query='select * FROM "athena_db"."movie_sales"',
        output_location=Variable.get("movie_sales_result"),
        aws_conn_id='aws_s3_conn_id'
    )

    ctas >> insert_original >> drop_ctas >> result