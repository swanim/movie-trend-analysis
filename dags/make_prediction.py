from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
import boto3
import pandas as pd
import numpy as np
from io import StringIO
from sklearn.preprocessing import StandardScaler, OneHotEncoder
import logging
from xgboost import XGBRegressor
import os
from botocore.exceptions import NoCredentialsError
from joblib import dump, load


def extract(**context):
    
    # S3에 접근하기 위한 access_key_id와 secret_access_key
    aws_access_key_id = Variable.get('aws_access_key_id')
    aws_secret_access_key = Variable.get('aws_secret_access_key')
    
    session = boto3.Session(
        aws_access_key_id= aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    
    # sales 데이터 불러오기
    s3 = session.client('s3')
    obj = s3.get_object(Bucket="team3-project3-bucket", Key='daily/temp/movie_sales/' + context['params']['sales_name'])
    data = obj['Body'].read().decode('utf-8')
    sales = pd.read_csv(StringIO(data))
    
    # summary 데이터 불러와서 merge 하기
    obj = s3.get_object(Bucket="team3-project3-bucket", Key='daily/temp/movie_summary/' + context['params']['summary_name'])
    data = obj['Body'].read().decode('utf-8')
    summary = pd.read_csv(StringIO(data))
    summary_dedup = summary[['movieCd', 'showTm']].drop_duplicates('movieCd')
    sales = sales.merge(summary_dedup, on='movieCd', how='left')
    
    # genre 데이터 불러와서 merge 하기
    obj = s3.get_object(Bucket="team3-project3-bucket", Key='daily/temp/movie_genre/' + context['params']['genre_name'])
    data = obj['Body'].read().decode('utf-8')
    genre = pd.read_csv(StringIO(data))
    genre_dedup = genre[['movieCd', 'genres']].drop_duplicates('movieCd')
    sales = sales.merge(genre_dedup, on='movieCd', how='left')
    
    # grade 데이터 불러와서 merge 하기
    obj = s3.get_object(Bucket="team3-project3-bucket", Key='daily/temp/movie_grade/' + context['params']['grade_name'])
    data = obj['Body'].read().decode('utf-8')
    grade = pd.read_csv(StringIO(data))
    grade_dedup = grade[['movieCd', 'audits']].drop_duplicates('movieCd')
    sales = sales.merge(grade_dedup, on='movieCd', how='left')
    
    # 중복 데이터는 제거하기
    sales.dropna(inplace = True)
    sales.to_csv('/tmp/sales.csv', index=False)
    s3.upload_file('/tmp/sales.csv', 'team3-project3-bucket', 'sales.csv')


    return True



def transform(**context):
    logging.info("pre_processing started")
    # S3에 접근하기 위한 access_key_id와 secret_access_key

    aws_access_key_id = Variable.get('aws_access_key_id')
    aws_secret_access_key = Variable.get('aws_secret_access_key')
    
    session = boto3.Session(
        aws_access_key_id= aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    
    # sales 데이터 불러오기
    s3 = session.client('s3')

    s3.download_file('team3-project3-bucket', 'sales.csv', '/tmp/sales.csv')
    sales = pd.read_csv('/tmp/sales.csv')    
    # 특정 수치형 변수를 범주형 변수
    sales['rank'] = sales['rank'].astype('object')
    sales['rankInten'] = sales['rankInten'].astype('object')
    moviedate = pd.to_datetime(sales['date'].astype(str), format='%Y-%m-%d')
    sales['month'] = moviedate.dt.month
    seasons = {1: 'winter', 2: 'winter', 3: 'spring', 4: 'spring', 5: 'spring', 
               6: 'summer', 7: 'summer', 8: 'summer', 9: 'fall', 10: 'fall', 
               11: 'fall', 12: 'winter'}
    sales['season'] = sales['month'].map(seasons)
    sales = sales.drop('month', axis=1)
    # 불필요한(중복되는) 변수 제거
    sales.drop('date',axis=1,inplace = True)
    sales.drop('movieCd',axis = 1, inplace = True)
    sales.drop('salesInten',axis=1, inplace = True)
    sales.drop('audiInten', axis=1 , inplace = True)
    sales.drop_duplicates(inplace = True)
    # Covariance를 본 후 다중공성선 제거를 위한 변수 제거
    sales.drop(['audiCnt','showCnt','audiChange'],axis=1, inplace = True)
    sales.drop(['audiAcc'],axis=1,inplace = True)
    # Object Type 데이터 전처리
    sales.drop(['rank','rankInten'],axis=1 ,inplace = True)
    sales.drop(['movieNm','openDt'],axis=1 ,inplace = True)
    # Target 선정
    print(sales.info())
    X = sales
    numeric_features = ['salesAmt', 'salesShare', 'salesChange', 'salesAcc', 'scrnCnt', 'showTm']
    categorical_features = ['rankOldAndNew', 'genres', 'audits', 'season']
    X_num = X[numeric_features]
    X_cat = X[categorical_features]
    

    s3.download_file('team3-project3-bucket', 'scaler.joblib', '/tmp/scaler.joblib')
    s3.download_file('team3-project3-bucket', 'encoder.joblib', '/tmp/encoder.joblib')

    # Load the previously saved scaler and encoder
    from joblib import load

    scaler = load('/tmp/scaler.joblib')
    encoder = load('/tmp/encoder.joblib')

    # Numeric Variable Normalization
    X_num_scaled = pd.DataFrame(scaler.fit_transform(X_num), columns=X_num.columns)
    
    # 원핫인코딩 for Catagorical Variable
    X_cat_encoded = encoder.fit_transform(X_cat).toarray()
    
    # Getting the names of one-hot encoded columns
    encoded_features = list(encoder.get_feature_names_out(categorical_features))
    X_cat_encoded = pd.DataFrame(X_cat_encoded, columns=encoded_features)
    X_processed = pd.concat([X_num_scaled, X_cat_encoded], axis=1)

    X_processed.to_csv('/tmp/X_processed.csv', index=False)
    s3.upload_file('/tmp/X_processed.csv', 'team3-project3-bucket', 'X.csv')
    
    return True


def run_model(**context):
    logging.info('Running Model Started')
    # S3에 접근하기 위한 access_key_id와 secret_access_key
    aws_access_key_id = Variable.get('aws_access_key_id')
    aws_secret_access_key = Variable.get('aws_secret_access_key')
    
    session = boto3.Session(
        aws_access_key_id= aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    
    # sales 데이터 불러오기
    s3 = session.client('s3')
    s3.download_file('team3-project3-bucket', 'X.csv', '/tmp/X_processed.csv')
    X_processed = pd.read_csv('/tmp/X_processed.csv')    

    xgb_loaded = XGBRegressor()

    
    s3.download_file('team3-project3-bucket', 'xgb_best_model.json', '/tmp/xgb_best_model.json')
    xgb_loaded.load_model('/tmp/xgb_best_model.json')
    y_pred = xgb_loaded.predict(X_processed)

    
    new_df = pd.DataFrame(y_pred, columns=['Prediction'])

    # Creating 'Prediction Date' column and filling it with tomorrow's date
    tomorrow_date = datetime.now() + timedelta(days=1)
    new_df['Target Date'] = tomorrow_date.strftime('%Y-%m-%d')


    # If 'predictions.csv' exists, load it and append new data. Otherwise, just save new_df as 'predictions.csv'
    if os.path.isfile('predictions.csv'):
        df = pd.read_csv('predictions.csv')
        df = pd.concat([df, new_df])
    else:
        df = new_df

    df.to_csv('predictions.csv', index=False)
    
    return True



def load(**context):
    
    s3 = boto3.client('s3', aws_access_key_id=Variable.get("aws_access_key_id"), aws_secret_access_key=Variable.get("aws_secret_access_key"))

    filename = 'predictions.csv'
    bucket_name = 'team3-project3-bucket'
    object_name = 'predictions.csv'  

    try:
        s3.upload_file(filename, bucket_name, object_name)
        logging.info("Uploading")
    except NoCredentialsError:
        logging.error("No AWS credentials found")
        return False
    return True



default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}


with DAG(
    dag_id = 'prediction', 
    schedule_interval = '0 9 * * *',
    default_args=default_args,
    start_date=datetime(2023, 6, 28),
    catchup=False,
    tags=["high",'middle','middle','high'],
    max_active_runs = 10,
    max_active_tasks = 10
    ) as dag:

    get_data_task = PythonOperator(
        task_id='extract',
        python_callable= extract,
        params={'sales_name': 'movie_sales.csv',
                'summary_name':'movie_summary.csv', 'genre_name':'movie_genre.csv', 'grade_name':'movie_grade.csv'})
        
    
    process_data_task = PythonOperator(
        task_id = 'transform',
        python_callable = transform,
        )
    
    run_model_task = PythonOperator(
        task_id = 'run_model',
        python_callable = run_model,
        params={'sales_name': 'movie_sales.csv',
                'summary_name':'movie_summary.csv', 'genre_name':'movie_genre.csv', 'grade_name':'movie_grade.csv'}
        )
    
  
    upload_file_task = PythonOperator(
        task_id='load',
        python_callable= load)
    
    
    
    get_data_task >> process_data_task >> run_model_task >> upload_file_task
