from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import pandas as pd
from plugins import s3

API_KEY = "7a018f527dedab08db78749a2a2fdeca"  # 영화 진흥회 API_KEY임으로 .env 사용 안함
MOVIE_CODE_URL = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json"
MOVIE_DETAIL_URL = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieInfo.json"
DAG_ID = 'Fetch_and_Upload_Data_to_S3'


def get_data():
    movie_codes, movie_sales = get_movie_codes_and_sales()
    movie_summary, movie_genre, movie_director, movie_grade, movie_company = get_movie_details(movie_codes)
    return (
        ('movie_codes', movie_codes.to_csv(index=False)),
        ('movie_sales', movie_sales.to_csv(index=False)),
        ('movie_summary', movie_summary.to_csv(index=False)),
        ('movie_genre', movie_genre.to_csv(index=False)),
        ('movie_director', movie_director.to_csv(index=False)),
        ('movie_grade', movie_grade.to_csv(index=False)),
        ('movie_company', movie_company.to_csv(index=False))
    )


def get_movie_codes_and_sales():
    api_key = API_KEY
    movie_code_url = MOVIE_CODE_URL
    start_date = datetime.now() - timedelta(days=2)
    end_date = datetime.now() - timedelta(days=1)
    target_date = start_date
    multi_movie_yn = None
    rep_nation_cd = None
    wide_area_cd = None
    item_per_page = 10
    movie_codes = pd.DataFrame()
    res = pd.DataFrame()

    while target_date < end_date:
        params = {
            'key': api_key,
            'targetDt': target_date.strftime('%Y%m%d'),
            'multiMovieYn': multi_movie_yn,
            'repNationCd': rep_nation_cd,
            'wideAreaCd': wide_area_cd,
            'itemPerPage': item_per_page
        }
        try:
            response = requests.get(movie_code_url, params=params, timeout=60)

            if response.status_code == 200:
                print("uploading movie movie sales data... " + target_date.strftime('%Y%m%d') + " done")
                json_data = response.json()
                df = pd.DataFrame(json_data['boxOfficeResult']['dailyBoxOfficeList'])
                df['date'] = target_date.strftime('%Y%m%d')
                res = pd.concat([res, df])

        except Exception as e:
            print("API 호출 중 오류가 발생하였습니다.")
            print(str(e))
            pass

        target_date += timedelta(days=1)

    movie_sales = res.reset_index(drop=True).drop(['rnum'], axis=1)
    date_column = res.pop('date')
    res.insert(0, 'date', date_column)
    movie_codes = res[['movieCd', 'movieNm']].drop_duplicates()

    return movie_codes, movie_sales


def get_movie_details(movie_codes):
    movie_summary = pd.DataFrame()
    movie_genre = pd.DataFrame()
    movie_director = pd.DataFrame()
    movie_company = pd.DataFrame()
    movie_grade = pd.DataFrame()
    # 영화 코드 별 상세 정보 테이블 추출
    movie_detail_url = MOVIE_DETAIL_URL
    api_key = API_KEY
    index = 1
    for movieCd in movie_codes["movieCd"]:
        print("uploading movie details... " + str(index) + "/" + str(len(movie_codes)) + " done")
        # API 호출
        params = {
            'key': api_key,
            'movieCd': movieCd,
        }
        try:
            # API 호출
            response = requests.get(movie_detail_url, params=params, timeout=60)
            if response.status_code == 200:
                # JSON 데이터 파싱
                json_data = response.json()
                df = pd.json_normalize(json_data['movieInfoResult']['movieInfo'])

                # 추후에 parameter based function으로 최적화
                if len(df['actors'][0]) > 0:
                    df_summary = df.explode(['actors'])
                    df_summary['actors'] = df_summary['actors'].apply(lambda x: x['peopleNm'])
                    movie_summary = pd.concat([movie_summary, df_summary[['movieCd', 'movieNm', 'movieNmEn', 'showTm', 'prdtYear', 'openDt', 'actors']]]).drop_duplicates()

                if len(df['genres'][0]) > 0:
                    df_genre = df.explode(['genres'])
                    df_genre['genres'] = df_genre['genres'].apply(lambda x: x['genreNm'])
                    movie_genre = pd.concat([movie_genre, df_genre[['movieCd', 'genres']]]).drop_duplicates()

                if len(df['directors'][0]) > 0:
                    df_director = df.explode(['directors'])
                    df_director['directors'] = df_director['directors'].apply(lambda x: x['peopleNm'])
                    movie_director = pd.concat([movie_director, df_director[['movieCd', 'directors']]]).drop_duplicates()

                if len(df['companys'][0]) > 0:
                    df_company = df.explode(['companys'])
                    df_company['companys'] = df_company['companys'].apply(lambda x: x['companyNm'])
                    movie_company = pd.concat([movie_company, df_company[['movieCd', 'companys']]]).drop_duplicates()

                if len(df['audits'][0]) > 0:
                    df_grade = df.explode(['audits'])
                    df_grade['audits'] = df_grade['audits'].apply(lambda x: x['watchGradeNm'])
                    movie_grade = pd.concat([movie_grade, df_grade[['movieCd', 'audits']]]).drop_duplicates()

        except Exception as error:
            print("movieCd: " + movieCd)
            print("API 호출 중 오류가 발생하였습니다.")
            print(str(error))
        index += 1

    return movie_summary, movie_genre, movie_director, movie_grade, movie_company


def copy_to_s3(dataframes):
    s3_bucket = Variable.get("s3_bucket_name")
    s3_folder = 'daily'
    for name, dataframe in dataframes:
        s3_key = '{}/{}.csv'.format(s3_folder, name)
        s3.upload_to_s3('aws_s3_conn_id', s3_bucket, s3_key, dataframe, replace=True)


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

    movie_code_url = MOVIE_CODE_URL
    movie_detail_url = MOVIE_DETAIL_URL

    get_movie_data_task = PythonOperator(
        task_id='fetch_movie_data',
        python_callable=get_data
    )

    upload_to_s3_task = PythonOperator(
        task_id='copy_to_s3',
        python_callable=copy_to_s3,
        op_args=[get_movie_data_task.output]
    )

    get_movie_data_task >> upload_to_s3_task
