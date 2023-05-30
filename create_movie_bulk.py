from datetime import datetime, timedelta
import requests
import pandas as pd

def get_movie_codes():
    # 초기 데이터 세팅
    api_key = 'fab5cb6fcba4e18cd3cfd2f1167ce9d1'
    api_url = 'http://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json'
    start_date = datetime.now().date() - timedelta(days=90)

    # 매일 업데이트 되지만 안전하게 -2일로 설정
    end_date = datetime.now().date() - timedelta(days=2)
    item_per_page='10'
    multi_movie_yn=None
    rep_nation_cd=None
    wide_area_cd=None

    movie_codes = pd.DataFrame()

    # 영화 상영 정보와 레퍼런스 테이블 생성 
    res = pd.DataFrame()
    target_date = start_date
    
    while target_date <= end_date:
        print("uploading movie sales data... " + target_date.strftime('%Y%m%d') + " done")
        # API 호출을 위한 파라미터 설정
        params = {
            'key': api_key,
            'targetDt': target_date.strftime('%Y%m%d'),
            'itemPerPage': item_per_page,
            'multiMovieYn': multi_movie_yn,
            'repNationCd': rep_nation_cd,
            'wideAreaCd': wide_area_cd
        }
        try:
            # API 호출
            response = requests.get(api_url, params=params, timeout=60)

            if response.status_code == 200:
                # JSON 데이터 파싱
                json_data = response.json()
                df = pd.DataFrame(json_data['boxOfficeResult']['dailyBoxOfficeList'])
                res = pd.concat([res, df])

        except Exception as error:
                print("API 호출 중 오류가 발생하였습니다.")
                print(str(error))

        target_date += timedelta(days=1)
    res = res.reset_index(drop=True).drop(['rnum'], axis=1)
    res.to_csv('movie_sale.csv', index=False)
    movie_codes= res[['movieNm', 'movieCd']].drop_duplicates()
    movie_codes.to_csv('movie_codes.csv', index=False)
    return movie_codes


def get_movie_details(movie_codes):
    movie_summary = pd.DataFrame()
    movie_genre = pd.DataFrame()
    movie_director = pd.DataFrame()
    movie_company = pd.DataFrame()
    movie_grade = pd.DataFrame()
    # 영화 코드 별 상세 정보 테이블 추출
    api_url = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieInfo.json"
    api_key = "fab5cb6fcba4e18cd3cfd2f1167ce9d1"  # Enter your API key here
    index = 1
    for movieCd in movie_codes["movieCd"]:
        print("loading movie details... " + str(index) + "/" + str(len(movie_codes)) + " done")
        # API 호출
        params = {
            'key': api_key,
            'movieCd': movieCd,
        }
        try:
            # API 호출
            response = requests.get(api_url, params=params, timeout=60)
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

    movie_summary.to_csv('movie_summary.csv', index=False)
    movie_genre.to_csv('movie_genre.csv', index=False)
    movie_director.to_csv('movie_director.csv', index=False)
    movie_company.to_csv('movie_company.csv', index=False)
    movie_grade.to_csv('movie_grade.csv', index=False)


def run():
    movie_codes = get_movie_codes()
    get_movie_details(movie_codes)
    
def main():
    run()    
    
if __name__ == "__main__":
    main()