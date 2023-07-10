# 영화 트렌드 분석
## 개요

영화 진흥위원회 오픈 [API](!https://www.kobis.or.kr/kobisopenapi/homepg/apiservice/searchServiceInfo.do) 를 활용하여 영화 트렌드를 분석. 


`[일별 박스 오피스, 영화 상세정보, 영화사 상세정보]`를 추출해서 정규화를 통해 총 6개의 테이블을 만들었다.

`private_subnet/create_daily_movie_data.py` 파일을 이용하여 JSON 형태의 데이터에서 다음과 같은 테이블을 만들었다.

![데이터 구조](https://github.com/data-dev-course/project2-team2/assets/43350428/091c5395-01e9-4b1d-966d-a68df9469480)

- movie_codes: 영화 코드와 영화명
- movie_companies: 영화사 코드와 영화사명
- movie_sales: 일별 박스오피스 데이터
- movie_summary: 영화 상세정보 데이터
- movie_grade: 영화 시청등급 정보
- movie_directors: 영화 감독 정보

## 팀 구성
|    | 이동수 | 조윤지 | 박주미 | 김현지 | 명우성 |
| :---: | :---: | :---: | :---: | :---: |:---: |
|GitHub| [@TonysHub](https://github.com/TonysHub) | [@joyunji](https://github.com/joyunji) | [@swanim](https://github.com/swanim) | [@hyeonji32](https://github.com/hyeonji32) | [@LameloBally](https://github.com/LameloBally)

## 아키텍쳐
![Architecture](https://github.com/data-dev-course/project2-team2/assets/43350428/a54e009f-0ec3-4b9d-8311-f5f19a28e08c)

#### 간략한 설명

1. 프라이빗 서브넷에 있는 EC2 인스턴스가 매일 저녁 7시(KST)에 cronjob을 통해 API Request를 보냄 (보안을 위해 NAT gateway 사용)
2. Response로 받은 데이터를 전처리, `sagemaker` 폴더와 `private_subnet/create_daily_movie_data.py` 참조
3. 필요한 데이터 정제 후, S3 버킷에 적재 (같은 region의 버킷임으로 Gateway Endpoint 적용)
4. Sagemaker 또한 같은 시기에 적용됨, 매일 받아온 데이터를 기준으로 예상 매출 정보 column 추가
5. Redshift Serverless의 쿼리 스케줄링으로 EventBridge를 initiate, 매일 저녁 9시에 S3에 있는 데이터를 웨어하우스에 INSERT
6. 데이터 추가가 끝나면, ELT를 통해 대시보드에 필요한 데이터만 `analytics_table`에 추가
7. 퍼블릭 서브넷에 있는 수퍼셋 대시보드에 데이터를 연결
8. 이때, ElastiCache를 이용하여 Redshift의 쿼리양과 Rpu 소모를 줄여줌
9. 대시보드는 인터넷 게이트웨이에 연결되어 있는 ElasticIP를 통해 웹과 소통 가능
10. 현재는 보안과 SQL injection의 위험으로, 각 개발자의 IP에서만 접근 가능하게 설정


## 시연

https://github.com/data-dev-course/project2-team2/assets/43350428/cacf590c-0cd1-4d47-9dda-e9244d11d6d9


