# 영화 트렌드 분석 V2

## 개요

[영화 트렌드 분석](https://github.com/data-dev-course/project2-team2) 프로젝트에서 CronJob, Amazon EventBridge 부분을 Airflow DAG를 활용해 고도화

- 수행 기간 : 2023.06.26 - 2023.06.30
- AWS 서비스를 활용한 DL → DW 작업
- Airflow를 이용한 데이터 파이프라인 구축
- Sklearn을 통해 내일 예상 매출액 획득

<br/>

## 팀 구성
|    | 박주미 | 이동수 | 조윤지 | 명우성 | 김현지 |
| :---: | :---: | :---: | :---: | :---: |:---: |
|GitHub| [@swanim](https://github.com/swanim) | [@TonysHub](https://github.com/TonysHub) | [@joyunji](https://github.com/joyunji) | [@LameloBally](https://github.com/LameloBally) | [@hyeonji32](https://github.com/hyeonji32)


<br/>

## 기술 스택

| 분야 | stack |
| --- | --- |
| cloud | AWS |
| machine learning | sklearn |
| data pipeline | airflow |
| container | docker |
| 협업 툴 | Trello, Slack, Github |
<br/>

## 아키텍처
![프로젝트3](https://github.com/data-dev-course/project2-airflow/assets/64563859/17e3ae63-7483-45e7-8174-a11f2dc5d4b7)


<br/>

### 아키텍처 상세 설명

1. 매일 api로 가져온 데이터를 적절히 변환하고 s3 버킷에 적재
2. s3(raw_data)버킷에 매일 올라오는 데이터를 바탕으로 머신러닝을 실행. 이를 통해 얻은 예상 매출액을 s3(raw_data)버킷의 movie_sales_pred 폴더에다 업로드
3. Data Lake → Data Warehouse
    - daily로 들어오는 데이터를 원본 테이블과 합쳐 저장하기 위한 목적
    1. S3 → Amazon Redshift
    2. S3(raw data) → glue Crawlers(테이블 생성) → Amazon Athena(쿼리 실행) → S3(accumulated_data) 
    - 특이 사항 : 처음에는 1번으로 진행했는데 비용이 많이 나가서 2번 방법으로 구축