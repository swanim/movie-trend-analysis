/*
Create temp table
Atherna를 사용하여 매일 업데이트 되는 csv 파일을 임시로 저장하는 테이블 생성
- movie_codes_temp
- movie_company_temp
- movie_director_temp
- movie_genre_temp
- movie_grade_temp
- movie_sales_temp
- movie_summary_temp
- movie_sales_pred_temp
*/
CREATE EXTERNAL TABLE IF NOT EXISTS movie_codes_temp(
    `movieCd` varchar(10),
    `movieNm` varchar(150))
LOCATION 's3://team3-project3-bucket/daily/temp/movie_codes_temp'

CREATE EXTERNAL TABLE IF NOT EXISTS movie_director_temp(
  `movieCd` varchar(10),
  `directors` varchar(50))
LOCATION 's3://team3-project3-bucket/daily/temp/movie_director_temp/';

CREATE EXTERNAL TABLE IF NOT EXISTS movie_company_temp(
  `movieCd` varchar(10),
  `companys` varchar(100))
LOCATION 's3://team3-project3-bucket/daily/temp/movie_company_temp/';

CREATE EXTERNAL TABLE IF NOT EXISTS movie_genre_temp(
    `movieCd` varchar(10),
    `genres` varchar(20))
 LOCATION 's3://team3-project3-bucket/daily/temp/movie_genre_temp'

CREATE EXTERNAL TABLE IF NOT EXISTS movie_grade_temp(
  `movieCd` varchar(10),
  `audits` varchar(80))
LOCATION 's3://team3-project3-bucket/daily/temp/movie_grade_temp/';

CREATE EXTERNAL TABLE IF NOT EXISTS movie_summary_temp(
  `movieCd` int,
  `movieNm` varchar(150),
  `movieNmEn` varchar(150),
  `showTm` int,
  `prdtYear` varchar(10),
  `openDt` date,
  `actors` varchar(50)
  )
LOCATION 's3://team3-project3-bucket/daily/temp/movie_summary_temp/';

CREATE EXTERNAL TABLE IF NOT EXISTS movie_sales_temp(
  `date` date,
  `rank` int,
  `rankInten` int,
  `rankOldAndNew` varchar(10),
  `movieCd` int,
  `movieNm` varchar(150),
  `openDt` date,
  `salesAmt` float,
  `salesShare` float,
  `salesInten` float,
  `salesChange` float,
  `salesAcc` float,
  `audiCnt` int,
  `audiInten` int,
  `audiChange` float,
  `audiAcc` int,
  `scrnCnt` int,
  `showCnt` int)
LOCATION 's3://team3-project3-bucket/daily/temp/movie_sales_temp'

-- movie_sales_temp에 showingDays 칼럼 추가
ALTER TABLE movie_sales_temp ADD COLUMNS (showingDays FLOAT);


CREATE EXTERNAL TABLE `movie_sales_pred_00f8fd0f4f6feb0efbaebc50c4180664`(
  `date` bigint, 
  `rank` bigint, 
  `rankinten` bigint, 
  `rankoldandnew` string, 
  `moviecd` bigint, 
  `movienm` string, 
  `opendt` string, 
  `salesamt` bigint, 
  `salesshare` double, 
  `salesinten` bigint, 
  `saleschange` double, 
  `salesacc` bigint, 
  `audicnt` bigint, 
  `audiinten` bigint, 
  `audichange` double, 
  `audiacc` bigint, 
  `scrncnt` bigint, 
  `showcnt` bigint, 
  `salespred` bigint)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://team3-project3-bucket/daily/temp/movie_sales_pred/'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='movie_sales_pred_temp', 
  'areColumnsQuoted'='false', 
  'averageRecordSize'='147', 
  'classification'='csv', 
  'columnsOrdered'='true', 
  'compressionType'='none', 
  'delimiter'=',', 
  'objectCount'='1', 
  'recordCount'='20', 
  'sizeKey'='2967', 
  'skip.header.line.count'='1', 
  'typeOfData'='file')
