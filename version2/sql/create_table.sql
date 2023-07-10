/*
Create table
Athena를 사용하여 s3에 daily로 업데이트되는 csv파일을 원본으로 테이블 생성
- movie_codes
- movie_company
- movie_director
- movie_genre
- movie_grade
- movie_sales
- movie_summary
- movie_sales_pred
*/

CREATE EXTERNAL TABLE IF NOT EXISTS `athena_db`.`movie_codes` (
  `movieCd` varchar(10),
  `movieNm` varchar(150)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://team3-athena-results/accumulated_data/movie_codes/movie_codes_table'
TBLPROPERTIES (
  'classification' = 'csv',
  'skip.header.line.count' = '1'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `athena_db`.`movie_director` (
  `movieCd` varchar(10),
  `directors` varchar(50)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://team3-athena-results/accumulated_data/movie_director/movie_director_table'
TBLPROPERTIES (
  'classification' = 'csv',
  'skip.header.line.count' = '1'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `athena_db`.`movie_company` (
  `movieCd` varchar(10),
  `companys` varchar(100)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://team3-athena-results/accumulated_data/movie_company/movie_company_table'
TBLPROPERTIES (
  'classification' = 'csv',
  'skip.header.line.count' = '1'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `athena_db`.`movie_grade` ( `movieCd` varchar(10), `audits` varchar(80)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://team3-athena-results/accumulated_data/movie_grade/movie_grade_table'
TBLPROPERTIES ( 'classification' = 'csv', 'skip.header.line.count' = '1'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `athena_db`.`movie_genre` ( `movieCd` varchar(10), `genres` varchar(20)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://team3-athena-results/accumulated_data/movie_genre/movie_genre_table'
TBLPROPERTIES ( 'classification' = 'csv', 'skip.header.line.count' = '1'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `athena_db`.`movie_summary` ( `movieCd` int, `movieNm` varchar(150), `movieNmEn` varchar(150), `showTm` int, `prdtYear` varchar(10), `openDt` date, `actors` varchar(50)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://team3-athena-results/accumulated_data/movie_summary/movie_summary_table'
TBLPROPERTIES ( 'classification' = 'csv', 'skip.header.line.count' = '1'
)

CREATE EXTERNAL TABLE IF NOT EXISTS `athena_db`.`movie_sales` (
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
  `showCnt` int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://team3-athena-results/accumulated_data/movie_sales/movie_sales_table'
TBLPROPERTIES (
  'classification' = 'csv',
  'skip.header.line.count' = '1'
);
-- movie_sales에 showingDays 칼럼 추가
ALTER TABLE movie_sales ADD COLUMNS (showingDays FLOAT);


CREATE EXTERNAL TABLE `movie_sales_pred`(
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
  's3://team3-athena-results/accumulated_data/movie_sales_pred/'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='movie_sales_pred', 
  'areColumnsQuoted'='false', 
  'averageRecordSize'='152', 
  'classification'='csv', 
  'columnsOrdered'='true', 
  'compressionType'='none', 
  'delimiter'=',', 
  'objectCount'='1', 
  'recordCount'='2', 
  'sizeKey'='331', 
  'skip.header.line.count'='1', 
  'typeOfData'='file')




