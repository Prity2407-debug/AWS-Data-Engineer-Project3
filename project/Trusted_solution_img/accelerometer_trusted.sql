CREATE EXTERNAL TABLE `accelerometer_trusted`(
  `user` string COMMENT 'from deserializer', 
  `timestamp` bigint COMMENT 'from deserializer', 
  `x` double COMMENT 'from deserializer', 
  `y` double COMMENT 'from deserializer', 
  `z` double COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://parent-datalake/accelerometer/trusted/'
TBLPROPERTIES (
  'CreatedByJob'='accelerometer_trusted_glue_job-copy', 
  'CreatedByJobRun'='jr_5b7afe0b144d50233952cd03abdb89d2e39e80911253ef2bb552792cff2f257b', 
  'classification'='json')