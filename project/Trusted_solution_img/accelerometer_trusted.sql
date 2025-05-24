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
  'CreatedByJobRun'='jr_355a2ffdf274b61fb6d950a0d32c5b4917270bb2bf74c9d2e39cc6faec6600e7', 
  'classification'='json')