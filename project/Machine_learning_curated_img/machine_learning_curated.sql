CREATE EXTERNAL TABLE `machine_learning_curated`(
  `user` string COMMENT 'from deserializer', 
  `timestamp` bigint COMMENT 'from deserializer', 
  `x` double COMMENT 'from deserializer', 
  `y` double COMMENT 'from deserializer', 
  `z` double COMMENT 'from deserializer', 
  `distancefromobject` int COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://parent-datalake/machine_learning_curated/'
TBLPROPERTIES (
  'CreatedByJob'='machine_learning_curated_glue_job', 
  'CreatedByJobRun'='jr_314c45bd1c30e235ea16f93cbe64d6d62b269fc9f163b642345ef045d1b86417', 
  'classification'='json')