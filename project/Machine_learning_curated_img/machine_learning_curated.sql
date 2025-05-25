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
  'CreatedByJobRun'='jr_3deb31c2f872e09416e940d88ee5378c0deab105df0c9616499820b1596031c4', 
  'classification'='json')