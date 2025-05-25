CREATE EXTERNAL TABLE `step_trainer_trusted`(
  `serialnumber` string COMMENT 'from deserializer', 
  `sensorreadingtime` bigint COMMENT 'from deserializer', 
  `distancefromobject` int COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://parent-datalake/step_trainer/trusted/'
TBLPROPERTIES (
  'CreatedByJob'='step_trainer_trusted_glue_job', 
  'CreatedByJobRun'='jr_85593595359a40653f908057601bfe2426e1a7fe074fd0021186d66ad9fce4fa', 
  'classification'='json')