CREATE EXTERNAL TABLE `step_trainer_trusted`(
  `sensorreadingtime` bigint COMMENT 'from deserializer', 
  `serialnumber` string COMMENT 'from deserializer', 
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
  'CreatedByJobRun'='jr_d26bcec3767cfe9a501ac9546b34334bc0eb2a364609429a67485a92a98b912b', 
  'classification'='json')