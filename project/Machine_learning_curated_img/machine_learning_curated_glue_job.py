import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1748053519554 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1748053519554")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1748055262716 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_node1748055262716")

# Script generated for node SQL Query
SqlQuery556 = '''
select accelerometer_trusted.user, accelerometer_trusted.timestamp, accelerometer_trusted.x, 
accelerometer_trusted.y, accelerometer_trusted.z,step_trainer_trusted.distanceFromObject
from step_trainer_trusted
INNER JOIN accelerometer_trusted
ON accelerometer_trusted.timestamp=step_trainer_trusted.sensorReadingTime;
'''
SQLQuery_node1748055347301 = sparkSqlQuery(glueContext, query = SqlQuery556, mapping = {"step_trainer_trusted":step_trainer_trusted_node1748055262716, "accelerometer_trusted":accelerometer_trusted_node1748053519554}, transformation_ctx = "SQLQuery_node1748055347301")

# Script generated for node machine_learning_curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1748055347301, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1748052665957", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
machine_learning_curated_node1748055456303 = glueContext.getSink(path="s3://parent-datalake/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="machine_learning_curated_node1748055456303")
machine_learning_curated_node1748055456303.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="machine_learning_curated")
machine_learning_curated_node1748055456303.setFormat("json")
machine_learning_curated_node1748055456303.writeFrame(SQLQuery_node1748055347301)
job.commit()