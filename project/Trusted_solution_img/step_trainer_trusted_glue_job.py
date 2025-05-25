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

# Script generated for node Step_trainer_landing
Step_trainer_landing_node1748043895523 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="step_trainer_landinglanding", transformation_ctx="Step_trainer_landing_node1748043895523")

# Script generated for node Customer_curated
Customer_curated_node1748043875256 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="customer_curated", transformation_ctx="Customer_curated_node1748043875256")

# Script generated for node SQL Query
SqlQuery598 = '''
SELECT 
  myDataSource1.serialNumber,
  myDataSource1.sensorReadingTime,
  myDataSource1.distanceFromObject
FROM myDataSource1
JOIN myDataSource2
  ON myDataSource1.serialNumber = myDataSource2.serialNumber
'''
SQLQuery_node1748045384308 = sparkSqlQuery(glueContext, query = SqlQuery598, mapping = {"myDataSource1":Step_trainer_landing_node1748043895523, "myDataSource2":Customer_curated_node1748043875256}, transformation_ctx = "SQLQuery_node1748045384308")

# Script generated for node step_trainer_trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1748045384308, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1748045036751", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
step_trainer_trusted_node1748045738229 = glueContext.getSink(path="s3://parent-datalake/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="step_trainer_trusted_node1748045738229")
step_trainer_trusted_node1748045738229.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="step_trainer_trusted")
step_trainer_trusted_node1748045738229.setFormat("json")
step_trainer_trusted_node1748045738229.writeFrame(SQLQuery_node1748045384308)
job.commit()