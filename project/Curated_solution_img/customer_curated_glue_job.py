import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

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

# Script generated for node Customer_trusted
Customer_trusted_node1747967685701 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="customer_trusted", transformation_ctx="Customer_trusted_node1747967685701")

# Script generated for node Accelerometer_Landing
Accelerometer_Landing_node1747968715545 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="accelerometer_landing", transformation_ctx="Accelerometer_Landing_node1747968715545")

# Script generated for node SQL Query
SqlQuery493 = '''
SELECT DISTINCT *
FROM myDataSource1 c
JOIN myDataSource2 a
  ON c.email = a.user
WHERE c.shareWithResearchAsOfDate IS NOT NULL
  AND a.timestamp >= c.shareWithResearchAsOfDate;
'''
SQLQuery_node1747970405098 = sparkSqlQuery(glueContext, query = SqlQuery493, mapping = {"myDataSource1":Customer_trusted_node1747967685701, "myDataSource2":Accelerometer_Landing_node1747968715545}, transformation_ctx = "SQLQuery_node1747970405098")

# Script generated for node Drop Duplicates
DropDuplicates_node1747972234941 =  DynamicFrame.fromDF(SQLQuery_node1747970405098.toDF().dropDuplicates(["serialNumber"]), glueContext, "DropDuplicates_node1747972234941")

# Script generated for node Drop Fields
DropFields_node1747972248359 = DropFields.apply(frame=DropDuplicates_node1747972234941, paths=["user", "timestamp", "x", "y", "z"], transformation_ctx="DropFields_node1747972248359")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropFields_node1747972248359, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1747967341459", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1747972261425 = glueContext.getSink(path="s3://parent-datalake/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1747972261425")
AmazonS3_node1747972261425.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="customer_curated")
AmazonS3_node1747972261425.setFormat("json")
AmazonS3_node1747972261425.writeFrame(DropFields_node1747972248359)
job.commit()