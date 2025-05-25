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

# Script generated for node Customer_trusted
Customer_trusted_node1747967685701 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="customer_trusted", transformation_ctx="Customer_trusted_node1747967685701")

# Script generated for node Accelerometer_trusted
Accelerometer_trusted_node1747968715545 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="accelerometer_trusted", transformation_ctx="Accelerometer_trusted_node1747968715545")

# Script generated for node SQL Query
SqlQuery539 = '''
SELECT DISTINCT 
  myDataSource1.customerName,
  myDataSource1.email,
  myDataSource1.phone,
  myDataSource1.birthDay,
  myDataSource1.serialNumber,
  myDataSource1.registrationDate,
  myDataSource1.lastUpdateDate,
  myDataSource1.shareWithResearchAsOfDate,
  myDataSource1.shareWithPublicAsOfDate,
  myDataSource1.shareWithFriendsAsOfDate
FROM myDataSource1
JOIN myDataSource2
  ON myDataSource1.email = myDataSource2.user
WHERE myDataSource1.shareWithResearchAsOfDate IS NOT NULL
  AND myDataSource2.timestamp >= myDataSource1.shareWithResearchAsOfDate;
'''
SQLQuery_node1747970405098 = sparkSqlQuery(glueContext, query = SqlQuery539, mapping = {"myDataSource1":Customer_trusted_node1747967685701, "myDataSource2":Accelerometer_trusted_node1747968715545}, transformation_ctx = "SQLQuery_node1747970405098")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1747970405098, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1747967341459", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1747972261425 = glueContext.getSink(path="s3://parent-datalake/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1747972261425")
AmazonS3_node1747972261425.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="customer_curated")
AmazonS3_node1747972261425.setFormat("json")
AmazonS3_node1747972261425.writeFrame(SQLQuery_node1747970405098)
job.commit()