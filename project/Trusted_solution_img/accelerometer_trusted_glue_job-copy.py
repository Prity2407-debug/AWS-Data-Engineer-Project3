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

# Script generated for node Accelerator_Landing
Accelerator_Landing_node1747967685701 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="accelerometer_landing", transformation_ctx="Accelerator_Landing_node1747967685701")

# Script generated for node Customer_Landing
Customer_Landing_node1747968715545 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="customer_landing", transformation_ctx="Customer_Landing_node1747968715545")

# Script generated for node SQL Query
SqlQuery529 = '''
SELECT * 
FROM myDataSource1
JOIN myDataSource2 
  ON myDataSource1.user = myDataSource2.email
WHERE myDataSource2.shareWithResearchAsOfDate IS NOT NULL
  AND myDataSource1.timestamp >= myDataSource2.shareWithResearchAsOfDate;
'''
SQLQuery_node1747970405098 = sparkSqlQuery(glueContext, query = SqlQuery529, mapping = {"myDataSource1":Accelerator_Landing_node1747967685701, "myDataSource2":Customer_Landing_node1747968715545}, transformation_ctx = "SQLQuery_node1747970405098")

# Script generated for node Drop Fields
DropFields_node1747970588718 = DropFields.apply(frame=SQLQuery_node1747970405098, paths=["customerName", "email", "birthDay", "phone", "serialNumber", "registrationDate", "lastUpdateDate", "shareWithResearchAsOfDate", "shareWithPublicAsOfDate", "shareWithFriendsAsOfDate"], transformation_ctx="DropFields_node1747970588718")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropFields_node1747970588718, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1747967341459", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1747970619134 = glueContext.getSink(path="s3://parent-datalake/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1747970619134")
AmazonS3_node1747970619134.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="accelerometer_trusted")
AmazonS3_node1747970619134.setFormat("json")
AmazonS3_node1747970619134.writeFrame(DropFields_node1747970588718)
job.commit()