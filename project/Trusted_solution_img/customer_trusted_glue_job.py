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

# Script generated for node Customer_Landing
Customer_Landing_node1747967685701 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="customer_landinglanding", transformation_ctx="Customer_Landing_node1747967685701")

# Script generated for node Privacy Filter
SqlQuery624 = '''
select * from myDataSource where shareWithResearchAsOfDate is not null;

'''
PrivacyFilter_node1747967737045 = sparkSqlQuery(glueContext, query = SqlQuery624, mapping = {"myDataSource":Customer_Landing_node1747967685701}, transformation_ctx = "PrivacyFilter_node1747967737045")

# Script generated for node Customer_Trusted
EvaluateDataQuality().process_rows(frame=PrivacyFilter_node1747967737045, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1747967341459", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Customer_Trusted_node1747967882545 = glueContext.getSink(path="s3://parent-datalake/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Customer_Trusted_node1747967882545")
Customer_Trusted_node1747967882545.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="customer_trusted")
Customer_Trusted_node1747967882545.setFormat("json")
Customer_Trusted_node1747967882545.writeFrame(PrivacyFilter_node1747967737045)
job.commit()