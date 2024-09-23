import string
from pathlib import Path
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
import datetime
import logging
import getpass
import json
import logging
import traceback
from pathlib import Path

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, trim, udf, collect_list, coalesce, lit, when, split
from pyspark.sql.types import StructType, StringType, StructField, DateType, IntegerType

from utils.db_utils import insert_record
from utils.data_quality import DataQuality
from utils.helpers import read_json_get_dict
from utils.comprehensive_logging import init_logging

from utils.job_tracking_utils import INSERT_SQL
from utils.nested_json_utils import flatten_nested_data
import json
import boto3

s3 = boto3.resource('s3')
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
logger = glueContext.get_logger()

mysql_options = {
    "url": "jdbc:mysql://chello-insights.cb8gwiek2g7m.us-east-2.rds.amazonaws.com:3306/raw",
    "dbtable": "claims",
    "balance_sheet": "balance_sheet_debug",
    "job_tracking_table": "job_tracking",
    "user": "new_admin_user",
    "password": "Mychello08012024*"
}


def validatedata(job_id:string, raw_data_df_flatten: DataFrame, dq_rules_dict):
    columns=dq_rules_dict['columns']
    dq_location=dq_rules_dict['execution_reports_dir']
    raw_data_df_flatten.printSchema()

    validate_raw_df = raw_data_df_flatten.withColumn('null_check_result', when(raw_data_df_flatten[columns].isNull(), 'TRUE').otherwise('FALSE'))
    null_values=validate_raw_df.filter(validate_raw_df['null_check_result'].cast("string") == 'TRUE')
    null_values.repartition(1).write.mode('overwrite').csv(dq_location,header = 'true')
    processed_good_rows = validate_raw_df.filter(validate_raw_df['null_check_result'].cast("string") == 'FALSE')
    return processed_good_rows.drop("null_check_result")

def load_data_2_rds(raw_data_df:DataFrame):
    raw_data_df.write \
        .format("jdbc") \
        .option("url", mysql_options["url"]) \
        .option("dbtable", mysql_options["balance_sheet"]) \
        .option("user", mysql_options["user"]) \
        .option("password", mysql_options["password"]) \
        .mode("append") \
        .save()

def read_dq_json_file(dq_rules_conf_file:string, job_id:string):
    dq_file = glueContext.create_dynamic_frame.from_options(
        format_options={
            "quoteChar": '"',
            "withHeader": True,
            "separator": ",",
            "multiline": False,
        },
        connection_type="s3",
        format="json",
        connection_options={
            "paths": [dq_rules_conf_file],
        },
        transformation_ctx="dq_file",
    )
    dq_file_df = dq_file.toDF().repartition(1)
    dq_file_df_flatten=flatten_nested_data(dq_file_df)
    dq_file_df=(dq_file_df_flatten.withColumn('execution_reports_dir',lit(f"s3://chello/ingestion/dq_reports/"))
                .withColumn('job_id',lit(job_id)))
    dq_dict=dq_file_df.rdd.map(lambda row: row.asDict()).collect()
    return dq_dict

def validate_and_process_data(job_id:string, s3_location:string, dq_rules_conf_file:string):
    print("*****************************************************************")
    print(" Read Data... And Performing repartition.... ")
    print("*****************************************************************")
    print(" This is the Job Id I am connecting to....", job_id)
    raw_data = glueContext.create_dynamic_frame.from_options(
        format_options={
            "quoteChar": '"',
            "withHeader": True,
            "separator": ",",
            "multiline": False,
        },
        connection_type="s3",
        format="json",
        connection_options={
            "paths": [s3_location],
        },
        transformation_ctx="raw_data",
    )
    val=datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    raw_data_df = raw_data.toDF().repartition(1)
    raw_data_df_flatten=flatten_nested_data(raw_data_df)
    total_record_count=raw_data_df_flatten.count()
    total_record_count_str=str(total_record_count)
    print()
    # Validate and move forward.
    insert_record(INSERT_SQL,(job_id,'PAYMENT',total_record_count_str, total_record_count_str,
                              '0',val,val,val,val,s3_location,'STARTED'))
    print(" Above the Job Tracking Data Insert STMT.....")
    print(dq_rules_conf_file)
    print("Going to the JSON Loader....... ")
    dq_rules=read_dq_json_file(dq_rules_conf_file, job_id)
    dq_rules_dict=dq_rules[0]

    not_null_records=validatedata(job_id, raw_data_df_flatten, dq_rules_dict)

    print(not_null_records.count())
    load_data_2_rds(not_null_records)
