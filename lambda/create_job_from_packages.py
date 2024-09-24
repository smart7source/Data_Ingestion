import boto3
import datetime

s3 = boto3.client("s3")
glue = boto3.client("glue")
job_datetime=datetime.datetime.now().strftime('%Y%m%d%H%M%S')

def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))
    # Get the object from the event and show its content type
    s3_bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    file_name = event["Records"][0]["s3"]["object"]["key"]
    print("What is the FILE Name....... ")
    print(file_name)
    s3_file_name=file_name
    balance_sheet="BalanceSheet"
    invoices="Invoices"

    if file_name != None and balance_sheet.lower() in file_name.lower():
        s3_file_name=balance_sheet
    elif file_name != None and invoices.lower() in file_name.lower():
        s3_file_name=invoices
    else:
        print("Not found!")
    source_data="s3://"+s3_bucket_name+"/"
    dq_config=f"s3://chello/ingestion/dq_config_file/dq-rules.json"
    print(source_data)
    job_name=s3_file_name+"_"+job_datetime
    script_location = f"s3://chello/ingestion/dataIngestion.py"
    default_args = {
        "--extra-py-files": f"s3://chello/ingestion/external_library/utils.zip,s3://chello/ingestion/external_library/PyMySQL-1.1.1-py3-none-any.whl,s3://chello/ingestion/external_library/data_load.zip",
        "--TempDir": f"s3://chello/temporary/"
    }
    glue_job = glue.create_job(
        Name=job_name,
        Role="lambda-full",
        Command={
            "Name": "glueetl",
            "ScriptLocation": script_location,
            "PythonVersion": "3",
        },
        DefaultArguments=default_args,
        Timeout=15,
        GlueVersion="4.0",
    )
    print("my_job:", glue_job)

    try:
        response = s3.get_object(Bucket=s3_bucket_name, Key=s3_file_name)
        print("*******************************************************")
        job_metadata = glue.start_job_run(JobName=glue_job["Name"], Arguments = {'--raw_s3_path' : source_data, '--dq_rule_config_file' : dq_config})
        status = glue.get_job_run(JobName=glue_job["Name"], RunId=job_metadata["JobRunId"])
        print(status["JobRun"]["JobRunState"])

        print("CONTENT TYPE: " + response["ContentType"])
        return response["ContentType"]
    except Exception as e:
        print(e)
        print(
            "Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.".format(
                s3_file_name, s3_bucket_name
            )
        )
        raise e
