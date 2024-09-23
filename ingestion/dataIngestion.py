import sys
from awsglue.utils import getResolvedOptions
from data_load.json_loader import validate_and_process_data

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'raw_s3_path','dq_rule_config_file'])

job_id=args['JOB_NAME']
s3_location=args['raw_s3_path']
dq_rules_conf_file=args['dq_rule_config_file']
validate_and_process_data(job_id,s3_location,dq_rules_conf_file)
