
INSERT_SQL="""insert into `job_tracking` (`job_id`,`job_description`,`total_records`,`processed_records`,
                    `error_records`,`job_start_time`,`job_end_time`,`created_ts`,`updated_ts`,
                    `file_location`,`status`)
         values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
    """
