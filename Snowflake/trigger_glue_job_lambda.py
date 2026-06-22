/*  
AWS Lambda-Based Event-Driven Glue Job Trigger for Automated S3 Data Ingestion
Serverless AWS Lambda function that uses Boto3 to trigger the load-datat0-snowflake Glue job automatically when new files are uploaded to Amazon S3, enabling an end-to-end S3 → Glue → Snowflake ingestion pipeline.

*/

import boto3

glue = boto3.client('glue')

def lambda_handler(event, context):

    response = glue.start_job_run(
        JobName='load-datat0-snowflake'
    )

    return {
        'statusCode': 200,
        'JobRunId': response['JobRunId']
    }