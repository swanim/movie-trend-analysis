import logging
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable


def upload_to_s3(s3_conn_id, s3_bucket, s3_key, csv_string, replace):
    """
    Upload the CSV string to S3
    """
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)
    logging.info("Saving {} to {} in S3".format(csv_string, s3_key))
    s3_hook.load_string(
        string_data=csv_string,
        key=s3_key,
        bucket_name=s3_bucket,
        replace=replace
    )


def copy_to_s3(dataframes):
    s3_bucket = Variable.get("s3_bucket_name")
    s3_folder = 'daily'
    for name, dataframe in dataframes:
        s3_key = '{}/{}.csv'.format(s3_folder, name)
        upload_to_s3('aws_s3_conn_id', s3_bucket, s3_key, dataframe, replace=True)
