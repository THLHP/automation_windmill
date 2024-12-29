import wmill
import requests
import json
import boto3
from typing import TypedDict
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError

class s3(TypedDict):
    bucket: str
    region: str
    endPoint: str
    accessKey: str
    secretKey: str

def main(
    input_file: bytes,
    bucket: s3,
    file_name = "placeholder",
    file_meta = {},
):
    if not bucket:
        print("No bucket selected, defaulting to cloudflare")
        bucket = wmill.get_resource("f/dicoms/cfr2_creds")


    # Initialize a session using your R2 credentials
    session = boto3.Session(
        aws_access_key_id=bucket['accessKey'],
        aws_secret_access_key=bucket['secretKey']
    )

    # Create an S3 client
    s3_client = session.client(
        's3',
        endpoint_url=bucket['endPoint']
    )

    # Upload the file with custom metadata
    s3_client.put_object(
        Bucket=bucket['bucket'],
        Key=file_name,
        Body=input_file
    )

    print(f"File {file_name} uploaded to bucket {bucket['bucket']}")