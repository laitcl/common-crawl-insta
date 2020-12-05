import boto3

filename = ""


s3 = boto3.client("s3")
s3_resource = boto3.resource('s3')
bucket_name = "commoncrawl"
s3.get_object(Bucket=bucket_name, Key="filename")