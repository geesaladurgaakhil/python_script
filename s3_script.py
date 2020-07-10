"""import boto3
s3=boto3.resource('s3')
bucket_name="jiraakhil"
try:
    responce=s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint':'us-east-2'})
    print(responce)
except Exception as error:
    print(error)"""


# list all the buckets and their contents
import boto3
s3 = boto3.resource('s3')
for bucket in s3.buckets.all():
    print(bucket.name)
    for item in bucket.objects.all():
        print(item.key)