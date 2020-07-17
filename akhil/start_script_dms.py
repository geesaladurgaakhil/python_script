import sys
import boto3

# script for dms task
client = boto3.client('dms')
response = client.start_replication_task(
    ReplicationTaskArn='arn:aws:dms:us-east-2:887458950115:task:N4V6HFFIC3FHQZ26MEHZ47J6DC54BW6XEM2NHDQ',
    StartReplicationTaskType='start-replication', # used this line for first time migration
    #StartReplicationTaskType='reload-target', # used this line for second time migration
    )