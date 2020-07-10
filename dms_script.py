import boto3
import os
import sys
client = boto3.client('dms')


# Source and Target Endpoints Creation by using source, target rds
def end_point_creation(endpoint_identifier, endpoint_type, engine_name, user_name, password, server_name, database_name):
    source_response = client.create_endpoint(
    EndpointIdentifier=endpoint_identifier,
    EndpointType=endpoint_type,
    EngineName=engine_name,
    Username=user_name,
    Password=password,
    ServerName=server_name,
    Port=5432,
    DatabaseName=database_name,
    SslMode='none',
    )
    endpoint_arn = source_response['Endpoint']['EndpointArn'] # Geting source endpoint arn for dms task creation
    print('endpoint_arn:', endpoint_arn)
    return endpoint_arn


# replication instance creation
def replication_instance_creation(replication_instance_id, replication_subnet_grp_id):
    replication_response = client.create_replication_instance(
    ReplicationInstanceIdentifier=replication_instance_id,
    AllocatedStorage=50,
    ReplicationInstanceClass='dms.r4.4xlarge',
    VpcSecurityGroupIds=['sg-6d6e7116'],
    AvailabilityZone='us-east-2a',
    ReplicationSubnetGroupIdentifier=replication_subnet_grp_id,
    EngineVersion='3.3.3',
    PubliclyAccessible=False,
    )
    replication_arn = replication_response['ReplicationInstance']['ReplicationInstanceArn'] # Getting replication arn for dms task creation
    print('replication_arn: ',replication_arn)
    return replication_arn


# waiter code for executing replica status to be active
def waiter(replication_arn):
    waiter = client.get_waiter('replication_instance_available')
    waiter.wait(Filters=[
    {
    'Name': 'replication-instance-arn',
    'Values': [
        replication_arn,
    ]
    },
    ],
    WaiterConfig={
    'Delay': 300,
    'MaxAttempts': 300,
    }
    )

# dms task creation
def task_creation(stage, source_endpoint_arn, target_endpoint_arn, replication_arn):
    table_mappings="""{
      "rules": [
        {
          "rule-type": "selection",
          "rule-id": "1",
          "rule-name": "1",
          "object-locator": {
            "schema-name": "%",
            "table-name": "%"
          },
          "rule-action": "include",
          "filters": []
        }
      ]
    }"""
    # Create Replication DMS Task
    task_response = client.create_replication_task(
    ReplicationTaskIdentifier=stage,
    SourceEndpointArn=source_endpoint_arn,
    TargetEndpointArn=target_endpoint_arn,
    ReplicationInstanceArn=replication_arn,
    MigrationType='full-load',
    TableMappings=table_mappings
    )
    print('Replication Task response:', task_response)
    print('Use this ARN while starting migration: ', task_response['ReplicationTask']['ReplicationTaskArn'])


def main():
    endpoint_identifier=input("Enter the source endpoint identifier :")
    endpoint_type=input("Enter the engine type :")
    engine_name=input("Enter the engine name :")
    user_name=input("Enter the username :")
    password=input("Enter the password:")
    server_name=input("Enter the Server_name :")
    database_name=input("Enter the database name :")
    # Source endpoint creation
    source_endpoint_arn = end_point_creation(
    endpoint_identifier=endpoint_identifier,
    endpoint_type=endpoint_type,
    engine_name=engine_name,
    user_name=user_name,
    password=password,
    server_name=server_name,
    database_name=database_name
    )

    # Target endpoint creation
    endpoint_identifier = input("Enter the Target endpoint identifier :")
    endpoint_type = input("Enter the engine type :")
    engine_name = input("Enter the engine name :")
    user_name = input("Enter the username :")
    password = input("Enter the password:")
    server_name = input("Enter the Server_name :")
    database_name = input("Enter the database name :")
    target_endpoint_arn = end_point_creation(
    endpoint_identifier=endpoint_identifier,
    endpoint_type=endpoint_type,
    engine_name=engine_name,
    user_name=user_name,
    password=password,
    server_name=server_name,
    database_name=database_name,
    )

    # replication instance creation
    repId=input("Enter the replication Instance id :")
    rep_subnet_grp_id=input("enter the replication_subnet_grp_id :")
    replication_arn = replication_instance_creation(
    replication_instance_id=repId,
    replication_subnet_grp_id=rep_subnet_grp_id)

    # waiting for task to replication instance to create
    waiter(replication_arn)

    # Replication Task creation
    task_creation(stage=os.environ.get('STAGE_NAME', 'sandbox2'),
    source_endpoint_arn=source_endpoint_arn,
    target_endpoint_arn=target_endpoint_arn,
    replication_arn=replication_arn)

if __name__ == '__main__':
    sys.exit(main())
