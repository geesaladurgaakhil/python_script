import boto3
import os
import sys
client = boto3.client('dms')
rds=boto3.client('rds')

# getting environmental variables from environment
source_server_name=os.environ.get('SOURCE_DATABASE_HOST')
source_user_name=os.environ.get('SOURCE_DATABASE_USER')
source_password=os.environ.get('SOURCE_DATABASE_PASSWORD')
source_database=os.environ.get('SOURCE_DATABASE_NAME')
target_server_name=os.environ.get('DATABASE_HOST')
target_user_name=os.environ.get('DATABASE_USER')
target_password=os.environ.get('DATABASE_PASSWORD')
target_database=os.environ.get('DATABASE_NAME')
stage=os.environ.get('STAGE')

# getting the source rds values
def get_source_values(DBInstanceIdentifier):
    dbs = rds.describe_db_instances()
    for db in dbs['DBInstances']:
        if db['DBInstanceIdentifier'] == DBInstanceIdentifier:
            return db['VpcSecurityGroups'][0]['VpcSecurityGroupId'],db['Engine'] #db['DBSubnetGroup']['DBSubnetGroupName']

# getting the target rds values
def get_target_values(DBInstanceIdentifier):
    dbs = rds.describe_db_instances()
    for db in dbs['DBInstances']:
        if db['DBInstanceIdentifier'] == DBInstanceIdentifier:
            return db['Engine']


# Source and Target Endpoints Creation by using source, target rds
def end_point_creation(endpoint_identifier, endpoint_type, engine_name, user_name, password, server_name,database_name):
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
    endpoint_arn = source_response['Endpoint']['EndpointArn'] # Getting source endpoint arn for dms task creation
    # print('endpoint_arn:', endpoint_arn)
    return endpoint_arn


# replication instance creation
def replication_instance_creation(replication_instance_id, replication_subnet_grp_id, replication_instance_class,vpc_security_groupIds,availability_zone):
    replication_response = client.create_replication_instance(
    ReplicationInstanceIdentifier=replication_instance_id,
    AllocatedStorage=50,
    ReplicationInstanceClass=replication_instance_class,
    VpcSecurityGroupIds=[vpc_security_groupIds],
    AvailabilityZone=availability_zone,
    ReplicationSubnetGroupIdentifier=replication_subnet_grp_id,
    EngineVersion='3.3.3',
    PubliclyAccessible=False,
    )
    replication_arn = replication_response['ReplicationInstance']['ReplicationInstanceArn'] # Getting replication arn for dms task creation
    print('replication_arn: ', replication_arn)
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
    table_mappings = """{
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
    }
    """
    # Create Replication DMS Task
    task_response = client.create_replication_task(
    ReplicationTaskIdentifier=stage,
    SourceEndpointArn=source_endpoint_arn,
    TargetEndpointArn=target_endpoint_arn,
    ReplicationInstanceArn=replication_arn,
    MigrationType='full-load',
    TableMappings=table_mappings
    )
    print('Use this ARN while starting migration: ', task_response['ReplicationTask']['ReplicationTaskArn'])


def main():
    # assigning the source values to the variables
    source_rds_name=sys.argv[1]   # source_DBInstanceIdentifier
    endpoint=get_source_values(source_rds_name)
    vpcSecurityGroupId = endpoint[0]
    #subnet_grp=endpoint[1]
    source_engine_name=endpoint[1]

    # assigning the target values to the variables
    target_rds_name=sys.argv[2]     # target_DBInstanceIdentifier
    target_engine_name=get_target_values(target_rds_name)



    # Source endpoint creation
    source_endpoint_arn = end_point_creation(
    endpoint_identifier='csaservice-'+ sys.argv[3] +'-source-endpoint',
    endpoint_type='source',
    engine_name=source_engine_name,
    user_name=source_user_name,
    password=source_password,
    server_name=source_server_name,
    database_name=source_database
    )
    print('source_endpoint_arn :', source_endpoint_arn)

    # Target endpoint creation
    target_endpoint_arn = end_point_creation(
    endpoint_identifier='csaservice-'+ sys.argv[4] +'-target-endpoint',
    endpoint_type='target',
    engine_name=target_engine_name,
    user_name=target_user_name,
    password=target_password,
    server_name=target_server_name,
    database_name=target_database,
    )
    print('target_endpoint_arn :', target_endpoint_arn)

    # replication instance creation
    replication_arn = replication_instance_creation(
    replication_instance_id='csaservice-'+ stage + '-replication',
    replication_subnet_grp_id='akhil',        # subnet_grp
    replication_instance_class='dms.r4.4xlarge',
    vpc_security_groupIds=vpcSecurityGroupId,
    availability_zone='us-east-2a'
    )

    # waiting for task to replication instance to create
    waiter(replication_arn)

    # Replication Task creation
    task_creation(stage=stage,
    source_endpoint_arn=source_endpoint_arn,
    target_endpoint_arn=target_endpoint_arn,
    replication_arn=replication_arn)


if __name__=='__main__':
    sys.exit(main())