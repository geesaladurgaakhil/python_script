"""import boto3
rds=boto3.client('rds')

def get_source_values():
    dbs = rds.describe_db_instances()
    for db in dbs['DBInstances']:
        if db['DBInstanceIdentifier'] =='database-akhil':
            return db['Endpoint']['Address'], db['VpcSecurityGroups'][0]['VpcSecurityGroupId'],db['Engine'],db['DBName'],db['MasterUsername'],db['DBSubnetGroup']['DBSubnetGroupName']

endpoint=get_source_values()
end=endpoint[0]
vpcGid=endpoint[1]
engine=endpoint[2]
DBName=endpoint[3]
MasterUsername=endpoint[4]
subnet_grp=endpoint[5]
print(end)
print(vpcGid)
print(engine)
print(DBName)
print(MasterUsername)
print(subnet_grp)"""

import boto3
import sys
rds=boto3.client('rds')

def get_source_values(DBInstanceIdentifier):
    dbs = rds.describe_db_instances()
    for db in dbs['DBInstances']:
        if db['DBInstanceIdentifier'] == DBInstanceIdentifier:  #'database-akhil2-instance-1-us-east-2b':
            return db['Endpoint']['Address'], db['VpcSecurityGroups'][0]['VpcSecurityGroupId'],db['Engine'],db['DBName'],db['MasterUsername'],db['DBSubnetGroup']['DBSubnetGroupName']

target_rds_name=sys.argv[1]
endpoint=get_source_values(target_rds_name)
end=endpoint[0]
vpcGid=endpoint[1]
engine=endpoint[2]
DBName=endpoint[3]
MasterUsername=endpoint[4]
subnet_grp=endpoint[5]
print(end)
print(vpcGid)
print(engine)
print(DBName)
print(MasterUsername)
print(subnet_grp)




