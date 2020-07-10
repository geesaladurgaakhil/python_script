# creation of db instance
import boto3
rds = boto3.client('rds')
response = rds.create_db_instance(
    DBInstanceIdentifier='dbserver3',
    MasterUsername='dbadmin',
    MasterUserPassword='abcdefg123456789',
    DBInstanceClass='db.t2.micro',
    Engine='mariadb',
    AllocatedStorage=5)
print(response)

# Listing all db instances
"""import boto3
rds = boto3.client('rds')
# get all of the db instances
dbs = rds.describe_db_instances()
for db in dbs['DBInstances']:
      print(
      db['MasterUsername'],
      db['Endpoint']['Address'],
      db['Endpoint']['Port'],
      db['DBInstanceStatus'],
      db['DBInstanceClass'],
      db['Engine'])
"""
"""import boto3
rds=boto3.client('rds')
def rds_des():
   dbs=rds.describe_db_instances()
   for db in dbs['DBInstances']:
      return db['MasterUsername'],db['DBInstanceStatus']
akhil=rds_des()
print(akhil)"""