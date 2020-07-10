import boto3
import sys
client = boto3.client('dms')

n=len(sys.argv)
print("\nArguments passed:\n", end = " ")
"""for i in range(1, n):
    print(sys.argv[i], "\n",end = " ",)"""

# endpoint_identifier, endpoint_type, engine_name, user_name, password, server_name, database_name
def end_point_creation():
    source_response = client.create_endpoint(
    EndpointIdentifier=sys.argv[1],
    EndpointType=sys.argv[2],
    EngineName=sys.argv[3],
    Username=sys.argv[4],
    Password=sys.argv[5],
    ServerName=sys.argv[6],
    Port=5432,
    DatabaseName=sys.argv[7],
    SslMode='none',
    )
    endpoint_arn = source_response['Endpoint']['EndpointArn']     # Getting source endpoint arn for dms task creation
    print('endpoint_arn:', endpoint_arn)
    return endpoint_arn

end_point_creation()