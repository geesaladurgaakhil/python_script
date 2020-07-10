import boto3
import sys
client = boto3.client('dms')

def end_point_creation(endpoint_identifier, endpoint_type, engine_name, user_name, password, server_name,
                       database_name):
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
    endpoint_arn = source_response['Endpoint']['EndpointArn']  # Getting source endpoint arn for dms task creation
    #print('endpoint_arn:', endpoint_arn)
    return endpoint_arn


# Source endpoint creation
source_endpoint_arn = end_point_creation(
    endpoint_identifier=sys.argv[1],
    endpoint_type=sys.argv[2],
    engine_name=sys.argv[3],
    user_name=sys.argv[4],
    password=sys.argv[5],
    server_name=sys.argv[6],
    database_name=sys.argv[7]
    )
print('source_endpoint_arn :',source_endpoint_arn)

target_endpoint_arn=end_point_creation(
    endpoint_identifier=sys.argv[8],
    endpoint_type=sys.argv[9],
    engine_name=sys.argv[10],
    user_name=sys.argv[11],
    password=sys.argv[12],
    server_name=sys.argv[13],
    database_name=sys.argv[14]
)
print('target_endpoint_arn :',target_endpoint_arn)