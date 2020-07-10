source_endpoint_arn = end_point_creation(
    endpoint_identifier=endpoint_identifier,
    endpoint_type='source',
    engine_name='postgres',
    user_name='csaservicec13451',
    password='63c645da3e580b2e0671098cb672fd6d',
    server_name='csaservice-sandbox-f49eb4ea.cz4lsbnwubdn.us-east-1.rds.amazonaws.com',
    database_name='csaservice_sandbox'
    )

target_endpoint_arn = end_point_creation(
    endpoint_identifier='ravi7',
    endpoint_type='target',
    engine_name='aurora-postgresql',
    user_name='csaservice7f75f8',
    password='f263c16c3c4678f5dbaa1fce81131323',
    server_name='csaservice-sandbox-7d3bfb68.cz4lsbnwubdn.us-east-1.rds.amazonaws.com',
    database_name='csaservice_sandbox',

)