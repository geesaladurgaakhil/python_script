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
    endpoint_arn = source_response['Endpoint']['EndpointArn'] # Getting source endpoint arn for dms task creation
    return endpoint_arn


# replication instance creation
def replication_instance_creation(replication_instance_id, replication_subnet_grp_id,replication_instance_class,vpc_security_groupIds,availability_zone):
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
    table_mappings = """{
    "rules": [{
    "rule-type": "selection",
    "rule-id": "1",
    "rule-name": "data_snapshots",
    "object-locator": {
    "schema-name": "public",
    "table-name": "data_snapshots"
    },
    "rule-action": "include",
    "load-order": "5"
    },
    {
    "rule-type": "selection",
    "rule-id": "2",
    "rule-name": "data_snapshot_errors",
    "object-locator": {
    "schema-name": "public",
    "table-name": "data_snapshot_errors"
    },
    "rule-action": "include",
    "load-order": "5"
    },
    {
    "rule-type": "selection",
    "rule-id": "3",
    "rule-name": "bivariate_score_histories",
    "object-locator": {
    "schema-name": "public",
    "table-name": "bivariate_score_histories"
    },
    "rule-action": "include",
    "load-order": "5"
    },
    {
    "rule-type": "selection",
    "rule-id": "4",
    "rule-name": "univariate_score_histories",
    "object-locator": {
    "schema-name": "public",
    "table-name": "univariate_score_histories"
    },
    "rule-action": "include",
    "load-order": "5"
    },
    {
    "rule-type": "selection",
    "rule-id": "5",
    "rule-name": "bivariate_issue_links",
    "object-locator": {
    "schema-name": "public",
    "table-name": "bivariate_issue_links"
    },
    "rule-action": "include",
    "load-order": "5"
    },
    {
    "rule-type": "selection",
    "rule-id": "6",
    "rule-name": "univariate_issue_links",
    "object-locator": {
    "schema-name": "public",
    "table-name": "univariate_issue_links"
    },
    "rule-action": "include",
    "load-order": "5"
    },
    {
    "rule-type": "selection",
    "rule-id": "7",
    "rule-name": "kri_issue_links",
    "object-locator": {
    "schema-name": "public",
    "table-name": "kri_issue_links"
    },
    "rule-action": "include",
    "load-order": "5"
    },
    {
    "rule-type": "selection",
    "rule-id": "8",
    "rule-name": "event_incidence_issue_links",
    "object-locator": {
    "schema-name": "public",
    "table-name": "event_incidence_issue_links"
    },
    "rule-action": "include",
    "load-order": "5"
    },
    {
    "rule-type": "selection",
    "rule-id": "9",
    "rule-name": "variables",
    "object-locator": {
    "schema-name": "public",
    "table-name": "variables"
    },
    "rule-action": "include",
    "load-order": "4"
    },
    {
    "rule-type": "selection",
    "rule-id": "10",
    "rule-name": "subjects",
    "object-locator": {
    "schema-name": "public",
    "table-name": "subjects"
    },
    "rule-action": "include",
    "load-order": "4"
    },
    {
    "rule-type": "selection",
    "rule-id": "11",
    "rule-name": "study_healths",
    "object-locator": {
    "schema-name": "public",
    "table-name": "study_healths"
    },
    "rule-action": "include",
    "load-order": "4"
    },
    {
    "rule-type": "selection",
    "rule-id": "12",
    "rule-name": "risk_category_signal_cards",
    "object-locator": {
    "schema-name": "public",
    "table-name": "risk_category_signal_cards"
    },
    "rule-action": "include",
    "load-order": "4"
    },
    {
    "rule-type": "selection",
    "rule-id": "13",
    "rule-name": "domain_signal_cards",
    "object-locator": {
    "schema-name": "public",
    "table-name": "domain_signal_cards"
    },
    "rule-action": "include",
    "load-order": "4"
    },
    {
    "rule-type": "selection",
    "rule-id": "14",
    "rule-name": "country_signal_cards",
    "object-locator": {
    "schema-name": "public",
    "table-name": "country_signal_cards"
    },
    "rule-action": "include",
    "load-order": "4"
    },
    {
    "rule-type": "selection",
    "rule-id": "15",
    "rule-name": "site_signal_cards",
    "object-locator": {
    "schema-name": "public",
    "table-name": "site_signal_cards"
    },
    "rule-action": "include",
    "load-order": "4"
    },
    {
    "rule-type": "selection",
    "rule-id": "16",
    "rule-name": "kri_values",
    "object-locator": {
    "schema-name": "public",
    "table-name": "kri_values"
    },
    "rule-action": "include",
    "load-order": "4"
    },
    {
    "rule-type": "selection",
    "rule-id": "17",
    "rule-name": "resource_summaries",
    "object-locator": {
    "schema-name": "public",
    "table-name": "resource_summaries"
    },
    "rule-action": "include",
    "load-order": "4"
    },
    {
    "rule-type": "selection",
    "rule-id": "18",
    "rule-name": "event_domains",
    "object-locator": {
    "schema-name": "public",
    "table-name": "event_domains"
    },
    "rule-action": "include",
    "load-order": "4"
    },
    {
    "rule-type": "selection",
    "rule-id": "19",
    "rule-name": "study_environment_incidences",
    "object-locator": {
    "schema-name": "public",
    "table-name": "study_environment_incidences"
    },
    "rule-action": "include",
    "load-order": "4"
    },
    {
    "rule-type": "selection",
    "rule-id": "20",
    "rule-name": "study_environment_events",
    "object-locator": {
    "schema-name": "public",
    "table-name": "study_environment_events"
    },
    "rule-action": "include",
    "load-order": "4"
    },
    {
    "rule-type": "selection",
    "rule-id": "21",
    "rule-name": "subject_reported_events",
    "object-locator": {
    "schema-name": "public",
    "table-name": "subject_reported_events"
    },
    "rule-action": "include",
    "load-order": "4"
    },
    {
    "rule-type": "selection",
    "rule-id": "22",
    "rule-name": "signal_card_contributors",
    "object-locator": {
    "schema-name": "public",
    "table-name": "signal_card_contributors"
    },
    "rule-action": "include",
    "load-order": "3"
    },
    {
    "rule-type": "selection",
    "rule-id": "23",
    "rule-name": "univariate_scores",
    "object-locator": {
    "schema-name": "public",
    "table-name": "univariate_scores"
    },
    "rule-action": "include",
    "load-order": "3"
    },
    {
    "rule-type": "selection",
    "rule-id": "24",
    "rule-name": "bivariate_scores",
    "object-locator": {
    "schema-name": "public",
    "table-name": "bivariate_scores"
    },
    "rule-action": "include",
    "load-order": "3"
    },
    {
    "rule-type": "selection",
    "rule-id": "25",
    "rule-name": "plots",
    "object-locator": {
    "schema-name": "public",
    "table-name": "plots"
    },
    "rule-action": "include",
    "load-order": "2"
    },
    {
    "rule-type": "selection",
    "rule-id": "26",
    "rule-name": "box_plots",
    "object-locator": {
    "schema-name": "public",
    "table-name": "box_plots"
    },
    "rule-action": "include",
    "load-order": "1"
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

    # Target endpoint creation
    target_endpoint_arn = end_point_creation(
    endpoint_identifier=sys.argv[8],
    endpoint_type=sys.argv[9],
    engine_name=sys.argv[10],
    user_name=sys.argv[11],
    password=sys.argv[12],
    server_name=sys.argv[13],
    database_name=sys.argv[14],
    )
    print('target_endpoint_arn :',target_endpoint_arn)

    # replication instance creation
    replication_arn = replication_instance_creation(
    replication_instance_id=sys.argv[15],
    replication_subnet_grp_id=sys.argv[16],
    replication_instance_class=sys.argv[17],
    vpc_security_groupIds=sys.argv[18],
    availability_zone=sys.argv[19]
    )

    # waiting for task to replication instance to create
    waiter(replication_arn)

    # Replication Task creation
    task_creation(stage=os.environ.get('STAGE_NAME', sys.argv[20]),
    source_endpoint_arn=source_endpoint_arn,
    target_endpoint_arn=target_endpoint_arn,
    replication_arn=replication_arn)

if __name__ == '__main__':
    sys.exit(main())