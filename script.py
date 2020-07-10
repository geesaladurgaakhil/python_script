# Instance creation
import boto3
ec2 = boto3.resource('ec2')
"""instance = ec2.create_instances(
    ImageId='ami-0a63f96e85105c6d3',
    MinCount=1,
    MaxCount=1,
    InstanceType='t2.micro')
print(instance[0].id)"""


# terminating running instance
instance_id = 'i-02d115b732e375ba3'
instance = ec2.Instance(instance_id)
response = instance.terminate()
print(response)

