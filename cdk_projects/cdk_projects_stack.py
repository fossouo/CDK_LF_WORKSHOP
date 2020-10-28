import boto3
import json
from aws_cdk import core


class CdkProjectsStack(core.Stack):

    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        client = boto3.client('glue')

        # Give Developer Account Lake Formation privileges 

        self.registerbucket_grantpriv("arn:aws:s3:::lf-euler-406530995514","arn:aws:iam::406530995514:role/LF-GlueServiceRole","arn:aws:iam::406530995514:user/lf-developer","glue-demo","nyctaxi")

        # The code that defines your stack goes here
        #print(self.get_policies("arn:aws:iam::054655141806:policy/service-role/AWSGlueServiceRole-S3NYCTAXI"))
        
        self.create_glue_crawler("GlueTPC","LF-GlueServiceRole","glue-demo","here is the glue crawler that will get create S3 NYC TAXI DATA","s3://lf-euler-406530995514/glue/nyctaxi")
        
        # Execute Glue Crawler GlueTPC 
        try: 
            response = client.start_crawler(
                Name='GlueTPC'
            )
            print(response)
        except client.exceptions.CrawlerRunningException:
            print('already running')

        # The table nyctaxi is then discover 
        try: 
            response = client.get_table(
                DatabaseName='glue-demo',
                Name='nyctaxi'
            )
            print(response['Table']['Name'])
        except client.exceptions.EntityNotFoundException:
            print('Table not found')

 

#1. Create Glue Crawler 

    @staticmethod
    def create_glue_crawler(name,role,dbname,description,S3Path):
        client = boto3.client('glue')
        # Create Glue-Demo DB 
        
        try: 
            response = client.create_database(
                DatabaseInput={
                    'Name': dbname
                }
            )
            print(response)
        except client.exceptions.AlreadyExistsException:
            print('Database already exists')

        # Create the crawler 
        try:

            client.create_crawler(
                Name=name,
                Role=role,
                DatabaseName=dbname,
                Description=description,
                Targets={
                    'S3Targets': [
                        {
                            'Path': S3Path
                        }
                    ]
                }
            )
        
        except client.exceptions.AlreadyExistsException:
            print('Crawler already exists')


#1. Execute Glue Crawler 



#2. Get Policies in place for Provided IAM User 

    @staticmethod
    def get_policies(iam_user_arn):
        client = boto3.client('iam')
        policy = client.get_policy(
            PolicyArn = iam_user_arn
        )
        policy_version = client.get_policy_version(
            PolicyArn = iam_user_arn, 
            VersionId = policy['Policy']['DefaultVersionId']
        )

        #return json.dumps(policy_version['PolicyVersion']['Document']))
        return json.dumps(policy_version['PolicyVersion']['Document'],indent=2)


#2. Convert Policies related to Glue into LF Permissions 

    @staticmethod
    def registerbucket_grantpriv(S3RoleArn,IAMRoleArn,UserPrincipal,dbname,tblname):

        client = boto3.client('lakeformation')
        try: 
            response = client.put_data_lake_settings(
                DataLakeSettings={
                    'DataLakeAdmins': [
                        {
                            'DataLakePrincipalIdentifier': 'arn:aws:iam::406530995514:role/Admin-OneClick'
                        },
                    ],
                 
                }
            )
            print(response)
            print("Put LF Admin Account")
        except client.exceptions.InternalServiceException:
            print("the Admin Account is already registered")

#        try: 
#            client.put_data_lake_settings(
#                DataLakeSettings={
#                    'DataLakeAdmins': [
#                        {
#                            'DataLakePrincipalIdentifier': 'arn:aws:iam::406530995514:user/lf-admin'
#                        },
#                    ],                  
#                }
#            )
#            print("Put Admin Account")
#        except client.exceptions.InternalServiceException:
#            print("the Admin Account is already registered")

        try: 
            client.register_resource(
                ResourceArn=S3RoleArn,
                UseServiceLinkedRole=True,
                RoleArn=IAMRoleArn
            )
            print("Bucket Successfuly registered")
        except client.exceptions.AlreadyExistsException:
            print("Location already registered")

        try:
            client.grant_permissions(
                Principal={
                    'DataLakePrincipalIdentifier': UserPrincipal
                },
                Resource={
                    'Database': {
                        'Name': dbname
                    },
                    'Table': {
                        'DatabaseName': dbname,
                        'Name': tblname
                    }
                },
                Permissions=[
                    'ALL',
                ],
                PermissionsWithGrantOption=[
                    'ALL',
                ]
            )
            print("Granted privileges to user principal")
        except client.exceptions.ConcurrentModificationException:
            print("The User already have privileges")


#3. Apply LF Permissions 

#4. Validate LF Permissions of the IAM User