import boto3
import uuid


def get_ssm_value(ssm_param):
    """
    :param ssm_param: string
                      Name for the parameter we want to retrieve
    :return: string
             Value for the SSM
    """
    ssm_client = boto3.client('ssm', region_name='eu-central-1')
    target_val = ssm_client.get_parameter(
        Name=ssm_param,
        WithDecryption=False
    )

    return target_val["Parameter"]["Value"]


def trigger_glue_crawler(gc_name, aws_credentials=None,
                         region_name='eu-central-1'):
    """
    :param gc_name: string
                    Glue-Crawler name
    :param aws_credentials: dict
                            AWS credentials object in case of cross account
    :param region_name: string
                        AWS region
    :return: object
            Response from boto3 client
    """
    if aws_credentials:
        access_key = aws_credentials['Credentials']['AccessKeyId']
        secret_key = aws_credentials['Credentials']['SecretAccessKey']
        session_token = aws_credentials['Credentials']['SessionToken']
        client = boto3.client('glue', aws_access_key_id=access_key,
                              aws_secret_access_key=secret_key,
                              aws_session_token=session_token,
                              region_name=region_name)
    else:
        client = boto3.client('glue', region_name=region_name)

    resp = client.start_crawler(Name=gc_name)

    return resp


def get_cross_account_credentials(account_id, role_name,
                                  region_name='eu-central-1'):
    """
    :param account_id: string
                    AWS account Id
    :param role_name: string
                      Role name we want to use
    :param region_name: string
                        Region for the AWS account
    :return: object
             AWS credentials object
    """
    sts_connection = boto3.client('sts', region_name=region_name)
    role_arn = f"arn:aws:iam::{account_id}:role/{role_name}"
    aws_credentials = sts_connection.assume_role(
        RoleArn=role_arn,
        RoleSessionName=f"cross_acct_lambda_{role_name}"
    )
    return aws_credentials


def initiate_quicksight_ingestion(dataset_id,quicksight_account_id,
                                  aws_credentials=None,
                                  region_name='eu-central-1'):
    """
    :param dataset_id: string
                       Dataset Id for Quicksight dataset
    :param quicksight_account_id: string
                                  AWS account id for Quicksight
    :param aws_credentials: object
                            AWS credentials object
    :param region_name: string
                        AWS region
    :return: tuple
             Ingestion id and response from the ingestion
    """
    if aws_credentials:
        access_key = aws_credentials['Credentials']['AccessKeyId']
        secret_key = aws_credentials['Credentials']['SecretAccessKey']
        session_token = aws_credentials['Credentials']['SessionToken']
        client = boto3.client('quicksight', aws_access_key_id=access_key,
                              aws_secret_access_key=secret_key,
                              aws_session_token=session_token,
                              region_name=region_name)
    else:
        client = boto3.client('quicksight', region_name=region_name)

    ingestion_id = str(uuid.uuid4())
    response = client.create_ingestion(
        DataSetId=dataset_id,
        IngestionId=ingestion_id,
        AwsAccountId=quicksight_account_id
    )
    return response, ingestion_id





