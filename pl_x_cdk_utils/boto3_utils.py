import json
import uuid
import boto3


def get_ssm_value(ssm_param, region='eu-central-1', aws_credentials=None):
    """
    :param ssm_param: string
                      Name for the parameter we want to retrieve
    :param region: string
                   AWS region
    :param aws_credentials: object
                            AWS credentials
    :return: string
             Value for the SSM
    """
    if aws_credentials:
        access_key = aws_credentials['Credentials']['AccessKeyId']
        secret_key = aws_credentials['Credentials']['SecretAccessKey']
        session_token = aws_credentials['Credentials']['SessionToken']
        ssm_client = boto3.client(
            'ssm', aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            aws_session_token=session_token,
            region_name=region
            )
    else:
        ssm_client = boto3.client('ssm', region_name=region)
    target_val = ssm_client.get_parameter(
            Name=ssm_param,
            WithDecryption=False
            )

    return target_val["Parameter"]["Value"]


def trigger_glue_crawler(
        gc_name, aws_credentials=None,
        region_name='eu-central-1'
        ):
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
        client = boto3.client(
            'glue', aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            aws_session_token=session_token,
            region_name=region_name
            )
    else:
        client = boto3.client('glue', region_name=region_name)

    resp = client.start_crawler(Name=gc_name)

    return resp


def invoke_lambda(lambda_func, payload={}):
    """
    Invoke lambda function
    :param lambda_func: string
                         Name for the lambda function
    :param payload: dict
                    Payload i.e. event for lambda
    :return: object
            Response from boto3 client
    """
    boto_conn = boto3.client("lambda")
    resp = boto_conn.invoke(
            FunctionName=lambda_func,
            Payload=json.dumps(payload)
            )
    out = json.loads(resp["Payload"].read())
    return out


# Function to read file from s3
def get_object_from_s3(bucket_name, object_key, region='eu-central-1'):
    s3_client = boto3.client("s3", region_name=region)
    response = s3_client.get_object(
            Bucket=bucket_name,
            Key=object_key
            )
    response_json = response['Body'].read().decode('utf-8')
    return response_json


# Function to Upload file to s3
def upload_file_to_s3(file_path, bucket_name, prefix, region='eu-central-1'):
    s3_client = boto3.client("s3", region_name=region)
    try:
        response = s3_client.upload_file(file_path, bucket_name, prefix)
        print(
            f"----- File Uploaded to Bucket: {bucket_name}, "
            f"Path: {prefix} -----"
            )
    except Exception as e:
        print(
            f"----- Error on Uploading File to Bucket: {bucket_name}, "
            f"Error: {e} -----"
            )


def get_files_under_given_bucket_prefix(bucket, prefix):
    """
    Get all the files under given bucket and path
    :param bucket: string
                   Bucket name
    :param prefix: string
                   Path for the files
    :return: list
            List of files under given bucket and prefix
    """
    s3 = boto3.client("s3")
    all_objects = s3.list_objects(Bucket=bucket, Prefix=prefix, Delimiter="/")

    files_path_list = []
    for file in all_objects["Contents"] if "Contents" in all_objects else {}:
        files_path_list.append(file['Key']) if file['Size'] > 0 else ''

    return files_path_list


def get_cross_account_credentials(
        account_id, role_name,
        region_name='eu-central-1'
        ):
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


def initiate_quicksight_ingestion(
        dataset_id, quicksight_account_id,
        aws_credentials=None,
        region_name='eu-central-1'
        ):
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
        client = boto3.client(
            'quicksight', aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            aws_session_token=session_token,
            region_name=region_name
            )
    else:
        client = boto3.client('quicksight', region_name=region_name)

    ingestion_id = str(uuid.uuid4())
    response = client.create_ingestion(
            DataSetId=dataset_id,
            IngestionId=ingestion_id,
            AwsAccountId=quicksight_account_id
            )
    return response, ingestion_id


def change_s3_policy(
        sid, bucket_name, principal_arn, actions,
        resources=['*'], region='eu-central-1', aws_credentials=None
        ):
    """
    Add new bucket policy to existing bucket

    Parameters
    ----------
    sid : string
            statement id
    bucket_name : string
            name of the bucket resource
    principal_arn : string
            principal arn role name
    actions : list
            List of access for the given resources
    resources: list
            list of resources to give permissions on
    region: string
            Region for the deployment
    aws_credentials: object
                     AWS credentials object
    Returns
    -------
    dict
            put_bucket_policy response
    """
    if aws_credentials:
        access_key = aws_credentials['Credentials']['AccessKeyId']
        secret_key = aws_credentials['Credentials']['SecretAccessKey']
        session_token = aws_credentials['Credentials']['SessionToken']
        s3 = boto3.client(
            's3', aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            aws_session_token=session_token,
            region_name=region
            )
    else:
        s3 = boto3.client('s3', region_name=region)

    current_sids = []
    try:
        current_policy = json.loads(
                s3.get_bucket_policy(
                        Bucket=bucket_name
                        )['Policy']
                )
        # check duplicated sid
        for statement in current_policy["Statement"]:
            current_sids.append(statement["Sid"])
    except Exception as e:
        print(f"----- Error on loading current policies. Error: {e} -----")
        current_policy = dict()

    new_policy_statement = {
            "Sid": sid,
            "Effect": "Allow",
            "Principal": {
                    "AWS": principal_arn
                    },
            "Action": actions,
            "Resource": resources
            }

    working_statements = current_policy["Statement"] if \
        'Statement' in current_policy else []
    # add only the new statement if it doesn't exist before
    if sid not in current_sids:
        working_statements.append(new_policy_statement)
    else:
        return {
                "message": f"couldn't add the new statement. "
                           f"sid: {sid} already exists in bucket policy, \
                please change it and try again",
                "ResponseMetadata": {"HTTPStatusCode": 400}
                }
    bucket_policy = {
            'Version': '2012-10-17',
            "Statement": list(working_statements)
            }

    response = s3.put_bucket_policy(
            Bucket=bucket_name, Policy=json.dumps(bucket_policy)
            )
    # check on response code
    if response["ResponseMetadata"]["HTTPStatusCode"] == 204:
        return {
                "message": (f"{bucket_name} bucket policy is "
                            f"updated successfully"),
                "ResponseMetadata": {"HTTPStatusCode": 204}
                }
    else:
        return response
