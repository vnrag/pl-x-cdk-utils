import boto3


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






