import boto3


def get_ssm_value(ssm_param):
    """retrieve ssm param

    Parameters
    ----------
    ssm_param : string
            parameter name
    Returns
    -------
    String
            parameter value
    """
    ssm_client = boto3.client('ssm', region_name='eu-central-1')
    target_val = ssm_client.get_parameter(
        Name=ssm_param,
        WithDecryption=False
    )

    return target_val["Parameter"]["Value"]






