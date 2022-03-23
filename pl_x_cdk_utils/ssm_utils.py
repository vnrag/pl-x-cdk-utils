from aws_cdk import (
    aws_ssm as ssm,
)


def put_ssm_string_parameter(
    construct,
    parameter_name,
    string_value,
    description=None,
    allowed_pattern=".*",
    id=None,
):
    """

    :param construct: object
                      Stack Scope
    :param parameter_name: string
                           SSM parameter name
    :param string_value: string
                         String value we want for SSM parameter
    :param description: string
                        Description for SSM
    :param allowed_pattern: string
                            Allowed pattern for SSM
    :param id: string
                logical id of the cdk construct
    :return: object
             AWS SSM parameter object
    """
    description = (
        description if description else f"SSM parameter for" f" {parameter_name}"
    )
    param_id = id if id else f"profile-for-ssm-put-{parameter_name}"
    res = ssm.StringParameter(
        construct,
        param_id,
        allowed_pattern=allowed_pattern,
        description=description,
        parameter_name=parameter_name,
        string_value=string_value,
        tier=ssm.ParameterTier.STANDARD,
    )

    return res


def retrieve_ssm_string_parameter_value(
    construct, parameter_name, parameter=False, id=None
):
    """

    :param construct: object
                      Stack Scope
    :param parameter_name: string
                           SSM parameter name
    :param parameter: boolean
                           SSM parameter flag
    :param id: string
                logical id of the cdk construct
    :return: object
             AWS SSM parameter token object used as string during synth
    """
    param_id = id if id else f"profile-for-ssm-retrieve-{parameter_name}"
    val = (
        ssm.StringParameter.from_string_parameter_attributes(
            construct,
            param_id,
            parameter_name=parameter_name,
        )
        if parameter
        else ssm.StringParameter.from_string_parameter_attributes(
            construct,
            f"profile-for-ssm-retrieve-{parameter_name}",
            parameter_name=parameter_name,
        ).string_value
    )

    return val
