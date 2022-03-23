from aws_cdk import aws_logs as logs, RemovalPolicy


def create_log_group(construct, name, id=None, removal_policy=RemovalPolicy.DESTROY):
    """
    Create AWS log group
    :param construct: object
                      Stack Scope
    :param name: string
                 Name for the log group
    :param id: string
                logical id of the cdk construct
    :param removal_policy: object
                           Removal policy object
    :return: object
             AWS CDK log group object
    """
    param_id = id if id else f"profile-for-log-{name}"
    log_group = logs.LogGroup(
        construct, param_id, log_group_name=name, removal_policy=removal_policy
    )

    return log_group


def get_log_group_from_name(construct, name):
    """
    get log group with given name
    :param construct: object
                      Stack Scope
    :param name: string
                 Name for the log group
    :param id: string
                logical id of the cdk construct
    :return: object
             AWS CDK log group object
    """
    param_id = id if id else f"profile-for-log-{name}"
    log_group = logs.LogGroup.from_log_group_name(
        construct, param_id, log_group_name=name
    )

    return log_group
