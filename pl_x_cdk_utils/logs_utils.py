from aws_cdk import (
    RemovalPolicy,
    aws_logs as logs,
    aws_logs_destinations as destinations
)


def create_log_group(
    construct,
    name,
    id=None,
    removal_policy=RemovalPolicy.DESTROY,
    retention=logs.RetentionDays.TWO_MONTHS,
):
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
    :param retention: object
                      RetentionDays object
    :return: object
             AWS CDK log group object
    """
    param_id = id if id else f"profile-for-log-{name}"
    log_group = logs.LogGroup(
        construct,
        param_id,
        log_group_name=name,
        removal_policy=removal_policy,
        retention=retention,
    )

    return log_group


def get_log_group_from_name(
    construct,
    name,
    id=None,
):
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


def add_lambda_log_subscription(
    construct,
    log_group,
    lambda_handler,
    profile_name,
    filter_pattern=None,
):
    """
    Implement lambda subscription to log
    Parameters
    ----------
    construct : object
                Stack Scope
    log_group : object
                Log group object
    lambda_handler : object
                     Lambda object for the subscription
    profile_name : string
                   Profile name for the subscription
    filter_pattern : object
                     Filter pattern object for the subscription
    Returns
    -------

    """
    filter_pattern = filter_pattern if filter_pattern else \
        logs.FilterPattern.any(
            logs.FilterPattern.string_value(
                json_field="$.type",
                comparison="=",
                value="*Succeeded*",
            ),
            logs.FilterPattern.string_value(
                json_field="$.type",
                comparison="=",
                value="*Failed*",
            )
        )
    logs.SubscriptionFilter(
        construct,
        profile_name,
        log_group=log_group,
        destination=destinations.LambdaDestination(
            lambda_handler,
        ),
        filter_pattern=filter_pattern,
    )


def add_success_and_error_subscription_to_log_group(
    construct,
    log_group,
    name,
    lambda_function,
    error_filter=None,
    success_filter=None,
):
    """
    Add subscription to log_group
    Parameters
    ----------
    construct : object
                Stack Scope
    log_group : object
                Log group object
    lambda_handler : object
                     Lambda object for the subscription
    error_filter : string
                   Error filter literal
    success_pattern : object
                      Success filter literal
    Returns
    -------
    """
    if error_filter:
        log_group.add_subscription_filter(
            f"{name}LogGroupErrorSubscription",
            filter_pattern=logs.FilterPattern.literal(
                error_filter,
            ),
            destination=destinations.LambdaDestination(
                lambda_function
            ),
            filter_name=f"log_subscription_error_{name}",
        )

    if success_filter:
        log_group.add_subscription_filter(
            f"{name}LogGroupErrorSubscription",
            filter_pattern=logs.FilterPattern.literal(
                success_filter,
            ),
            destination=destinations.LambdaDestination(
                lambda_function
            ),
            filter_name=f"log_subscription_error_{name}",
        )
