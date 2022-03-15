from aws_cdk import (
    Duration,
    RemovalPolicy,
    aws_glue,
    aws_iam as iam,
    aws_ssm as ssm,
    aws_logs as logs,
    aws_events as events,
    aws_lambda as _lambda,
    aws_events_targets as targets,
)
import aws_cdk as core


def implement_cdk_lambda(construct, lambda_path, roles=None, handler=None,
                         layers_list=None, memory_size=None, timeout=None,
                         function_name=None, runtime=None):
    """
    Implement Lambda Function
    :param construct: object
                      Stack Scope
    :param lambda_path: string
                        path for lambda asset
    :param roles: object
                  Role object assumed for lambda
    :param handler: string
                    File name followed by Function name
    :param layers_list: object
                        Layer object created for lambda layers
    :param memory_size: int
                        Memory assigned to lambda
    :param timeout: int
                    Time assigned for the lambda execution
    :param function_name: string
                          Name for the lambda function
    :param runtime: object
                    Lambda runtime object for python programming language
                    and version
    :return: object
             Lambda handler
    """
    handler = handler if handler else "lambda_handler.lambda_handler"
    memory_size = memory_size if memory_size else 128
    timeout = timeout if timeout else 5
    runtime = runtime if runtime else _lambda.Runtime.PYTHON_3_8
    l_handler = _lambda.Function(
        construct, f"profile-for-lambda-{function_name}",
        runtime=runtime,
        code=_lambda.Code.from_asset(lambda_path),
        handler=handler,
        layers=layers_list,
        memory_size=memory_size,
        timeout=Duration.seconds(timeout),
        role=roles,
        function_name=function_name
    )
    return l_handler


def create_glue_crawler(construct, name, db_name, role, table_prefix,
                        targets, configuration=None, schedule=None):
    """
    :param construct: object
                      Stack Scope
    :param name: string
                 Name for the glue-crawler
    :param db_name: string
                    Name for database
    :param role: object
                 AWS IAM role object
    :param table_prefix: string
                        Prefix we want to use in table
    :param targets: list
                    List of Key, value arguments for target,
                    eg: [{"path": "s3://bucket/path_to_file"}]
    :param configuration: string
                          Additional configurations for glue-crawler
    :param schedule: string
                     String value for cron
    :return: object
             Glue-Crawler object
    """
    glue_crawler = aws_glue.CfnCrawler(
        construct, f"profile-for-crawler-{name}",
        database_name=db_name,
        role=role,
        table_prefix=table_prefix,
        targets=targets,
        name=name
    )
    if configuration:
        glue_crawler.configuration = configuration
    if schedule:
        glue_crawler.schedule = aws_glue.CfnCrawler.ScheduleProperty(
            schedule_expression=schedule)

    return glue_crawler


def get_added_policies_as_role(construct, role_name, principal, actions_list,
                               resources_list=['*']):
    """
    :param construct: object
                      Stack Scope
    :param role_name: string
                      Role name
    :param principal: string
                      AWS principal that will be using the role
    :param actions_list: list
                         List of permission actions for the role
    :param resources_list: list
                           List for the resources we want to give the permission
    :return: object
             IAM role object
    """
    role = iam.Role(
        construct, f"profile-for-role-{role_name}", role_name=role_name,
        assumed_by=iam.ServicePrincipal(principal))
    role.add_to_policy(iam.PolicyStatement(
        resources=resources_list,
        actions=actions_list
    ))
    return role


def add_event_rule(construct, rule_name, cdk_functionality, minute="0",
                   hour="6", month="*", week_day="*", year="*",
                   event_input={}):
    """
    :param construct: object
                      Stack Scope
    :param rule_name: string
                      Name for the rule
    :param cdk_functionality: object
                              CDK handler where we want to implement rule
    :param minute: string
                   Minute for cron
    :param hour: string
                Hour for cron
    :param month: string
                   Month for cron
    :param week_day: string
                   Weekday for cron
    :param year: string
                   Year for cron
    :param event_input: dict
                        Input for the execution on given rule
    :return:
    """
    # Event rule for provided cron values
    event_rule = events.Rule(construct, f"profile-for-event-{rule_name}",
                             schedule=events.Schedule.cron(
                                 minute=minute, hour=hour, month=month,
                                 week_day=week_day, year=year)
                             )
    # Event input
    event_rule.add_target(
        targets.SfnStateMachine(
            cdk_functionality,
            input=events.RuleTargetInput.from_object(event_input),
        )
    )


def put_ssm_string_parameter(construct, parameter_name, string_value,
                             description, allowed_pattern=".*"):
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
    :return: object
             AWS SSM parameter object
    """
    res = ssm.StringParameter(construct,
                              f"profile-for-ssm-put-{parameter_name}",
                              allowed_pattern=allowed_pattern,
                              description=description,
                              parameter_name=parameter_name,
                              string_value=string_value,
                              tier=ssm.ParameterTier.STANDARD
                              )

    return res


def retrieve_ssm_string_parameter_value(construct, parameter_name):
    """

    :param construct: object
                      Stack Scope
    :param parameter_name: string
                           SSM parameter name
    :return: object
             AWS SSM parameter token object used as string during synth
    """
    val = ssm.StringParameter.from_string_parameter_attributes(
        construct, f"profile-for-ssm-retrieve-{parameter_name}",
        parameter_name=parameter_name).string_value

    return val


def create_log_group(construct, name, removal_policy=RemovalPolicy.DESTROY):
    """

    :param construct: object
                      Stack Scope
    :param name: string
                 Name for the log group
    :param removal_policy: object
                           Removal policy object
    :return: object
             AWS CDK log group object
    """
    log_group = logs.LogGroup(construct, f"profile-for-log-{name}",
                              log_group_name=name,
                              removal_policy=removal_policy)

    return log_group


def get_cdk_codebuild_step(git_source, commands, build_step='Synth',
                           submodules={}, role_policy_statements=[]):
    """

    :param git_source: object
                      Github connection
    :param commands: list
                     List of execution commands
    :param build_step: string
                       Name of the step
    :param submodules: dict
                       Github connection objects for the submodules
    :param role_policy_statements: object
                                   IAM role policies
    :return: object
             Codebuild step
    """
    codebuild_step = core.pipelines.CodeBuildStep(
        build_step, input=git_source,
        additional_inputs=submodules,
        commands=commands,
        role_policy_statements=role_policy_statements
    )
    return codebuild_step


def get_cdk_codepipeline(construct, repo_name, synth_step):
    """

    :param construct: object
                      Stack Scope
    :param repo_name: string
                      Name of the repo
    :param synth_step: object
                       Codebuild or ShellStep or any other..
    :return: object
             Pipeline object
    """
    pipeline = core.pipelines.CodePipeline(
        construct, f"profile-{repo_name}",
        pipeline_name=repo_name,
        cross_account_keys=True,
        self_mutation=True,
        synth=synth_step
    )
    return pipeline
