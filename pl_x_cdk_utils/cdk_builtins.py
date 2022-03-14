from aws_cdk import (
    Duration,
    aws_glue,
    aws_iam as iam,
    aws_lambda as _lambda
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
        construct, f"{function_name}-profile",
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


def create_glue_crawler(construct, name, profile, db_name, role, table_prefix,
                        targets, configuration=None, schedule=None):
    """
    :param construct: object
                      Stack Scope
    :param name: string
                 Name for the glue-crwaler
    :param profile: string
                    Profile name for glue-crawler
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
        construct, profile,
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
        construct, f"profile_for_{role_name}", role_name=role_name,
        assumed_by=iam.ServicePrincipal(principal))
    role.add_to_policy(iam.PolicyStatement(
        resources=resources_list,
        actions=actions_list
    ))
    return role


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
