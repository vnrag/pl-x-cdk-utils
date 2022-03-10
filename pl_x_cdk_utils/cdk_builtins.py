from aws_cdk import (
    Duration,
    aws_kms as kms,
    aws_ssm as ssm,
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
