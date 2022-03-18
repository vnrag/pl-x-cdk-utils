from aws_cdk import (
    Duration,
    aws_lambda as _lambda
)


def implement_lambda_function(
        construct, lambda_path, roles=None, handler=None, layers_list=None,
        memory_size=None, timeout=None, function_name=None, runtime=None,
        environment={}):
    """
    Implement cdk lambda
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
    :param environment: dict
                        Environment for lambda if needed
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
        function_name=function_name,
        environment=environment
    )
    return l_handler


def get_layer_from_arn(construct, layer_name, version):
    """

    :param construct: object
                      Stack Scope
    :param layer_name: string
                       Name of the layer
    :param version: string
                    Version for the layer
    :return: object
             Lambda layer object
    """
    lambda_layer = _lambda.LayerVersion.from_layer_version_arn(
        construct,
        f"profile-for-lambda-layer-{layer_name}",
        f"arn:aws:lambda:{construct.region}:{construct.account}:layer"
        f":{layer_name}:{version}")
    return lambda_layer


def get_bucket_object_from_arn(construct, function_name):
    """

    :param construct: object
                      Stack Scope
    :param function_name: string
                       Name of the layer
    :return: object
             Lambda function object
    """
    lambda_function = _lambda.Function.from_function_arn(
        construct,
        f"profile-for-lambda-function-{function_name}",
        f"arn:aws:lambda:{construct.region}:{construct.account}:function:{function_name}"
        )
    return lambda_function
