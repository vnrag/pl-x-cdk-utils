from aws_cdk import Duration, aws_lambda as _lambda


def implement_lambda_function(
    construct,
    lambda_path,
    id=None,
    roles=None,
    handler=None,
    layers_list=None,
    memory_size=None,
    timeout=None,
    function_name=None,
    runtime=None,
    environment={},
):
    """
    Implement cdk lambda
    :param construct: object
                      Stack Scope
    :param lambda_path: string
                        path for lambda asset
    :param id: string
                logical id of the cdk construct
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
    param_id = id if id else f"profile-for-lambda-{function_name}"
    handler = handler if handler else "lambda_handler.lambda_handler"
    memory_size = memory_size if memory_size else 128
    timeout = timeout if timeout else 5
    runtime = runtime if runtime else _lambda.Runtime.PYTHON_3_8
    l_handler = _lambda.Function(
        construct,
        param_id,
        runtime=runtime,
        code=_lambda.Code.from_asset(lambda_path),
        handler=handler,
        layers=layers_list,
        memory_size=memory_size,
        timeout=Duration.seconds(timeout),
        role=roles,
        function_name=function_name,
        environment=environment,
    )
    return l_handler


def implement_lambda_layer(
    construct,
    layer_name,
    layer_path,
    runtimes=None,
    description=None
):
    """
    Implement cdk lambda-layer
    :param construct: object
                      Stack Scope
    :param layer_name string
                      Name of the package(s)
    :param layer_path: string
                        path for layer asset
    :param runtimes: object
                    Lambda runtime object for python programming language
                    and version
    :param description: string
                        Information about the layer
    :return: object
             Lambda handler
    """
    runtimes = runtimes if runtimes else [_lambda.Runtime.PYTHON_3_8,
                                          _lambda.Runtime.PYTHON_3_9]
    description = description if description else f"Layer for {layer_name}"
    layer = _lambda.LayerVersion(
            construct, layer_name,
            code=lambda_.Code.from_asset(layer_path),
            compatible_runtimes=runtimes,
            description=description
            )
    return layer


def get_layer_from_arn(construct, layer_name, version, id=None):
    """
    Get lambda layer by ARN
    :param construct: object
                      Stack Scope
    :param layer_name: string
                       Name of the layer
    :param version: string
                    Version for the layer
    :param id: string
                logical id of the cdk construct
    :return: object
             Lambda layer object
    """
    param_id = id if id else f"profile-for-lambda-layer-{layer_name}"
    lambda_layer = _lambda.LayerVersion.from_layer_version_arn(
        construct,
        param_id,
        f"arn:aws:lambda:{construct.region}:{construct.account}:layer"
        f":{layer_name}:{version}",
    )
    return lambda_layer


def get_lambda_from_arn(construct, function_name, id=None):
    """
    Get lambda function by ARN
    :param construct: object
                      Stack Scope
    :param function_name: string
                       Name of the function
    :param id: string
                logical id of the cdk construct
    :return: object
             Lambda function object
    """
    param_id = id if id else f"profile-for-lambda-function-{function_name}"
    lambda_function = _lambda.Function.from_function_arn(
        construct,
        param_id,
        f"arn:aws:lambda:{construct.region}:{construct.account}:function:{function_name}",
    )
    return lambda_function
