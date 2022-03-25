from aws_cdk import aws_apigateway as api_gateway

DEFAULT_THROTTLE = {"rate_limit": 10000, "burst_limit": 1000}
DEFAULT_QUOTA = {"limit": 100000, "period": api_gateway.Period.DAY}
DEFAULT_REQUEST_TEMPLATE = {"application/json": '{ "statusCode": "200" }'}
DEFAULT_CORS = {
    "allow_origins": api_gateway.Cors.ALL_ORIGINS,
    "allow_methods": api_gateway.Cors.ALL_METHODS,
}
DEFAULT_DEPLOY_OPTIONS = {
    "logging_level": api_gateway.MethodLoggingLevel.INFO,
    "data_trace_enabled": True,
}


def deploy_rest_api(
    construct, api_name, id=None, description=None, api_cors=None, deploy_options=None
):
    """
    Rest API with api_gateway
    :param construct: object
                      Stack Scope
    :param api_name: string
                     Name for API
    :param id: string
                logical id of the cdk construct
    :param description: string
                        Description on API
    :param api_cors: object
                     CORS for the API
    :param deploy_options: object
                           Deploy options with logging level for API
    :return: object
             API object
    """
    if id:
        api = get_api_from_id(construct, id)
    else:
        param_id = f"profile-for-api-{api_name}"
        description = description if description else f"API for {api_name}"
        api_cors = api_cors if api_cors else DEFAULT_CORS
        deploy_options = deploy_options if deploy_options else \
            DEFAULT_DEPLOY_OPTIONS
        api = api_gateway.RestApi(
            construct,
            param_id,
            rest_api_name=api_name,
            description=description,
            default_cors_preflight_options=api_cors,
            deploy_options=deploy_options
                )
    return api


def get_api_from_id(construct, api_id):
    """
    Get api from given id
    Parameters
    ----------
    construct : object
                Stack Scope
    api_id : string
             Rest API id
    Returns
    -------
    API object
    """
    param_id = f"get-api-from-id-{api_id}"
    api = api_gateway.RestApi.from_rest_api_id(construct, param_id, api_id)
    return api


def integrate_lambda_to_api(lambda_handler, req_templates=None):
    """
    Integrate lambda to rest API
    :param lambda_handler: object
                           Lambda handler to be integrated
    :param req_templates: object
                          Request template object
    :return: object
             Lambda integrated API object
    """
    req_templates = req_templates if req_templates else DEFAULT_REQUEST_TEMPLATE
    lambda_integration = api_gateway.LambdaIntegration(
        lambda_handler, request_templates=req_templates
    )
    return lambda_integration


def add_resource_to_api(api_object, name, root=True, api_cors=None):
    """
    Add resource to existing API
    :param api_object: object
                       API object
    :param name: string
                 Name for the resource
    :param root: bool
                 Bool value to decide to either add root or not
    :param api_cors: object
                     Cors for resource
    :return: object
            Updated API object after resource
    """
    api_cors = api_cors if api_cors else DEFAULT_CORS
    if root:
        updated_api = api_object.root.add_resource(
            name, default_cors_preflight_options=api_cors
        )
    else:
        updated_api = api_object.add_resource(
            name, default_cors_preflight_options=api_cors
        )
    return updated_api


def add_usage_plan_to_api(api_object, name, id=None, quota=None,
                          throttle=None):
    """
    Add usage plan to API
    :param api_object: object
                       API object
    :param name: string
                 Name for the usage
    :param id: string
                logical id of the cdk construct
    :param quota: object
                  Quota object for the usage
    :param throttle: object
                     Throttle object for the usage
    :return: object
             Updated API object
    """
    param_id = id if id else f"api-usage-{name}"
    quota = quota if quota else DEFAULT_QUOTA
    throttle = throttle if throttle else DEFAULT_THROTTLE
    updated_api = api_object.add_usage_plan(
        param_id, name=name, quota=quota, throttle=throttle
    )
    return updated_api
