from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as sfn_tasks


def get_state_machine_from_arn(construct, state_machine_name):
    """
    Get state machine by ARN
    :param construct: object
                      Stack Scope
    :param state_machine_name: string
                       Name of the state machine
    :return: object
                State machine object
    """
    state_machine = sfn.StateMachine.from_state_machine_arn(
        construct,
        f"profile-for-state-machine-{state_machine_name}",
        f"arn:aws:states:{construct.region}:"
        f"{construct.account}:stateMachine:{state_machine_name}",
    )

    return state_machine


def step_invoke_lambda_function(
    construct,
    step_name,
    lambda_func,
    payload={},
    input_path="$",
    output_path="$",
    result_selector={"$": "$"},
    result_path="$.resp",
    path=False,
):
    """
    Get state machine by ARN
    :param construct: object
                      Stack Scope
    :param step_name: string
                       Name of the state machine task step
    :param lambda_func: object
                       Lambda function object
    :param payload: dict
                       Input of lambda event
    :param input_path: string
                       JSON string as input to the step
    :param output_path: string
                       JSON string as output from the step
    :param result_selector: dict
                       JSON string as result and assign a name to it for the whole state putput
    :param result_path: string
                       JSON string as result from the step override the whole output
    :param path: boolean
                       flag to load payload from given path or payload param
    :return: object
                State machine lambda task object
    """
    lambda_state = sfn_tasks.LambdaInvoke(
        construct,
        name=step_name,
        lambda_function=lambda_func,
        payload=sfn.TaskInput.from_json_path_at("$")
        if path
        else sfn.TaskInput.from_object(payload),
        input_path=input_path,
        output_path=output_path,
        result_path=result_path,
        result_selector=result_selector,
    )

    return lambda_state
