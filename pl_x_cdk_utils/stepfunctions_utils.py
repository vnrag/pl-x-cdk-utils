from aws_cdk import (
    aws_stepfunctions as sfn
)


def get_state_machine_from_arn(construct, state_machine_name):
    """

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
        f"{construct.account}:stateMachine:{state_machine_name}")

    return state_machine
