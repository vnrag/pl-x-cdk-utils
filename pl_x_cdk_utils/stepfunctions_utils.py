from aws_cdk import (
    Stack,
    Duration,
    aws_iam as iam,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
)
from aws_cdk.aws_stepfunctions_tasks import (
    EmrAddStep as eas,
    EmrCreateCluster as ecc,
    EmrTerminateCluster as etc,
)

from pl_x_cdk_utils.helpers import prepare_s3_path
from pl_x_cdk_utils.logs_utils import create_log_group


def deploy_state_machine(construct, name, definition, id=None, role=None,
                         log_group=None, log_level=sfn.LogLevel.ALL):
    """
     Deploy state machine
     :param construct: object
                       Stack Scope
     :param name: string
                  Name for the state machine
     :param definition: object
                         Task definition object
     :param id: string
                Stack id
     :param role: object
                  IAM Role object
     :param log_group: object
                       Log object
     :param log_level: object
                       Log level object
    :return: object
             State machine object
    """
    log_group = log_group if log_group else \
        create_log_group(construct, name=f"{name}")
    param_id = id if id else f"profile-for-state-machine-{name}"
    if role:
        state_machine = sfn.StateMachine(
            construct,
            param_id,
            state_machine_name=name,
            definition=definition,
            role=role,
            logs=sfn.LogOptions(destination=log_group, level=log_level)
            )
    else:
        state_machine = sfn.StateMachine(
                construct,
                param_id,
                state_machine_name=name,
                definition=definition,
                logs=sfn.LogOptions(destination=log_group, level=log_level)
                )
    return state_machine


def get_state_machine_from_arn(construct, state_machine_name, id=None):
    """
    Get state machine by ARN
    :param construct: object
                      Stack Scope
    :param state_machine_name: string
                       Name of the state machine
    :param id: string
                logical id of the cdk construct
    :return: object
                State machine object
    """
    param_id = id if id else f"profile-for-state-machine-{state_machine_name}"
    state_machine = sfn.StateMachine.from_state_machine_arn(
        construct,
        param_id,
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
    json_path="$",
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
                       JSON string as result and assign a name to it for the
                       whole state output
    :param result_path: string
                       JSON string as result from the step override the
                       whole output
    :param path: boolean
                       flag to load payload from given path or payload param
    :param json_path: boolean
                       Json path for the payload param
    :return: object
                State machine lambda task object
    """
    lambda_state = sfn_tasks.LambdaInvoke(
        construct,
        step_name,
        lambda_function=lambda_func,
        payload=sfn.TaskInput.from_json_path_at(json_path)
        if path
        else sfn.TaskInput.from_object(payload),
        input_path=input_path,
        output_path=output_path,
        result_path=result_path,
        result_selector=result_selector,
    )

    return lambda_state


def get_trigger_step_function_state(
        construct, state_name, state_machine, input_path='$',
        result_path='$.sfn_invoke', output_path="$",
        integration_pattern=sfn.IntegrationPattern.RUN_JOB):
    """
    Trigger state machine
    Parameters
    ----------
    construct : object
                Stack Scope
    state_name : string
                 Name for the state
    state_machine : object
                    State machine object
    input_path : string
                 Input path for the step-function to be triggered
    result_path : string
                 Result path for the result after the trigger
    output_path : string
                 Output path for the result after the trigger
    integration_pattern : object
                          Integration pattern object
    Returns
    -------
    State object
    """
    state = sfn_tasks.StepFunctionsStartExecution(
        construct, state_name,
        state_machine=state_machine,
        integration_pattern=integration_pattern,
        input_path=input_path,
        result_path=result_path,
        output_path=output_path
        )
    return state


def get_aws_service_call_state(construct, state_name, action,
                               service="appflow",
                               iam_action="appflow:StartFlow",
                               iam_resources=["*"],
                               parameters={"FlowName.$": "$"},
                               result_path="$"):
    """
    Get AWS service call state
    Parameters
    ----------
    construct : object
                Stack Scope
    state_name : string
                 Name for the state
    action : string
             Action on the call
    service : string
              Name for the service
    iam_action : string
                 IAM action for the service
    iam_resources : list
                    List of resources for the action
    parameters : object
                 Parameters object for the AWS service
    result_path: string
                 Result path for the state
    Returns
    -------
    State object
    """
    state = sfn_tasks.CallAwsService(
                construct, state_name,
                action=action,
                service=service,
                iam_action=iam_action,
                iam_resources=iam_resources,
                parameters=parameters,
                result_path=result_path
            )
    return state


def get_map_state(construct, state_name, items_path="$.args",
                  input_path="$", result_path="$.map_resp",
                  max_concurrency=6, parameters={},
                  result_selector={"map.$": "$"}):
    """
    Get map state
    Parameters
    ----------
    construct : object
                Stack Scope
    state_name : string
                 Name for the state
    items_path : string
                Items path from the array for the step-function to be triggered
    input_path : string
                 Input path for the step-function to be triggered
    result_path : string
                 Result path for the result after the trigger
    max_concurrency : int
                      Max concurrent calls over the iterations
    parameters : dict
                Parameters for the map state
    result_selector : dict
                Result selector for the output
    Returns
    -------
    State object
    """
    state = sfn.Map(construct, state_name, items_path=items_path,
                    input_path=input_path, result_path=result_path,
                    max_concurrency=max_concurrency, parameters=parameters,
                    result_selector=result_selector
                    )
    return state


def get_custom_state(construct, state_name, state_json):
    """
     Get the custom state with the provided json
    :param construct: object
                      Stack Scope
    :param state_name: string
                       Name of the state in the state machine
    :param state_json: object
                       Json object for the state
    :return: object
                State object
    """
    state = sfn.CustomState(construct, state_name, state_json=state_json)
    return state


def get_choice_state(construct, state_name):
    """
    Get Success state
    :param construct: object
                      Stack Scope
    :param state_name: string
                       Name of the state in the state machine
    :return: object
             Choice state object
    """
    state = sfn.Choice(construct, state_name)
    return state


def get_pass_state(construct, state_name, result_path="$.pass_state",
                   comment="Pass state for step-function",
                   result={}, path=False, output_path="$", parameters={}):
    """
    Get the Pass state for Step-Function
    Parameters
    ----------
    construct : object
                Stack Scope
    state_name : string
                 Name for the state
    result_path : string
                  Path for the result
    comment : string
              Comment assigned for the pass state
    result : object/string
             Result for the pass state
    path : bool
           Flag to check if the given result is object or string
    output_path : string
           Output path for the result
    parameters : dict
           Parameters to be collected
    Returns
    -------
    State object
    """
    result = sfn.Result.from_json_path_at(result) if path \
        else sfn.Result.from_object(result)
    state = sfn.Pass(
            construct, state_name, comment=comment,
            result_path=result_path, result=result, output_path=output_path,
            parameters=parameters)
    return state


def get_condition_state(
    conditional_state,
    comparison_path,
    transition_state,
    type="bool",
    comparison_val=True,
):
    """
    Get condition state
    :param conditional_state: object
                             State object to implement the conditions
    :param comparison_path: string
                            Path to compare the condition with
    :param transition_state: object
                             State object for the next state if condition
                             matches
    :param type: string
                 Type of condition
    :param comparison_val: string/bool
                           Value to compare in the condition
     :return: object
              Choice state object
    """
    if type == "bool":
        state = conditional_state.when(
            sfn.Condition.boolean_equals(comparison_path, comparison_val),
            transition_state
        )

    return state


def get_wait_state(construct, state_name, duration, comment=None):
    """
    Get wait state for step function
    Parameters
    ----------
    construct : object
                Stack Scope
    state_name : string
                 Name for state
    duration : int
               Duration for the wait
    comment : string
              Comment for the state
    Returns
    -------
    State object
    """
    comment = comment if comment else f"Waiting For {duration}"
    state = sfn.Wait(construct, state_name, time=sfn.WaitTime.duration(
            Duration.seconds(duration)), comment=comment)
    return state


def get_succeed_state(construct, state_name="Succeeded"):
    """
     Get Success state
    :param construct: object
                      Stack Scope
    :param state_name: string
                       Name of the state in the state machine
    :return: object
             Success state object
    """
    succeeded_state = sfn.Succeed(construct, state_name)
    return succeeded_state


def get_failed_state(
    construct, state_name="Failed", cause="One of the State Failed",
        error="Failed"):
    """
     Get Failed state
    :param construct: object
                      Stack Scope
    :param state_name: string
                       Name of the state in the state machine
    :param cause: string
                  Cause for the failure
    :param error: string
                  Error message for the failure state
    :return: object
             Failed state object
    """
    failed_state = sfn.Fail(construct, state_name, cause=cause, error=error)
    return failed_state


def get_sns_publish_state(construct, state_name, topic, message, path=True,
                          result_path="$.sns", subject="SNS Message"):
    """
    Get SNS publish state for step function
    Parameters
    ----------
    construct : object
                Stack Scope
    state_name : string
                 Name of the state in the state machine
    topic : object
            SNS Topic
    message : string/dict
              Either string or dict for the message object
    path : bool
           Flag to determine if the message is from Json or Path in state
           machine
    result_path : string
                  Path for the state result
    subject: string
             Subject to be passed on message
    Returns
    -------
    State object
    """
    message = sfn.TaskInput.from_json_path_at(message) if path else \
        sfn.TaskInput.from_object(message)
    state = sfn_tasks.SnsPublish(
            construct, state_name, topic=topic,
            message=message, result_path=result_path,
            subject=subject)
    return state


def create_sfn_tasks_instance_fleet(
    instance_role_type: str,
    instance_type: str,
    target_on_demand_capacity: int = 0,
    target_spot_capacity: int = 0,
    bid_price: str = None,  # type: ignore
    weighted_capacity: int = 0,
) -> ecc.InstanceFleetConfigProperty:
    """Create instance fleets for the EMR cluster.

    Args:
        instance_role_type (str): instance role type (MASTER, CORE, or TASK)
        instance_type (str): instance group (t2...., m5....)
        target_on_demand_capacity (int, optional): number of on demand
        instances. Defaults to 0.
        target_spot_capacity (int, optional): number of spot instances.
        Defaults to 0.
        bid_price (str, optional): bid price for spot instance.
        Defaults to None.
        weighted_capacity (int, optional):weighted capacity for each instance.
         Defaults to 0.

    Returns:
        ecc.InstanceFleetConfigProperty: instance fleet config
    """

    if instance_role_type == "TASK":
        fleet = ecc.InstanceFleetConfigProperty(
            instance_fleet_type=eval(
                    f"ecc.InstanceRoleType.{instance_role_type}"),
            # the properties below are optional
            instance_type_configs=[
                ecc.InstanceTypeConfigProperty(
                    instance_type=instance_type,
                    bid_price=bid_price,
                    weighted_capacity=weighted_capacity,
                )
            ],
            launch_specifications=
            ecc.InstanceFleetProvisioningSpecificationsProperty(
                spot_specification=ecc.SpotProvisioningSpecificationProperty(
                    timeout_action=ecc.SpotTimeoutAction.TERMINATE_CLUSTER,
                    timeout_duration_minutes=600)),
            name=instance_role_type,
            target_on_demand_capacity=target_on_demand_capacity,
            target_spot_capacity=target_spot_capacity
        )
    else:
        fleet = ecc.InstanceFleetConfigProperty(
            instance_fleet_type=eval(
                    f"ecc.InstanceRoleType.{instance_role_type}"),
            # the properties below are optional
            instance_type_configs=[
                ecc.InstanceTypeConfigProperty(
                    instance_type=instance_type,
                    bid_price=bid_price,
                    weighted_capacity=weighted_capacity,
                )
            ],
            name=instance_role_type,
            target_on_demand_capacity=target_on_demand_capacity,
            target_spot_capacity=target_spot_capacity,
        )

    return fleet


def create_sfn_tasks_instances(
    ec2_subnet_id: str,
    emr_managed_master_security_group: str,
    emr_managed_slave_security_group: str,
    bid_price: str,
    config={},
) -> ecc.InstancesConfigProperty:
    """Create instances for the EMR cluster.

    Args:
        ec2_subnet_id (str): subnet id for the instance
        emr_managed_master_security_group (str): master security group
        emr_managed_slave_security_group (str): slave security group
        bid_price (str): bid price for spot instance (TASK)
        config (dict): configurations needed for instances

    Returns:
        ecc.InstancesConfigProperty: instances config
    """
    master_instance_type = (
        config["master_instance_type"]
        if "master_instance_type" in config
        else "m5.xlarge"
    )
    core_instance_type = (
        config["core_instance_type"] if "core_instance_type" in config
        else "m5.xlarge"
    )
    core_on_demand = config["core_on_demand"] if "core_on_demand" in config \
        else 1
    core_weighted = config["core_weighted"] if "core_weighted" in config \
        else 1
    core_spot = config["core_spot"] if "core_spot" in config else 0

    instances = ecc.InstancesConfigProperty(
        ec2_subnet_id=ec2_subnet_id,
        emr_managed_master_security_group=emr_managed_master_security_group,
        emr_managed_slave_security_group=emr_managed_slave_security_group,
        # keep_job_flow_alive_when_no_steps=True,
        termination_protected=False,
        instance_fleets=[
            create_sfn_tasks_instance_fleet(
                "MASTER",
                master_instance_type,
                target_on_demand_capacity=1,
                weighted_capacity=1,
            ),
            create_sfn_tasks_instance_fleet(
                "CORE",
                core_instance_type,
                target_on_demand_capacity=core_on_demand,
                target_spot_capacity=core_spot,
                weighted_capacity=core_weighted,
            ),
            create_sfn_tasks_instance_fleet(
                "TASK",
                "m5.xlarge",
                target_on_demand_capacity=0,
                target_spot_capacity=3,
                bid_price=bid_price,
                weighted_capacity=1,
            ),
        ],
    )

    return instances


def create_sfn_tasks_emr_cluster(
    scope: Stack,
    step_name: str,
    cluster_name: str,
    cluster_role: iam.IRole,
    service_role: iam.IRole,
    emr_config_bucket: str,
    bootstrap_uri: str,
    log_uri: str,
    ec2_subnet_id: str,
    emr_managed_master_security_group: str,
    emr_managed_slave_security_group: str,
    bid_price: str,
    weighted_capacity: int,
    applications: list = ["spark"],
    release_label: str = "emr-6.2.0",
) -> ecc:
    """Create EMR cluster.

    Args:
        scope (Stack): scope of the Stack
        step_name (str): step name in the step function
        cluster_name (str): cluster name
        cluster_role (iam.IRole): role for the instance cluster to be created
        service_role (iam.IRole): role for the instance service
        emr_config_bucket (str): bucket name to load the emr configs
        bootstrap_uri (str): script path for bootstraping the cluster
        log_uri (str): path to store the emr logs
        ec2_subnet_id (str): subnet id for the instance
        emr_managed_master_security_group (str): master security group
        emr_managed_slave_security_group (str): slave security group
        bid_price (str): bid price for spot instance (TASK)
        weighted_capacity (int): weighted capacity for each instance
        applications (list, optional): list of applications to be included
         in the cluster. Defaults to ["spark"].
        release_label (str, optional): release version of the emr cluster.
        Defaults to "emr-6.2.0".

    Returns:
        ecc: EMR cluster
    """
    cluster = ecc(
        scope,
        step_name,
        instances=create_sfn_tasks_instances(
            ec2_subnet_id,
            emr_managed_master_security_group,
            emr_managed_slave_security_group,
            bid_price,
            weighted_capacity,
        ),
        cluster_role=cluster_role,
        name=cluster_name,
        service_role=service_role,
        applications=[
            ecc.ApplicationConfigProperty(name=application)
            for application in applications
        ],
        bootstrap_actions=[
            ecc.BootstrapActionConfigProperty(
                name="Install external libraries",
                script_bootstrap_action=ecc.ScriptBootstrapActionConfigProperty(
                    path=prepare_s3_path(emr_config_bucket, bootstrap_uri),
                ),
            )
        ],
        log_uri=prepare_s3_path(emr_config_bucket, log_uri),
        release_label=release_label,
        scale_down_behavior=
        ecc.EmrClusterScaleDownBehavior.TERMINATE_AT_TASK_COMPLETION,
        # tags=[CfnTag(key="key", value="value")],
        visible_to_all_users=True,
        result_path="$.cluster",
    )

    return cluster


def add_sfn_tasks_emr_step(
    scope: Stack,
    step_name: str,
    jar: str,
    args: list = [],
) -> eas:
    """Add execution step to the EMR cluster

    Args:
        scope (Stack): scope of the Stack
        step_name (str): name of the step in the step function
        jar (str): name of the jar file
        args (list, optional): list of args to execute in the EMR cluster.
        Defaults to [].

    Returns:
        eas: execution step in EMR
    """
    emr_step = eas(
        scope,
        step_name,
        cluster_id=sfn.JsonPath.string_at("$.cluster.ClusterId"),
        name=step_name,
        jar=jar,
        args=args,
        action_on_failure=sfn_tasks.ActionOnFailure.CONTINUE,
        result_path="$.task",
    )

    return emr_step


def terminate_sfn_tasks_emr_cluster(
    scope: Stack,
    step_name: str,
) -> etc:
    """Terminate the cluster.

    Args:
        scope (Stack): scope of the Stack
        step_name (str): name of the step in step function

    Returns:
        etc: step to terminate the cluster
    """
    terminate_cluster = etc(
        scope,
        step_name,
        cluster_id=sfn.JsonPath.string_at("$.cluster.ClusterId"),
    )

    return terminate_cluster
