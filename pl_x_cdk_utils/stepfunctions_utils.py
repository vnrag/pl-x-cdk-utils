from aws_cdk import (
    aws_iam as iam,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
    Stack,
)
from aws_cdk.aws_stepfunctions_tasks import (
    EmrAddStep as eas,
    EmrCreateCluster as ecc,
    EmrTerminateCluster as etc,
)

from pl_x_cdk_utils.helpers import (
    prepare_s3_path,
)


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
        step_name,
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
        target_on_demand_capacity (int, optional): number of on demand instances. Defaults to 0.
        target_spot_capacity (int, optional): number of spot instances. Defaults to 0.
        bid_price (str, optional): bid price for spot instance. Defaults to None.
        weighted_capacity (int, optional):weighted capacity for each instance. Defaults to 0.

    Returns:
        ecc.InstanceFleetConfigProperty: instance fleet config
    """
    fleet = None

    if instance_role_type == "TASK":
        fleet = ecc.InstanceFleetConfigProperty(
            instance_fleet_type=eval(f"ecc.InstanceRoleType.{instance_role_type}"),
            # the properties below are optional
            instance_type_configs=[
                ecc.InstanceTypeConfigProperty(
                    instance_type=instance_type,
                    bid_price=bid_price,
                    weighted_capacity=weighted_capacity,
                )
            ],
            launch_specifications=ecc.InstanceFleetProvisioningSpecificationsProperty(
                spot_specification=ecc.SpotProvisioningSpecificationProperty(
                    timeout_action=ecc.SpotTimeoutAction.TERMINATE_CLUSTER,
                    timeout_duration_minutes=600,
                ),
            ),
            name=instance_role_type,
            target_on_demand_capacity=target_on_demand_capacity,
            target_spot_capacity=target_spot_capacity,
        )
    else:
        fleet = ecc.InstanceFleetConfigProperty(
            instance_fleet_type=eval(f"ecc.InstanceRoleType.{instance_role_type}"),
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
    weighted_capacity: int,
) -> ecc.InstancesConfigProperty:
    """Create instances for the EMR cluster.

    Args:
        ec2_subnet_id (str): subnet id for the instance
        emr_managed_master_security_group (str): master security group
        emr_managed_slave_security_group (str): slave security group
        bid_price (str): bid price for spot instance (TASK)
        weighted_capacity (int): weighted capacity for each instance

    Returns:
        ecc.InstancesConfigProperty: instances config
    """
    instances = ecc.InstancesConfigProperty(
        ec2_subnet_id=ec2_subnet_id,
        emr_managed_master_security_group=emr_managed_master_security_group,
        emr_managed_slave_security_group=emr_managed_slave_security_group,
        # keep_job_flow_alive_when_no_steps=True,
        termination_protected=False,
        instance_fleets=[
            create_sfntask_instance_fleet(
                "MASTER",
                "m5.xlarge",
                target_on_demand_capacity=1,
                target_spot_capacity=0,
                weighted_capacity=weighted_capacity,
            ),
            create_sfntask_instance_fleet(
                "CORE",
                "m5.xlarge",
                target_on_demand_capacity=1,
                target_spot_capacity=0,
                weighted_capacity=weighted_capacity,
            ),
            create_sfntask_instance_fleet(
                "TASK",
                "m5.xlarge",
                target_on_demand_capacity=0,
                target_spot_capacity=3,
                bid_price=bid_price,
                weighted_capacity=weighted_capacity,
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
        applications (list, optional): list of applications to be included in the cluster. Defaults to ["spark"].
        release_label (str, optional): release version of the emr cluster. Defaults to "emr-6.2.0".

    Returns:
        ecc: EMR cluster
    """
    cluster = ecc(
        scope,
        step_name,
        instances=create_sfntask_instances(
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
        scale_down_behavior=ecc.EmrClusterScaleDownBehavior.TERMINATE_AT_TASK_COMPLETION,
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
        args (list, optional): list of args to execute in the EMR cluster. Defaults to [].

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
