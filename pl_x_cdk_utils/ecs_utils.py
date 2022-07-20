import uuid
from aws_cdk import aws_ecs as ecs

from .logs_utils import create_log_group


def get_ecs_cluster(construct, id=None, cluster_name=None, vpc=None):
    """

    Parameters
    ----------
    construct : object
                Stack Scope
    id : string
         logical id of the cdk construct
    cluster_name : string
                   Name of the cluster
    vpc : object
         IVpc object

    Returns
    -------

    """
    id = id if id else f"ecs-cluster-profile-{uuid.uuid4()}"
    ecs_cluster = ecs.Cluster(construct, id=id,
                              cluster_name=cluster_name, vpc=vpc)
    return ecs_cluster


def get_fargate_task_definition(construct, family=None,
                                memory_limit_mib=None, cpu=None,
                                task_role=None, id=None):
    """

    Parameters
    ----------
    construct : object
                Stack Scope
    family : string
             Name for the task definiton
    memory_limit_mib : int
                       Amount of memory used by the task
    cpu : int
          CPU
    task_role : object
                IAM role
    id: string
        Logical id for the task

    Returns
    -------

    """
    id = id if id else f"ecs-fargate-task-profile-{uuid.uuid4()}"
    task_definition = ecs.FargateTaskDefinition(
            construct, id, family=family,
            memory_limit_mib=memory_limit_mib, cpu=cpu, task_role=task_role
            )
    return task_definition


def get_capacity_provider(construct, autoscaling_group, id=None,
                          capacity_provider_name=None):
    """

    Parameters
    ----------
    construct : object
                Stack Scope
    autoscaling_group : object
                   IAutoScalingGroup object
    id : string
         logical id of the cdk construct
    capacity_provider_name : string
                             Name for the capacity provider

    Returns
    -------

    """
    id = id if id else f"ecs-capacity-provider-profile-{uuid.uuid4()}"
    capacity_provider = ecs.AsgCapacityProvider(
            construct, id,
            auto_scaling_group=autoscaling_group,
            capacity_provider_name=capacity_provider_name)
    return capacity_provider


def get_optimized_amazon_linux_image():
    return ecs.EcsOptimizedImage.amazon_linux2()


def get_image_for_container(source="asset", source_path=None):
    """

    Parameters
    ----------
    source : string
             String to determine the source for image
    source_path : string/object
                  Source path for the image

    Returns
    -------

    """
    if source == "asset":
        image = ecs.ContainerImage.from_asset(source_path)
        return image


def get_logging_for_ecs(construct, log_group_name=None, log_id=None,
                        stream_prefix=None, log_group=None):
    """

    Parameters
    ----------
    construct : object
                Stack Scope
    log_group_name : string
                     Log-group name
    log_id : string
             Logical id for log group
    stream_prefix : string
                    Prefix for log stream
    log_group: object
              Log group object

    Returns
    -------

    """
    if log_group is None:
        log_id = log_id if log_id else f"{log_group_name}Ecs"
        log_group = create_log_group(
                construct, log_group_name,
                id=log_id
                )
    logging = ecs.LogDriver.aws_logs(
            log_group=log_group,
            stream_prefix=stream_prefix
            )
    return logging
