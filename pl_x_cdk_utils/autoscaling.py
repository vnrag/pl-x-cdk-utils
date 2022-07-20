import uuid
from aws_cdk import aws_autoscaling as autoscaling


def get_autoscaling_group(construct, vpc, id=None,
                          auto_scaling_group_name=None, instance_type=None,
                          machine_image=None, min_capacity=None,
                          max_capacity=None):
    """

    Parameters
    ----------
    construct : object
                Stack Scope
    vpc : object
          IVPC object
    id : string
         Logical id
    auto_scaling_group_name: string
                             Name for the autoscaling group
    instance_type : object
                    Instance type object
    machine_image : object
                    Machine Image
    min_capacity : int
                   Minimum capacity in autoscaling
    max_capacity : int
                   Maximum capacity in autoscaling
    Returns
    -------

    """
    id = id if id else f"autoscaling-profile-{uuid.uuid4()}"
    group = autoscaling.AutoScalingGroup(
            construct, id=id, vpc=vpc,
            auto_scaling_group_name=auto_scaling_group_name,
            instance_type=instance_type,
            machine_image=machine_image,
            min_capacity=min_capacity,
            max_capacity=max_capacity
            )
    return group

