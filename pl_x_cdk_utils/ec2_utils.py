import uuid
from aws_cdk import aws_ec2 as ec2


def retrieve_vpc(construct, vpc_id, id=None):
    """

    Parameters
    ----------
    construct : object
                Stack Scope
    vpc_id : string
             VPC id
    id : string
         Logical id

    Returns
    -------

    """
    id = id if id else f"ec2-vpc-profile-{uuid.uuid4()}"
    vpc = ec2.Vpc.from_lookup(construct, id, vpc_id=vpc_id)
    return vpc


def get_default_vpc(construct):
    """
    Get the default VPC for the account

    Parameters
    ----------
    construct : object
                Stack Scope

    """
    vpc = ec2.Vpc.from_lookup(
        construct,
        "default-vpc",
        is_default=True,
        vpc_name="default"
    )
    return vpc


def get_subnets(construct, vpc_id, subnet_type="public"):
    """
    Get the subnets for the VPC
    """
    vpc = retrieve_vpc(construct, vpc_id)
    if subnet_type == "public":
        return vpc.public_subnets
    elif subnet_type == "private":
        return vpc.private_subnets
    else:
        raise ValueError(f"Invalid subnet type: {subnet_type}")


def get_ecs_instance(instance_type):
    """

    Parameters
    ----------
    instance_type : string
                    Type of instance

    Returns
    -------

    """
    return ec2.InstanceType(instance_type)
