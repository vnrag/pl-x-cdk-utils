from aws_cdk import (
    aws_iam as iam
)


def get_policy_statement(actions, resources=['*']):
    """
    Get policy statement for given actions and resources
    :param actions: list
                    List of permissions for the role
    :param resources: list
                      List of resources for the permissions
    :return: object
             IAM role object
    """
    policy_statement = iam.PolicyStatement(
        actions=actions,
        resources=resources
    )
    return policy_statement


def get_added_policies_as_role(construct, role_name, principal, actions_list,
                               resources_list=['*']):
    """
    :param construct: object
                      Stack Scope
    :param role_name: string
                      Role name
    :param principal: string
                      AWS principal that will be using the role
    :param actions_list: list
                         List of permission actions for the role
    :param resources_list: list
                           List for the resources we want to give the permission
    :return: object
             IAM role object
    """
    role = iam.Role(
        construct, f"profile-for-role-{role_name}", role_name=role_name,
        assumed_by=iam.ServicePrincipal(principal))
    role.add_to_policy(iam.PolicyStatement(
        resources=resources_list,
        actions=actions_list
    ))
    return role


def get_role_from_arn(construct, role_name):
    """
    Get role object from the arn with given role name
    :param construct: object
                      Stack Scope
    :param role_name: string
                      IAM role name
    :return: object
             IAM role object
    """
    role_arn = f"arn:aws:iam::{construct.account}:role/{role_name}"
    role = iam.Role.from_role_arn(
        construct, f"profile-for-role-{role_name}", role_arn=role_arn)
    return role
