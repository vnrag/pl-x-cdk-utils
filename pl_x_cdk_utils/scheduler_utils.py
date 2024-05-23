from aws_cdk import aws_scheduler
import json


def get_schedule(
    construct,
    schedule_expression,
    target_type,
    resource_arn,
    role_arn,
    cluster_arn=None,
    subnet_ids=None,
    security_group_id=None,
    ecs_launch_type="FARGATE",
    ecs_task_count=1,
    ecs_platform_version="LATEST",
    ecs_vpc_assign_public_ip="DISABLED",
    schedule_input=None,
    target=None,
    id=None,
    flexible_time_window="OFF",
    description=None,
    end_date=None,
    group_name=None,
    kms_key_arn=None,
    name=None,
    schedule_expression_timezone=None,
    start_date=None,
    state=None,
):
    """
    Get schedule object with provided parameters
    :param construct: object
                      Stack Scope
                      e.g. self
    :param schedule_expression: string
                                e.g. "cron(0 0 * * ? *)"
    :param target_type: string e.g. "lambda", "ecs", "glue", "stepfunction"
    :param resource_arn: string
    :param role_arn: string
    :param cluster_arn string
    :param subnet_ids list
    :param security_group_id string
    :param ecs_launch_type string
    :param ecs_task_count bigint
    :param ecs_platform_version string
    :param ecs_vpc_assign_public_ip string
    :param schedule_input dict
    :param target aws_scheduler.CfnSchedule.TargetProperty
    :param id: string
    :param flexible_time_window: string defaults to "OFF"
    :param description: string
    :param end_date: string?
    :param group_name: string
    :param kms_key_arn: string
    :param name: string
    :param schedule_expression_timezone: string e.g. "Berlin/Europe"
    :param start_date: string?
    :param state: string
    """

    id = id if id else f"Scheduler-{name}"
    schedule_input = json.dumps(schedule_input) if schedule_input else None

    if not target:
        if target_type == "ecs":
            target = aws_scheduler.CfnSchedule.TargetProperty(
                arn="arn:aws:scheduler:::aws-sdk:ecs:runTask",
                role_arn=role_arn,
                input=json.dumps(
                    {
                        "TaskDefinition": resource_arn,
                        "Cluster": cluster_arn,
                        "Count": ecs_task_count,
                        "LaunchType": ecs_launch_type,
                        "PlatformVersion": ecs_platform_version,
                        "NetworkConfiguration": {
                            "AwsvpcConfiguration": {
                                "Subnets": subnet_ids,
                                "SecurityGroups": [security_group_id],
                                "AssignPublicIp": ecs_vpc_assign_public_ip
                            }
                        },
                        "PlacementConstraints": [],
                        "PlacementStrategy": [],
                        "Tags": [],
                        "EnableECSManagedTags": True
                    }
                ),
            )
        elif target_type == "lambda":
            target = aws_scheduler.CfnSchedule.TargetProperty(
                arn=resource_arn,
                role_arn=role_arn,
                input=schedule_input,
            )
        elif target_type == "glue":
            target = aws_scheduler.CfnSchedule.TargetProperty(
                arn="arn:aws:scheduler:::aws-sdk:glue:startJobRun",
                role_arn=role_arn,
                input=schedule_input,
            )
        elif target_type == "stepfunction":
            target = aws_scheduler.CfnSchedule.TargetProperty(
                arn=resource_arn,
                role_arn=role_arn,
                input=schedule_input,
            )

    return aws_scheduler.CfnSchedule(
        construct,
        id,
        flexible_time_window=aws_scheduler.CfnSchedule.FlexibleTimeWindowProperty(
            mode=flexible_time_window
        ),
        schedule_expression=schedule_expression,
        schedule_expression_timezone=schedule_expression_timezone,
        name=name,
        description=description,
        end_date=end_date,
        group_name=group_name,
        kms_key_arn=kms_key_arn,
        start_date=start_date,
        target=target,
        state=state,
    )
