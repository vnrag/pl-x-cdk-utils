from aws_cdk import aws_ecs as ecs
from aws_cdk import aws_events as events, aws_events_targets as targets


def add_event_rule(
    construct,
    rule_name,
    cdk_functionality,
    id=None,
    minute="0",
    hour="6",
    day="*",
    month="*",
    year="*",
    event_input={},
    cdk_function="state_machine",
    description="",
    enabled=True,
    kwargs=None,
):
    """
    Event rule for the cdk functionalities
    :param construct: object
                      Stack Scope
    :param rule_name: string
                      Name for the rule
    :param cdk_functionality: object
                              CDK handler where we want to implement rule
    :param id: string
                logical id of the cdk construct
    :param minute: string
                   Minute for cron
    :param hour: string
                Hour for cron
    :param day: string
                Day for cron
    :param month: string
                   Month for cron
    :param year: string
                   Year for cron
    :param event_input: dict
                        Input for the execution on given rule
    :param description: string
                        event rule description
    :param enabled: boolean
                        event rule enabled status
    :param kwargs: dict
                   Dict with parameters for task
    :return:
    """
    param_id = id if id else f"profile-for-event-{rule_name}"
    # Event rule for provided cron values
    event_rule = events.Rule(
        construct,
        param_id,
        rule_name=rule_name,
        description=description,
        enabled=enabled,
        schedule=events.Schedule.cron(
            minute=minute, hour=hour, day=day, month=month, year=year
        ),
    )
    # Event input
    if cdk_function == "state_machine":
        event_rule.add_target(
            targets.SfnStateMachine(
                cdk_functionality,
                input=events.RuleTargetInput.from_object(event_input),
            )
        )
    if cdk_function == "lambda":
        event_rule.add_target(
            targets.LambdaFunction(
                cdk_functionality,
                event=events.RuleTargetInput.from_object(event_input),
            )
        )
    if cdk_function == "ecs_task":
        kwargs.update({"platform_version": ecs.FargatePlatformVersion.LATEST})\
            if "platform_version" not in kwargs else ""
        kwargs.update({"task_definition": cdk_functionality})\
            if "task_definition" not in kwargs else ""
        event_rule.add_target(targets.EcsTask(**kwargs))
