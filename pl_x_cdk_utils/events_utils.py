from aws_cdk import (
    aws_events as events,
    aws_events_targets as targets
)


def add_event_rule(construct, rule_name, cdk_functionality, minute="0",
                   hour="6", month="*", week_day="*", year="*",
                   event_input={}, cdk_function='state_machine', 
                   description="", enabled=True):
    """
    Event rule for the cdk functionalities
    :param construct: object
                      Stack Scope
    :param rule_name: string
                      Name for the rule
    :param cdk_functionality: object
                              CDK handler where we want to implement rule
    :param minute: string
                   Minute for cron
    :param hour: string
                Hour for cron
    :param month: string
                   Month for cron
    :param week_day: string
                   Weekday for cron
    :param year: string
                   Year for cron
    :param event_input: dict
                        Input for the execution on given rule
    :param description: string
                        event rule description
    :param enabled: boolean
                        event rule enabled status
    :return:
    """
    # Event rule for provided cron values
    event_rule = events.Rule(construct, f"profile-for-event-{rule_name}",
                             description=description,
                             enabled=enabled,
                             schedule=events.Schedule.cron(
                                 minute=minute, hour=hour, month=month,
                                 week_day=week_day, year=year)
                             )
    # Event input
    if cdk_function == 'state_machine':
        event_rule.add_target(
            targets.SfnStateMachine(
                cdk_functionality,
                input=events.RuleTargetInput.from_object(event_input),
            )
        )
    if cdk_function == 'lambda':
        event_rule.add_target(
            targets.LambdaFunction(
                cdk_functionality,
                event=events.RuleTargetInput.from_object(event_input),
            )
        )