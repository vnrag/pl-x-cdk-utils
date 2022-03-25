from aws_cdk import aws_sns as sns


def get_sns_topic(construct, topic_name, display_name="Subscription Topic",
                  fifo=False, id=None):
    """
    Create SNS topic
    Parameters
    ----------
    construct : object
                Stack Scope
    topic_name : string
                 Name for the topic
    display_name : string
                   Display name for the topic
    fifo : bool
           Boolean for the fifo
    id: string
        Id to reuse the existing resource if available
    Returns
    -------
    SNS topic object
    """
    if id:
        topic = sns.from_topic_arn(
                construct, id,
                topic_arn=f"arn:aws:sns:{construct.region}:"
                          f"{construct.account}:{topic_name}")
    else:
        topic = sns.Topic(
            construct, f"Topic{topic_name}",
            display_name=display_name,
            fifo=fifo,
            topic_name=topic_name)
    return topic
