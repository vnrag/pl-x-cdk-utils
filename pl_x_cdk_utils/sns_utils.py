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
    param_id = id if id else f"Topic{topic_name}"
    topic = sns.Topic(
        construct, param_id,
        display_name=display_name,
        fifo=fifo,
        topic_name=topic_name)

    return topic
