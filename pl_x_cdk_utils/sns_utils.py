from aws_cdk import aws_sns as sns


def get_sns_topic(construct, topic_name, display_name="Subscription Topic",
                  fifo=False):
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

    Returns
    -------
    SNS topic object
    """
    topic = sns.Topic(
        construct, f"Topic{topic_name}",
        display_name=display_name,
        fifo=fifo,
        topic_name=topic_name)
    return topic
