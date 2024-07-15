from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    aws_sqs,
    aws_lambda,
    aws_lambda_event_sources,
    aws_sns,
    aws_sns_subscriptions,
    aws_cloudwatch,
)


class SQSUtils:
    @staticmethod
    def get_queue_from_name(
        stack: Stack,
        queue_name: str,
    ) -> aws_sqs.Queue:
        """
        Retrieves an existing SQS queue by its name.

        Args:
        - stack (aws_cdk.Stack): The Stack to use.
        - queue_name (str): The name of the queue to retrieve.

        Returns:
        - aws_sqs.Queue
        """
        return aws_sqs.Queue.from_queue_attributes(
            stack,
            f"ImportedQueue{queue_name.split(':')[-1]}",
            queue_name,
        )

    @staticmethod
    def get_queue_from_arn(
        stack: Stack,
        queue_arn: str,
    ) -> aws_sqs.Queue:
        """
        Retrieves an existing SQS queue by its ARN.

        Args:
        - stack (aws_cdk.Stack): The Stack to use.
        - queue_arn (str): The ARN of the queue to retrieve.

        Returns:
        - aws_sqs.Queue
        """
        # Generate a unique ID based on the ARN
        queue_id = queue_arn.split("/")[-1].split(":")[-1]

        return aws_sqs.Queue.from_queue_arn(
            stack,
            f"ImportedQueue{queue_id}",
            queue_arn,
        )

    @staticmethod
    def create_queue(
        stack: Stack,
        name: str,
        **kwargs,
    ) -> aws_sqs.Queue:
        """
        Create an SQS Queue with the provided name and kwargs.

        Args:
        - stack (aws_cdk.Stack): The Stack to use.
        - name (str): The name of the queue.
        - kwargs (dict): any additional props to use for the creation of the queue.

        Returns:
        - aws_sqs.Queue
        """
        if "data_key_reuse" in kwargs:
            kwargs["data_key_reuse"] = Duration.seconds(
                kwargs.pop("data_key_reuse")
            )
        if "delivery_delay" in kwargs:
            kwargs["delivery_delay"] = Duration.seconds(
                kwargs.pop("delivery_delay")
            )
        if "receive_message_wait_time" in kwargs:
            kwargs["receive_message_wait_time"] = Duration.seconds(
                kwargs.pop("receive_message_wait_time")
            )
        if "visibility_timeout" in kwargs:
            kwargs["visibility_timeout"] = Duration.seconds(
                kwargs.pop("visibility_timeout")
            )
        if "retention_period" in kwargs:
            kwargs["retention_period"] = Duration.seconds(
                kwargs.pop("retention_period")
            )
        if "deduplication_scope" in kwargs:
            if kwargs["deduplication_scope"] == "MESSAGE_GROUP":
                kwargs["deduplication_scope"] = aws_sqs.DeduplicationScope.MESSAGE_GROUP
            elif kwargs["deduplication_scope"] == "QUEUE":
                kwargs["deduplication_scope"] = aws_sqs.DeduplicationScope.QUEUE
        if "encryption" in kwargs:
            if kwargs["encryption"] == "KMS_MANAGED":
                kwargs["encryption"] = aws_sqs.QueueEncryption.KMS_MANAGED
            elif kwargs["encryption"] == "SQS_MANAGED":
                kwargs["encryption"] = aws_sqs.QueueEncryption.SQS_MANAGED
            elif kwargs["encryption"] == "UNENCRYPTED":
                kwargs["encryption"] = aws_sqs.QueueEncryption.UNENCRYPTED
            elif kwargs["encryption"] == "KMS":
                kwargs["encryption"] = aws_sqs.QueueEncryption.KMS
        if "fifo_throughput_limit" in kwargs:
            if kwargs["fifo_throughput_limit"] == "PER_MESSAGE_GROUP_ID":
                kwargs["fifo_throughput_limit"] = aws_sqs.FifoThroughputLimit.\
                    PER_MESSAGE_GROUP_ID
            elif kwargs["fifo_throughput_limit"] == "PER_QUEUE":
                kwargs["fifo_throughput_limit"] = aws_sqs.FifoThroughputLimit.PER_QUEUE
        if "removal_policy" in kwargs:
            if kwargs["removal_policy"] == "DESTROY":
                kwargs["removal_policy"] = RemovalPolicy.DESTROY
            elif kwargs["removal_policy"] == "RETAIN":
                kwargs["removal_policy"] = RemovalPolicy.RETAIN
        if "queue_name" in kwargs:
            kwargs["queue_name"] = kwargs["queue_name"][:80]
        else:
            kwargs["queue_name"] = name[:80]

        return aws_sqs.Queue(
            stack,
            name,
            **kwargs,
        )

    @staticmethod
    def attach_dead_letter_queue(
        queue: aws_sqs.Queue,
        dead_letter_queue: aws_sqs.Queue,
        **kwargs,
    ) -> None:
        """
        Attaches a Dead Letter Queue to an SQS queue.

        Args:
        - queue (aws_sqs.Queue): The queue to attach the Dead Letter Queue to.
        - dead_letter_queue (aws_sqs.Queue): The Dead Letter Queue to attach.
        - kwargs (dict): any additional props to use for the creation of the queue.

        Returns:
        - None
        """
        max_receive_count = kwargs.pop(
            "max_receive_count",
            5,
        )

        queue.add_dead_letter_queue(
            max_receive_count=max_receive_count,
            queue=dead_letter_queue,
        )

    @staticmethod
    def set_queue_attributes(
        queue: aws_sqs.Queue,
        attributes: dict,
    ) -> None:
        """
        Sets attributes for the SQS queue.

        Args:
        - queue (aws_sqs.Queue): The queue to set attributes for.
        - attributes (dict): A dictionary of attributes to set.

        Returns:
        - None
        """
        for key, value in attributes.items():
            queue.add_override(
                f"Properties.{key}",
                value,
            )

    @staticmethod
    def add_lambda_trigger(
        queue: aws_sqs.Queue,
        lambda_function: aws_lambda.Function,
        **kwargs,
    ) -> None:
        """
        Adds a Lambda function as a trigger for the specified SQS queue.

        Args:
        - queue (aws_sqs.Queue): The queue to add the trigger to.
        - lambda_function (aws_lambda.Function): The Lambda function to add as a trigger.
        - kwargs (dict): any additional props to use for adding the Lambga-Function.

        Returns:
        - None
        """
        batch_size = kwargs.pop(
            "batch_size",
            10,
        )

        event_source = aws_lambda_event_sources.SqsEventSource(
            queue,
            batch_size=batch_size,
        )

        lambda_function.add_event_source(
            event_source,
        )

    @staticmethod
    def add_sns_subscription(
        queue: aws_sqs.Queue,
        topic: aws_sns.Topic,
        **kwargs,
    ) -> None:
        """
        Subscribes the specified SQS queue to an SNS topic.

        Args:
        - queue (aws_sqqs.Queue): The queue to subscribe to the SNS topic.
        - topic (aws_sns.Topic): The SNS topic to subscribe to.

        Returns:
        - None
        """
        raw_message_delivery = kwargs.pop(
            "raw_message_delivery",
            True,
        )

        subscription = aws_sns_subscriptions.SqsSubscription(
            queue,
            raw_message_delivery=raw_message_delivery,
        )
        topic.add_subscription(
            subscription
        )

    @staticmethod
    def add_cloudwatch_alarm(
        stack: Stack,
        alarm_name: str,
        queue: str,
        **kwargs,
    ) -> aws_cloudwatch.Alarm:
        """
        Creates a CloudWatch alarm for the specified SQS queue.
        """
        metric = queue.metric(
            "ApproximateNumberOfMessagesVisible",
        )
        threshold = kwargs.pop(
            "threshold",
            100,
        )
        evaluation_periods = kwargs.pop(
            "evaluation_periods",
            1,
        )

        return aws_cloudwatch.Alarm(
            stack,
            alarm_name,
            metric=metric,
            threshold=threshold,
            evaluation_periods=evaluation_periods,
            **kwargs
        )
