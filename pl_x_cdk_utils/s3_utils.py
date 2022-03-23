from aws_cdk import aws_s3 as s3


def get_bucket_object_from_name(construct, bucket_name, id=None):
    """
    Get bucket object with provided bucket name
    :param construct: object
                      Stack Scope
    :param bucket_name: string
                        Name of the bucket
    :param id: string
                logical id of the cdk construct
    :return: object
             Bucket object
    """
    param_id = id if id else f"profile-for-bucket-{bucket_name}"
    bucket_object = s3.Bucket.from_bucket_name(construct, param_id, bucket_name)
    return bucket_object


def get_bucket_object_from_arn(construct, bucket_arn, id=None):
    """
    Get bucket object with provided bucket arn for cross-account buckets
    :param construct: object
                      Stack Scope
    :param bucket_arn: string
                        ARN of the bucket
    :param id: string
                logical id of the cdk construct
    :return: object
             Bucket object
    """
    param_id = id if id else f"profile-for-bucket-{bucket_arn}"
    bucket_object = s3.Bucket.from_bucket_arn(construct, param_id, bucket_arn)
    return bucket_object
