from aws_cdk import (
    aws_s3 as s3
)


def get_bucket_object_from_name(construct, bucket_name):
    """
    Get bucket object with provided bucket name
    :param construct: object
                      Stack Scope
    :param bucket_name: string
                        Name of the bucket
    :return: object
             Bucket object
    """
    bucket_object = s3.Bucket.from_bucket_name(
        construct, f"profile-for-bucket-{bucket_name}", bucket_name)
    return bucket_object
