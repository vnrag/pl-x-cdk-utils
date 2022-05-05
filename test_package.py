from boto3_utils_layer import boto3_utils_layer

test = boto3_utils_layer.get_ssm_value("ConfigBucketName")
print("!!!!!", test)