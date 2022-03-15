from aws_cdk import (
    aws_kinesisfirehose as firehose,
)


def configure_deliver_stream_extended_s3_destination():
    extended_s3_config = firehose.CfnDeliveryStream.ExtendedS3DestinationConfigurationProperty(bucket_arn=data_bucket.bucket_arn,
                                                                                                   buffering_hints=firehose.CfnDeliveryStream.BufferingHintsProperty(
                                                                                                       interval_in_seconds=60,
                                                                                                       size_in_m_bs=128
                                                                                                   ),
                                                                                                   cloud_watch_logging_options=firehose.CfnDeliveryStream.CloudWatchLoggingOptionsProperty(
                                                                                                       enabled=True,
                                                                                                       log_group_name=log_group.log_group_name,
                                                                                                       log_stream_name=log_stream.log_stream_name),
                                                                                                   # for dynamic partitionning
                                                                                                   prefix="nssFirehose/year=!{timestamp:yyyy}/month=!{timestamp:MM}/!{timestamp:dd}_rand=!{firehose:random-string}",
                                                                                                   error_output_prefix="nssFirehoseFailures/!{firehose:error-output-type}/year=!{timestamp:yyyy}/month=!{timestamp:MM}/!{timestamp:dd}",
                                                                                                   data_format_conversion_configuration=firehose.CfnDeliveryStream.DataFormatConversionConfigurationProperty(enabled=True,
                                                                                                                                                                                                             output_format_configuration=firehose.CfnDeliveryStream.OutputFormatConfigurationProperty(
                                                                                                                                                                                                                 serializer=firehose.CfnDeliveryStream.SerializerProperty
                                                                                                                                                                                                                 (parquet_ser_de=firehose.CfnDeliveryStream.ParquetSerDeProperty(compression='SNAPPY'))),
                                                                                                                                                                                                             input_format_configuration=firehose.CfnDeliveryStream.InputFormatConfigurationProperty(
                                                                                                                                                                                                                 deserializer=firehose.CfnDeliveryStream.DeserializerProperty(hive_json_ser_de=firehose.CfnDeliveryStream.HiveJsonSerDeProperty())),
                                                                                                                                                                                                             schema_configuration=firehose.CfnDeliveryStream.SchemaConfigurationProperty(
                                                                                                                                                                                                                 database_name="kinesis_webtracking", table_name="nss", role_arn=firehose_role.role_arn)
                                                                                                                                                                                                             ),
                                                                                                   # dynamic_partitioning_configuration={"Enabled":True},
                                                                                                   # # processing_configuration={}
                                                                                                   role_arn=firehose_role.role_arn,

                                                                                                   )
