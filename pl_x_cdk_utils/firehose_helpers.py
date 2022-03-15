from aws_cdk.aws_kinesisfirehose import CfnDeliveryStream as FireHose


dynamic_output_path = "/year=!{timestamp:yyyy}/month=!{timestamp:MM}/!" \
                      "{timestamp:dd}_rand=!{firehose:random-string}"
dynamic_error_path = "_failures/!{firehose:error-output-type}/year=!" \
                     "{timestamp:yyyy}/month=!{timestamp:MM}/!{timestamp:dd}"


def get_delivery_stream_for_s3_destination(construct, name, config):
    s3_firehose_delivery_stream = FireHose(
        construct, f"profile-for-delivery-stream-{name}",
        delivery_stream_name=name,
        extended_s3_destination_configuration=config
    )
    return s3_firehose_delivery_stream


def get_buffering_hint(interval=60, size=128):
    buff_property = FireHose.BufferingHintsProperty(
        interval_in_seconds=interval, size_in_m_bs=size
    )
    return buff_property


def get_data_conversion_config_property(database_name, table_name, role_arn,
                                        compression='SNAPPY'):
    config_property = FireHose.DataFormatConversionConfigurationProperty(
        enabled=True,
        output_format_configuration=FireHose.OutputFormatConfigurationProperty(
            serializer=FireHose.SerializerProperty(
                parquet_ser_de=FireHose.ParquetSerDeProperty(
                    compression=compression))
        ),
        input_format_configuration=FireHose.InputFormatConfigurationProperty(
            deserializer=FireHose.DeserializerProperty(
                hive_json_ser_de=FireHose.HiveJsonSerDeProperty()
            )
        ), schema_configuration=FireHose.SchemaConfigurationProperty(
            database_name=database_name, table_name=table_name,
            role_arn=role_arn)
    )
    return config_property


def configure_extended_s3_destination_property(
        bucket_arn, output_prefix, log_group_name, log_stream_name,
        role_arn, db_name, table_name, buffering_hints=None):
    buffering_hints = buffering_hints if \
        buffering_hints else get_buffering_hint()
    log_option = FireHose.CloudWatchLoggingOptionsProperty(
        enabled=True, log_group_name=log_group_name,
        log_stream_name=log_stream_name)

    prefix = output_prefix + dynamic_output_path
    error_output_prefix = output_prefix + dynamic_error_path
    data_format_conversion_config = get_data_conversion_config_property(
        db_name, table_name, role_arn)
    extended_s3_config = FireHose.ExtendedS3DestinationConfigurationProperty(
        bucket_arn=bucket_arn, buffering_hints=buffering_hints,
        prefix=prefix, error_output_prefix=error_output_prefix,
        cloud_watch_logging_options=log_option,
        data_format_conversion_configuration=data_format_conversion_config,
        role_arn=role_arn
    )
    return extended_s3_config
