from aws_cdk.aws_kinesisfirehose import CfnDeliveryStream as Firehose


dynamic_output_path = (
    "/year=!{timestamp:yyyy}/month=!{timestamp:MM}/!"
    "{timestamp:dd}_rand=!{firehose:random-string}"
)
dynamic_error_path = (
    "_failures/!{firehose:error-output-type}/year=!"
    "{timestamp:yyyy}/month=!{timestamp:MM}/!{timestamp:dd}"
)


def get_delivery_stream_for_s3_destination(construct, name, config, id=None):
    """
    Delivery stream for s3
    :param construct: object
                      Stack Scope
    :param name: string
                 Name for the delivery stream
    :param config: object
                   Configuration object for s3 destination
    :param id: string
                logical id of the cdk construct
    :return: object
            S3 firehose object
    """
    param_id = id if id else f"profile-for-delivery-stream-{name}"
    s3_firehose_delivery_stream = Firehose(
        construct,
        param_id,
        delivery_stream_name=name,
        extended_s3_destination_configuration=config,
    )
    return s3_firehose_delivery_stream


def get_buffering_hint(interval=60, size=128):
    """
    Buffering hint for s3 destination property
    :param interval: int
                     Time interval for buffering, in seconds
    :param size: int
                 Memory for buffering, in mbs
    :return: object
             Buffering object
    """
    buff_property = Firehose.BufferingHintsProperty(
        interval_in_seconds=interval, size_in_m_bs=size
    )
    return buff_property


def get_data_conversion_config_property(
    database_name, table_name, role_arn, compression="SNAPPY"
):
    """
    Data conversion property for s3 destination property
    :param database_name: string
                          Glue database name
    :param table_name: string
                       Glue table name
    :param role_arn: string
                     Role arn for the role
    :param compression: string
                        Compression type
    :return: object
             Data conversion config property for s3 destination property
    """
    config_property = Firehose.DataFormatConversionConfigurationProperty(
        enabled=True,
        output_format_configuration=Firehose.OutputFormatConfigurationProperty(
            serializer=Firehose.SerializerProperty(
                parquet_ser_de=Firehose.ParquetSerDeProperty(compression=compression)
            )
        ),
        input_format_configuration=Firehose.InputFormatConfigurationProperty(
            deserializer=Firehose.DeserializerProperty(
                hive_json_ser_de=Firehose.HiveJsonSerDeProperty()
            )
        ),
        schema_configuration=Firehose.SchemaConfigurationProperty(
            database_name=database_name, table_name=table_name, role_arn=role_arn
        ),
    )
    return config_property


def configure_extended_s3_destination_property(
    bucket_arn,
    output_prefix,
    log_group_name,
    log_stream_name,
    role_arn,
    db_name,
    table_name,
    dynamic_partition=False,
    processor_property=None,
    buffering_hints=None,
    error_prefix=None,
    is_dynamic_prefix=False,
):
    """
    Property for delivery stream for s3
    :param bucket_arn: string
                       Bucket arn for s3
    :param output_prefix: string
                        S3 path for output
    :param log_group_name: string
                        Log group name
    :param log_stream_name: string
                        Log stream name
    :param role_arn: string
                        Role arn for the role
    :param db_name: string
                        Glue database name
    :param table_name: string
                       Glue table name
    :param processor_property: list
                        List of Objects from processor_property
    :param dynamic_partition: boolean
                        Flag to enable dynamic partition
    :param buffering_hints: object
                        Buffering hint object
    :param error_prefix: string
                        S3 path for error output
    :param is_dynamic_prefix: boolean
                        Dynamic prefix added to output prefix
    :return: object
             Configuration for s3 destination delivery stream
    """
    buffering_hints = buffering_hints if buffering_hints else get_buffering_hint()
    log_option = Firehose.CloudWatchLoggingOptionsProperty(
        enabled=True, log_group_name=log_group_name, log_stream_name=log_stream_name
    )

    prefix = output_prefix + dynamic_output_path if is_dynamic_prefix else output_prefix
    error_output_prefix = (
        output_prefix + dynamic_error_path if is_dynamic_prefix else error_prefix
    )
    data_format_conversion_config = get_data_conversion_config_property(
        db_name, table_name, role_arn
    )
    processing_config = (
        Firehose.ProcessingConfigurationProperty(
            enabled=True, processors=processor_property
        )
        if processor_property
        else None
    )
    dynamic_partitioning_property = (
        Firehose.DynamicPartitioningConfigurationProperty(
            enabled=True,
            retry_options=Firehose.RetryOptionsProperty(duration_in_seconds=60),
        )
        if dynamic_partition
        else None
    )
    extended_s3_config = Firehose.ExtendedS3DestinationConfigurationProperty(
        bucket_arn=bucket_arn,
        buffering_hints=buffering_hints,
        prefix=prefix,
        error_output_prefix=error_output_prefix,
        cloud_watch_logging_options=log_option,
        data_format_conversion_configuration=data_format_conversion_config,
        role_arn=role_arn,
        processing_configuration=processing_config,
        dynamic_partitioning_configuration=dynamic_partitioning_property,
    )
    return extended_s3_config
