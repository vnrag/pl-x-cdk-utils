import aws_cdk as cdk

from aws_cdk import (
    aws_glue,
    aws_glue_alpha as glue,
    aws_iam as iam,
    Stack,
)


def create_glue_crawler(
    construct,
    name,
    db_name,
    role,
    table_prefix,
    targets,
    configuration=None,
    schedule=None,
    id=None,
):
    """
    :param construct: object
                      Stack Scope
    :param name: string
                 Name for the glue-crawler
    :param db_name: string
                    Name for database
    :param role: object
                 AWS IAM role object
    :param table_prefix: string
                        Prefix we want to use in table
    :param targets: list
                    List of Key, value arguments for target,
                    eg: {
                            "s3Targets": [
                                {
                                    "path": f"s3://{bucket_name}/provider/"
                                },
                                {
                                    "path": f"s3://{bucket_name}/provider/"
                                },
                            ]
                        }
    :param configuration: string
                          Additional configurations for glue-crawler
    :param schedule: string
                     String value for cron
    :param id: string
                logical id of the cdk construct
    :return: object
             Glue-Crawler object
    """
    param_id = id if id else f"profile-for-crawler-{name}"
    glue_crawler = aws_glue.CfnCrawler(
        construct,
        param_id,
        database_name=db_name,
        role=role,
        table_prefix=table_prefix,
        targets=targets,
        name=name,
    )
    if configuration:
        glue_crawler.configuration = configuration
    if schedule:
        glue_crawler.schedule = aws_glue.CfnCrawler.ScheduleProperty(
            schedule_expression=schedule
        )

    return glue_crawler


def create_glue_database(construct, name, id=None):
    """
    Create Glue database
    :param construct: object
                      Stack Scope
    :param name: string
                Name for the glue database
    :param id: string
                logical id of the cdk construct
    :return: object
            Glue database object
    """
    param_id = id if id else f"profile-for-glue-db-{name}"
    glue_database = glue.Database(construct, param_id, database_name=name)
    return glue_database


def create_glue_table(
    construct,
    database,
    table_name,
    bucket,
    s3_prefix,
    columns,
    compressed=True,
    data_format=None,
    id=None,
):
    """
    Create Glue table
    :param construct: object
                      Stack Scope
    :param database: object
                     Glue database objectb
    :param table_name: string
                       Name for the table
    :param bucket: object
                   S3 bucket object
    :param s3_prefix: string
                      S3 prefix for the data
    :param columns: list
                    List of the columns with name and their type
    :param compressed: bool
                       Boolean value for the compression
    :param data_format: object
                        Format object for the datasets available
    :param id: string
                logical id of the cdk construct
    :return: object
             Glue table object
    """
    param_id = id if id else f"profile-for-glue-table-{table_name}"
    data_format = data_format if data_format else glue.DataFormat.PARQUET
    glue_table = glue.Table(
        construct,
        param_id,
        database=database,
        table_name=table_name,
        columns=columns,
        bucket=bucket,
        s3_prefix=s3_prefix,
        data_format=data_format,
        compressed=compressed,
    )
    return glue_table


def prepare_glue_table_columns(
    col_details,
    struct_cols={},
    key_type=glue.Schema.STRING,
    input_string="",
    is_primitive=False,
):
    """
    Prepare columns list for the Glue table with column details dict
    :param col_details: dict
                        Columns details as dict with name and type
    :param struct_cols: dict
                        More Columns to be added for struct data type (name and type)
    :param key_type: glue Schema Object
                        Optional field for map column schema type
    :param input_string: string
                        Glue InputString for map/array type
    :param is_primitive: boolean
                        Flag to determine type of the map/array value primitive or not
    :return: list
            List of columns with name and type
    """
    columns = []
    for col_name, col_type in col_details.items():
        temp = dict()
        temp["name"] = col_name
        if col_type.lower() in "string":
            temp["type"] = glue.Schema.STRING
        elif col_type.lower() in "integer":
            temp["type"] = glue.Schema.INTEGER
        elif col_type.lower() in "bigint":
            temp["type"] = glue.Schema.BIG_INT
        elif col_type.lower() in "float":
            temp["type"] = glue.Schema.FLOAT
        elif col_type.lower() in "double":
            temp["type"] = glue.Schema.DOUBLE
        elif col_type.lower() in "timestamp":
            temp["type"] = glue.Schema.TIMESTAMP
        elif col_type.lower() in "boolean":
            temp["type"] = glue.Schema.BOOLEAN
        elif col_type.lower() in "struct":
            temp["type"] = glue.Schema.struct(
                columns=prepare_glue_table_columns(col_details=struct_cols)
            )
        elif col_type.lower() in "map":
            temp["type"] = glue.Schema.map(
                key_type=key_type, input_string=input_string, is_primitive=is_primitive
            )
        elif col_type.lower() in "array":
            temp["type"] = glue.Schema.array(
                input_string=input_string, is_primitive=is_primitive
            )
        columns.append(temp)

    return columns


def create_glue_python_shell_job(
    construct: Stack,
    id: str,
    command: aws_glue.CfnJob.JobCommandProperty,
    role: str,
    glue_version: str = "4.0",
    name: str,
    default_arguments: dict = {},
    timeout:int=60,
    tags: dict,
):
    job = aws_glue.CfnJob(
        construct,
        id,
        command=command,
        role=role,
        # the properties below are optional
        glue_version=glue_version,
        name=name,
        default_arguments=default_arguments,
        timeout=timeout,
        tags=tags,
    )

    return job


def create_glue_python_etl_job(
    construct: Stack,
    id: str,
    job_name: str,
    script_path: str,
    bucket_obj: object,
    extra_jar_path: str,
    glue_role: iam.Role,
    default_arguments: dict = {},
    spark_ui_enabled: bool = True,
    glue_version: glue.GlueVersion = glue.GlueVersion.V3_0,
    tags: dict = {},
    worker_count: int = 2,
    worker_type: glue.WorkerType = glue.WorkerType.G_1_X,
    timeout: cdk.Duration = cdk.Duration.minutes(60),
) -> glue.Job:
    """Create glue Python etl job.

    Args:
        construct (Stack): scope of the Stack
        id (str): id for glue job
        job_name (str): glue job name
        script_path (str): python script path for glue
        bucket_obj (object): s3 bucket object
        extra_jar_path (str): path for extra jars
        glue_role (iam.Role): role for glue job
        default_arguments (dict): default arguments for glue job
        spark_ui_enabled (bool): flag to enable/disable spark ui
        glue_version (glue.GlueVersion): glue version
        tags (dict): tags configuration
        worker_count (int): glue job worker count
        worker_type (glue.WorkerType): job worker type
        timeout (cdk.Duration): timeout duration

    Returns:
        glue.Job: create glue job object
    """
    job = glue.Job(
        construct,
        f"{job_name}_{id}",
        job_name=job_name,
        spark_ui=glue.SparkUIProps(enabled=spark_ui_enabled),
        executable=glue.JobExecutable.python_etl(
            glue_version=glue_version,
            python_version=glue.PythonVersion.THREE,
            script=glue.Code.from_asset(
                script_path,
            ),
            extra_jars=[
                glue.Code.from_bucket(
                    bucket_obj,
                    extra_jar_path,
                ),
            ],
        ),
        continuous_logging=glue.ContinuousLoggingProps(
            enabled=True,
        ),
        default_arguments=default_arguments,
        role=glue_role,
        tags=tags,
        worker_count=worker_count,
        worker_type=worker_type,
        timeout=timeout,
    )

    return job


def create_glue_job_trigger(
    construct: Stack,
    job: glue.Job,
    trigger_conf: dict,
    timeout: int = 60,
    start_on_creation: bool = True,
    trigger_type: str = "SCHEDULED",
) -> aws_glue.CfnTrigger:
    """Trigger to schedule glue jobs.

    Args:
        construct (Stack): scope of the Stack
        job (glue.Job): created glue job object
        trigger_conf (dict): configuration for the glue job
        timeout (int): job timeout
        start_on_creation (bool): flag to identify job run policy
        trigger_type (str): type of job run

    Returns:
        aws_glue.CfnTrigger: associate the type of trigger for the glue job
    """
    aws_glue.CfnTrigger(
        construct,
        f"{trigger_conf['arguments']['--job_name']}_glue_job_trigger",
        actions=[
            aws_glue.CfnTrigger.ActionProperty(
                arguments=trigger_conf["arguments"],
                job_name=job.job_name,
                timeout=timeout,
            )
        ],
        type=trigger_type,
        # the properties below are optional
        name=f"{trigger_conf['arguments']['--job_name']}_glue_job_trigger",
        schedule=trigger_conf["schedule"],
        start_on_creation=start_on_creation,
    )
