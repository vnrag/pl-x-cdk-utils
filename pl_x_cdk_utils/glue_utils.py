from aws_cdk import aws_glue, aws_glue_alpha as glue


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
