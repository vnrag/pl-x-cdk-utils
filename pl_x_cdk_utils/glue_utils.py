from aws_cdk import (
    aws_glue,
    aws_glue_alpha as glue
)


def create_glue_crawler(construct, name, db_name, role, table_prefix,
                        targets, configuration=None, schedule=None):
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
                    eg: [{"path": "s3://bucket/path_to_file"}]
    :param configuration: string
                          Additional configurations for glue-crawler
    :param schedule: string
                     String value for cron
    :return: object
             Glue-Crawler object
    """
    glue_crawler = aws_glue.CfnCrawler(
        construct, f"profile-for-crawler-{name}",
        database_name=db_name,
        role=role,
        table_prefix=table_prefix,
        targets=targets,
        name=name
    )
    if configuration:
        glue_crawler.configuration = configuration
    if schedule:
        glue_crawler.schedule = aws_glue.CfnCrawler.ScheduleProperty(
            schedule_expression=schedule)

    return glue_crawler


def create_glue_database(construct, name):
    """
    Create Glue database
    :param construct: object
                      Stack Scope
    :param name: string
                Name for the glue database
    :return: object
            Glue database object
    """
    glue_database = glue.Database(
        construct, f"profile-for-glue-db-{name}",
        database_name=name)
    return glue_database


def create_glue_table(construct, database, table_name, bucket,
                      s3_prefix, columns, compressed=True, data_format=None):
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
    :return: object
             Glue table object
    """
    data_format = data_format if data_format else glue.DataFormat.PARQUET
    glue_table = glue.Table(
        construct, f"profile-for-glue-table-{table_name}",
        database=database, table_name=table_name, columns=columns,
        bucket=bucket, s3_prefix=s3_prefix,
        data_format=data_format, compressed=compressed
    )
    return glue_table


def prepare_glue_table_columns(col_details):
    """
    Prepare columns list for the Glue table with column details dict
    :param col_details: dict
                        Columns details as dict with name and type
    :return: list
            List of columns with name and type
    """
    columns = []
    for col_name, col_type in col_details.items():
        temp = dict()
        temp['name'] = col_name
        if col_type.lower() in 'string':
            temp['type'] = glue.Schema.STRING
        elif col_type.lower() in 'integer':
            temp['type'] = glue.Schema.INTEGER

        columns.append(temp)

    return columns

