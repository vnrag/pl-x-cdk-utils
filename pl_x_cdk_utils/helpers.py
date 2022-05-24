import os


def prepare_arg_for_jar_step(
    bucket_name: str,
    file_path: str,
    module_jars: list = list(),
    additional_arg: str = "",
) -> list:
    """Prepare execution script args for emr cluster.

    Args:
        bucket_name (str): name of the bucket
        file_path (str): file path for the script
        module_jars (list): list of jars for external modules , e.g. deequ
        additional_arg (str): arguements to the emr script

    Returns:
        list: jar script format for the emr cluster
    """
    script_path = f"s3://{bucket_name}/{file_path}"

    jar_args = [
        "spark-submit",
        "--deploy-mode",
        "cluster",
    ]

    if module_jars:
        jar_args.extend(module_jars)

    jar_args.append(script_path)

    if additional_arg:
        jar_args.append(additional_arg)

    return jar_args


def prepare_s3_path(
    bucket_name: str,
    prefix: str,
) -> str:
    """Prepare s3 path given bucket name and paritions.

    Args:
        bucket_name (str): bucket name
        prefix (str): partition path

    Returns:
        str: full s3 path
    """
    path = os.path.join("s3://", bucket_name, prefix)

    return path


def check_policy_statement_syntax(policy_statements):
    """
    Checks the policy statement syntax
    :param policy_statements: dict
    :return:
    """
    working_statements = []
    mal_statements = []
    current_sids = []
    valid_principal = True
    for statement in policy_statements["Statement"]:
        if type(statement["Principal"]["AWS"]) is str:
            if "arn:aws:" not in statement["Principal"]["AWS"]:
                valid_principal = False
        elif type(statement["Principal"]["AWS"]) is list:
            for principal in statement:
                if "arn:aws:" not in principal:
                    valid_principal = False
        if valid_principal:
            working_statements.append(statement)
        else:
            mal_statements.append(statement)
        current_sids.append(statement["Sid"])
    return valid_principal, working_statements, mal_statements, current_sids
