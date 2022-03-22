def prepare_arg_for_jar_step(
    bucket_ssm: str,
    file_path: str,
) -> list:
    """Prepare execution script args for emr cluster.

    Args:
        bucket_ssm (str): name of the bucket
        file_path (str): file path for the script

    Returns:
        list: jar script format for the emr cluster
    """
    jar_args = ["spark-submit", "--deploy-mode", "cluster"]
    fpath = f"s3://{bucket_ssm}/{file_path}"

    jar_args.append(fpath)

    return jar_args


def prepare_s3_path(
    bucket_ssm: str,
    prefix: str,
) -> str:
    """Prepare s3 path given bucket name and paritions.

    Args:
        bucket_ssm (str): bucket name
        prefix (str): partition path

    Returns:
        str: full s3 path
    """
    path = f"s3://{bucket_ssm}/{prefix}"

    return path
