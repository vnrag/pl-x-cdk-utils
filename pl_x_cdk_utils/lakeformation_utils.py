from aws_cdk import (
    aws_lakeformation,
    aws_iam,
    aws_s3,
    aws_ssm,
)


def set_lakeformation_permission_db(
    construct,
    database_name,
    role_name,
    permissions=["ALL"],
    permissions_with_grant_option=["ALL"],
):
    role = aws_iam.Role.from_role_name(
        construct,
        f"LakeFormationDBRole{role_name}{database_name}",
        role_name=role_name,
    )

    return aws_lakeformation.CfnPermissions(
        construct,
        f"LakeFormationPermissionsDB{role_name}{database_name}",
        data_lake_principal=aws_lakeformation.CfnPermissions.
        DataLakePrincipalProperty(
            data_lake_principal_identifier=role.role_arn
        ),
        resource=aws_lakeformation.CfnPermissions.ResourceProperty(
            database_resource=aws_lakeformation.CfnPermissions.
            DatabaseResourceProperty(
                name=database_name,
            ),
        ),
        permissions=permissions,
        permissions_with_grant_option=permissions_with_grant_option,
    )


def set_lakeformation_permission_table(
    construct,
    database_name,
    role_name,
    table_wildcard={},
    permissions=["ALL"],
    permissions_with_grant_option=["ALL"],
):
    role = aws_iam.Role.from_role_name(
        construct,
        f"LakeFormationTableRole{role_name}{database_name}",
        role_name=role_name,
    )

    return aws_lakeformation.CfnPermissions(
        construct,
        f"LakeFormationPermissionsTableRole{role_name}{database_name}",
        data_lake_principal=aws_lakeformation.CfnPermissions.
        DataLakePrincipalProperty(
            data_lake_principal_identifier=role.role_arn
        ),
        resource=aws_lakeformation.CfnPermissions.ResourceProperty(
            table_resource=aws_lakeformation.CfnPermissions.
            TableResourceProperty(
                database_name=database_name,
                table_wildcard=table_wildcard,
            ),
        ),
        permissions=permissions,
        permissions_with_grant_option=permissions_with_grant_option,
    )


def set_lakeformation_permission_location(
    construct,
    bucket_name_ssm,
    role_name,
    permissions=["DATA_LOCATION_ACCESS"],
    permissions_with_grant_option=["DATA_LOCATION_ACCESS"],
):
    role = aws_iam.Role.from_role_name(
        construct,
        f"LakeFormationLocationRole{role_name}{bucket_name_ssm}",
        role_name=role_name,
    )

    bucket_name = aws_ssm.StringParameter.value_for_string_parameter(
        construct,
        parameter_name=bucket_name_ssm,
    )

    s3_resource = aws_s3.Bucket.from_bucket_attributes(
        construct,
        f"BucketForLakeFormation{role_name}{bucket_name_ssm}",
        bucket_name=bucket_name,
    )

    return aws_lakeformation.CfnPermissions(
        construct,
        f"LakeFormationPermissionsLocation{role_name}{s3_resource.bucket_name}",
        data_lake_principal=aws_lakeformation.CfnPermissions.
        DataLakePrincipalProperty(
            data_lake_principal_identifier=role.role_arn
        ),
        resource=aws_lakeformation.CfnPermissions.ResourceProperty(
            data_location_resource=aws_lakeformation.CfnPermissions.
            DataLocationResourceProperty(
                s3_resource=s3_resource.bucket_arn
            )
        ),
        permissions=permissions,
        permissions_with_grant_option=permissions_with_grant_option,
    )
