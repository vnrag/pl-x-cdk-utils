from aws_cdk import (
    aws_kms as kms,
    aws_ssm as ssm,
    aws_iam as iam,
    aws_lambda as _lambda
)
import aws_cdk as core


def get_cdk_codebuild_step(git_source, commands, build_step='Synth',
                           submodules={}, role_policy_statements=[]):
    codebuild_step = core.pipelines.CodeBuildStep(
        build_step, input=git_source,
        additional_inputs=submodules,
        commands=commands,
        role_policy_statements=role_policy_statements
    )
    return codebuild_step


def get_cdk_codepipeline(construct, repo_name, synth_step):
    pipeline = core.pipelines.CodePipeline(
        construct, f"profile-{repo_name}",
        pipeline_name=repo_name,
        cross_account_keys=True,
        self_mutation=True,
        synth=synth_step
    )
    return pipeline
