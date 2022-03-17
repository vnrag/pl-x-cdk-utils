from aws_cdk import (
    pipelines,
    Environment
)


def add_manual_approve_step(name):
    """
    Add manual step in the stage
    :param name: string
                 Name of the Step
    :return: object
             ManualApprovalStep object
    """
    return pipelines.ManualApprovalStep(f"Approve {name}")


def get_codepipeline(construct, repo_name, synth_step):
    """
    Codepipeline constrict for given repo and synth step
    :param construct: object
                      Stack Scope
    :param repo_name: string
                      Name of the repo
    :param synth_step: object
                       Codebuild or ShellStep or any other..
    :return: object
             Pipeline object
    """
    pipeline = pipelines.CodePipeline(
        construct, f"profile-{repo_name}",
        pipeline_name=repo_name,
        cross_account_keys=True,
        self_mutation=True,
        synth=synth_step
    )
    return pipeline


def get_github_connection(repo, branch, codestar_arn, owner='vnrag'):
    """
    Get github connection with given codestar arn
    :param repo: string
                 Name for the github repository
    :param branch: string
                   Branch of the github repository for pipeline
    :param codestar_arn: string
                         Codestar arn with the connection to github
    :param owner: string
              Owner for the github repository
    :return: object
             Github connection object
    """
    github_connection = pipelines.CodePipelineSource.connection(
        f"{owner}/{repo}", branch,
        connection_arn=codestar_arn
    )
    return github_connection


def get_codebuild_step(git_source, commands, build_step='Synth',
                       submodules={}, role_policy_statements=[]):
    """
    Codebuild step needed for codepipeline
    :param git_source: object
                      Github connection
    :param commands: list
                     List of execution commands
    :param build_step: string
                       Name of the step
    :param submodules: dict
                       Github connection objects for the submodules
    :param role_policy_statements: object
                                   IAM role policies
    :return: object
             Codebuild step
    """
    codebuild_step = pipelines.CodeBuildStep(
        build_step, input=git_source,
        additional_inputs=submodules,
        commands=commands,
        role_policy_statements=role_policy_statements
    )
    return codebuild_step


def add_pipeline_stage(pipeline, stage, scope, account_details,
                       pre_stage=None, post_stage=None):
    """
    Add stage on the pipeline object
    :param pipeline: object
                     Pipeline object
    :param stage: object
                  Pipeline stage object
    :param scope: object
                  Construct for the stack
    :param account_details: dict
                            Environment details
    :param pre_stage: object
                      Steps before the stage execution
    :param post_stage: object
                       Steps after the stage execution
    :return: object
             Pipeline object with stage
    """
    stage = pipeline.add_stage(stage(
        scope,
        account_details['name'],
        env=Environment(
            account=account_details['id'],
            region=account_details['region']
        ),
    ),
        pre=pipelines.Step.sequence(account_details['approve'])
    )
    if pre_stage:
        stage.add_pre(pre_stage)
    if post_stage:
        stage.add_post(post_stage)
