from aws_cdk import pipelines


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
