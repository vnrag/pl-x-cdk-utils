
def get_parallel_step(next_state=None, catch=None, branches={}):
    branches_list = []
    for branch, task in branches.items():
        temp = {
            "StartAt": branch,
            "States": {
                branch: task
            }
        }
        branches_list.append(temp)

    parallel_state = {
        "Type": "Parallel",
        "Branches": branches_list,
        "ResultPath": "$.ParallelResult"
    }
    if next_state:
        parallel_state["Next"] = next_state
    else:
        parallel_state["End"] = True
    if catch:
        parallel_state["Catch"] = catch

    return parallel_state


def get_map_state(iteration_step, next_state=None, catch=None,
                  items_path="$.args", max_con=100, map_result="$.map"):
    map_json = {
        "Type": "Map",
        "ItemsPath": items_path,
        "Iterator": iteration_step,
        "ResultPath": map_result if map_result else None
    }
    if max_con:
        map_json['MaxConcurrency'] = max_con
    if next_state:
        map_json["Next"] = next_state
    else:
        map_json["End"] = True
    if catch:
        map_json["Catch"] = catch

    return map_json


def get_cluster_start_json(next_state=None, input_params={}, config={},
                           x2large=True, scaling=True, weighted_capacity=2,
                           catch_state=None):
    default_cluster_configs = {
        "tags": [{"Key": "owner", "Value": "data"},
                 {"Key": "name", "Value": "cdk-step-function"}],
        "apps": [{"Name": "Hadoop"}, {"Name": "Spark"}],
        "instance": "m5.xlarge",
        "spot_capacity": 4,
        "fleet_config": [
            {
                "InstanceType": "m5.xlarge",
                "BidPrice": "0.08",
                "WeightedCapacity": weighted_capacity
            }
        ],
        "scaling_policy": {
            "ComputeLimits": {
                "MaximumCoreCapacityUnits": 4,
                "MaximumOnDemandCapacityUnits": 5,
                "MinimumCapacityUnits": 6,
                "UnitType": "InstanceFleetUnits"
            }
        }
    }
    default_cluster_configs['fleet_config'].append(
        {
            "InstanceType": "m5.2xlarge",
            "BidPrice": "0.2",
            "WeightedCapacity": 4
        }) if x2large else ''

    tags = config["tags"] if "tags" in config else \
        default_cluster_configs["tags"]
    core_on_demand = config["core_on_demand"] if "core_on_demand" in \
                                                 config else 5
    apps = config["apps"] if "apps" in config else\
        default_cluster_configs["apps"]
    instance = config["instance"] if "instance" in config else\
        default_cluster_configs["instance"]
    spot_capacity = config["spot_capacity"] if "spot_capacity" in config else \
        default_cluster_configs["spot_capacity"]
    task_on_demand = config["task_on_demand"] if "task_on_demand" in config \
        else 0
    default_cluster_configs["scaling_policy"]["ComputeLimits"][
        "MaximumCapacityUnits"] = spot_capacity
    fleet_config = config["fleet_config"] if "fleet_config" in config else \
        default_cluster_configs["fleet_config"]
    scaling_policy = config["scaling_policy"] if "scaling_policy" in config\
        else default_cluster_configs["scaling_policy"]
    step_concurrency = config["step_concurrency"] if "step_concurrency" \
                                                     in config else 10
    emr_version = config['emr_version'] if 'emr_version' in config else \
        "emr-6.2.0"
    ec2_subnet = config['ec2_subnet']
    emr_slave_security = config['emr_slave_security']
    emr_master_security = config['emr_master_security']
    emr_service_access = config['emr_service_access'] if \
        'emr_service_access' in config else None

    master_instance_config = config['master_instance_config'] if \
        'master_instance_config' in config else [{"InstanceType": instance}]
    core_instance_config = config['core_instance_config'] if \
        'core_instance_config' in config else [{"InstanceType": instance}]
    core_on_demand_key = "TargetOnDemandCapacity.$" if \
        "TargetOnDemandCapacity.$" in config else "TargetOnDemandCapacity"
    core_on_demand_val = config["TargetOnDemandCapacity.$"] if \
        "TargetOnDemandCapacity.$" in config else core_on_demand
    instance_type_config_key = "InstanceTypeConfigs.$" if \
        "InstanceTypeConfigs.$" in config else "InstanceTypeConfigs"
    instance_type_config_val = config["InstanceTypeConfigs.$"] if \
        "InstanceTypeConfigs.$" in config else fleet_config
    task_spot_key = "TargetSpotCapacity.$" if \
        "task_spot_key" in config else "TargetSpotCapacity"
    task_spot_val = config["task_spot_key"] if \
        "task_spot_key" in config else spot_capacity
    task_on_demand_key = "TargetOnDemandCapacity.$" if \
        "task_on_demand_key" in config else "TargetOnDemandCapacity"
    task_on_demand_val = config["task_on_demand_key"] if \
        "task_on_demand_key" in config else task_on_demand
    cluster_json = {
        "Type": "Task",
        "Resource": "arn:aws:states:::elasticmapreduce:createCluster.sync",
        "Parameters": {
                "ReleaseLabel": emr_version,
                "StepConcurrencyLevel": step_concurrency,
                "Tags": tags,
                "Applications": apps,
                "Instances": {
                    "InstanceFleets": [
                        {
                            "InstanceFleetType": "MASTER",
                            "Name": "MASTER_NODE",
                            "TargetOnDemandCapacity": 1,
                            "InstanceTypeConfigs": master_instance_config
                        },
                        {
                            "InstanceFleetType": "CORE",
                            "Name": "CORE_NODE",
                            core_on_demand_key: core_on_demand_val,
                            "InstanceTypeConfigs": core_instance_config
                        },
                        {
                            "InstanceFleetType": "TASK",
                            "Name": "TASK_NODES",
                            task_spot_key: task_spot_val,
                            task_on_demand_key: task_on_demand_val,
                            "LaunchSpecifications": {
                                "SpotSpecification": {
                                    "TimeoutDurationMinutes": 180,
                                    "TimeoutAction": "TERMINATE_CLUSTER"
                                }
                            },
                            instance_type_config_key: instance_type_config_val,
                        }
                    ],
                    "KeepJobFlowAliveWhenNoSteps": True,
                    "TerminationProtected": False,
                    "Ec2SubnetId": ec2_subnet,
                    "EmrManagedSlaveSecurityGroup": emr_slave_security,
                    "EmrManagedMasterSecurityGroup": emr_master_security
                },
            "BootstrapActions": [
                    {
                        "Name": "Install external libraries",
                        "ScriptBootstrapAction": {}
                    }
            ],
            "JobFlowRole": "EMR_EC2_DefaultRole",
            "ServiceRole": "EMR_DefaultRole",
            "EbsRootVolumeSize": 10,
            "ScaleDownBehavior": "TERMINATE_AT_TASK_COMPLETION",
            "VisibleToAllUsers": True
        },
        "ResultPath": "$.cluster"
    }
    if scaling:
        cluster_json['Parameters']['ManagedScalingPolicy'] = scaling_policy
    if emr_service_access:
        cluster_json['Parameters']['Instances'][
            'ServiceAccessSecurityGroup'] = emr_service_access
    if next_state:
        cluster_json['Next'] = next_state
    else:
        cluster_json['End'] = True
    if input_params:
        cluster_json["InputPath"] = "$"
        cluster_json["Parameters"]["Name"] = input_params["cluster_name"]
        cluster_json["Parameters"]["LogUri"] = input_params["log_uri"]
        cluster_json["Parameters"]["BootstrapActions"][0][
            "ScriptBootstrapAction"]["Path"] = \
            input_params["bootstrap_script"]
    else:
        cluster_json["InputPath"] = "$.emr_cluster_params"
        cluster_json["Parameters"]["Name.$"] = "$.cluster_name"
        cluster_json["Parameters"]["LogUri.$"] = "$.log_uri"
        cluster_json["Parameters"]["BootstrapActions"][0][
            "ScriptBootstrapAction"]["Path.$"] = "$.bootstrap_script"
    if catch_state:
        cluster_json['Catch'] = catch_state

    return cluster_json


def get_cluster_terminate_json(next_state=None, cluster_id_path=None,
                               catch_state=None):
    cluster_id_path = cluster_id_path if cluster_id_path else \
        "$.cluster.ClusterId"
    terminate_json = {
        "Type": "Task",
        "InputPath": "$",
        "Resource": "arn:aws:states:::elasticmapreduce:terminateCluster.sync",
        "Parameters": {
            "ClusterId.$": cluster_id_path
        },
        "ResultPath": "$.cluster_terminate"
    }
    if next_state:
        terminate_json["Next"] = next_state
    else:
        terminate_json["End"] = True
    if catch_state:
        terminate_json['Catch'] = catch_state

    return terminate_json


def get_json_for_jar_step(next_state=None, jar_config={}, arg_path_val=True,
                          name_path_val=True):
    arg_path = jar_config["arg_path"] if "arg_path" in jar_config else "$.arg"
    name_path = jar_config["name_path"] if "name_path" in jar_config else \
        f"{arg_path}[3]"
    result_path = jar_config["result_path"] if "result_path" in jar_config \
        else "$.result"
    next_state = next_state if next_state else False
    catch = jar_config["catch"] if "catch" in jar_config else False
    cluster_id_path = jar_config["cluster_id"] if "cluster_id" in jar_config \
        else "$.cluster.ClusterId"
    jar_step_json = {
        "Type": "Task",
        "Resource": "arn:aws:states:::elasticmapreduce:addStep.sync",
        "Parameters": {
            "Step": {
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar"
                }
            },
            "ClusterId.$": cluster_id_path
        },
        "ResultPath": result_path
    }
    if arg_path_val:
        jar_step_json["Parameters"]["Step"]["HadoopJarStep"]["Args.$"] = \
            arg_path
    else:
        jar_step_json["Parameters"]["Step"]["HadoopJarStep"]["Args"] = \
            jar_config["arg_value"]
    if next_state:
        jar_step_json["Next"] = next_state
    else:
        jar_step_json["End"] = True
    if name_path_val:
        jar_step_json["Parameters"]["Step"]["Name.$"] = name_path
    else:
        jar_step_json["Parameters"]["Step"]["Name"] = jar_config["name"] \
            if "name" in jar_config else "JarStep"
    if catch:
        jar_step_json["Catch"] = catch

    return jar_step_json


def get_json_for_lambda(arn, next_state=None, catch=None,
                        payload={"clusterId.$": "$.cluster.ClusterId"}):
    lambda_json = {
        "Type": "Task",
        "Resource": "arn:aws:states:::lambda:invoke",
        "ResultPath": "$.resp",
        "Parameters": {
            "FunctionName": arn,
            "Payload": payload
        }
    }

    if next_state:
        lambda_json["Next"] = next_state
    else:
        lambda_json["End"] = True
    if catch:
        lambda_json["Catch"] = catch

    return lambda_json


def get_json_for_step_function(arn, input_path=None, input=None,
                               next_state=None, catch=None, result_path=None,
                               output_path="$", result_selector=False,
                               name_path=None, name=None):
    sfn_json = {
        "Type": "Task",
        "Resource": "arn:aws:states:::states:startExecution.sync",
        "Parameters": {
            "StateMachineArn": arn
        }
    }
    if input:
        sfn_json["Parameters"]["Input"] = input if input else ''
    else:
        sfn_json["Parameters"]["Input.$"] = input_path if input_path else "$"

    if next_state:
        sfn_json["Next"] = next_state
    else:
        sfn_json["End"] = True
    if catch:
        sfn_json["Catch"] = catch
    if result_selector:
        sfn_json["ResultSelector"] = result_selector
    if name:
        sfn_json["Parameters"]["Name"] = name
    if name_path:
        sfn_json["Parameters"]["Name.$"] = name_path

    sfn_json["ResultPath"] = result_path if result_path else None
    sfn_json["OutputPath"] = output_path

    return sfn_json


def get_json_for_choice_state(next_state, default, variable=None,
                              boolean_val=False):
    choice_json = {
        "Type": "Choice",
        "Choices": [
            {
                "Variable": variable if variable else "$.Status.success",
                "BooleanEquals": boolean_val,
                "Next": next_state
            }
        ],
        "Default": default
    }

    return choice_json


def get_json_for_flag(next_state, result_path="$.Status", result_key="success",
                      boolean_val=False):
    flag_json = {
        "Type": "Pass",
        "Result": {
            result_key: boolean_val
        },
        "ResultPath": result_path,
        "Next": next_state
    }
    return flag_json


def get_json_for_sns(topic_arn, message=None, next_state=None):
    message = message if message else "States.StringToJson($.error.Cause)"
    sns_json = {
        "Type": "Task",
        "Resource": "arn:aws:states:::sns:publish",
        "Parameters": {
            "Message.$": message,
            "TopicArn": topic_arn
        },
        "ResultPath": "$.step_failure",
    }
    if next_state:
        sns_json["Next"] = next_state
    else:
        sns_json["End"] = True

    return sns_json


def get_json_for_succeed_state():
    succeed_json = {
        "Type": "Succeed"
    }

    return succeed_json


def get_json_for_failed_state(error=None, cause=None):
    failed_json = {
        "Type": "Fail",
        "Error": error if error else "Error Occurred",
        "Cause": cause if cause else "One of the Step Failed."
    }
    return failed_json


def get_catch_state(next_state):
    catch_json = [{
        "ErrorEquals": ["States.ALL"],
        "ResultPath": "$.error",
        "Next": next_state
    }]
    return catch_json
