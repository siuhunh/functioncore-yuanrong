# Copyright (c) 2023 Huawei Technologies Co., Ltd
#
# This software is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
# http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
import sys
import yaml
from datetime import datetime

def getCronExpression():
    currentDateAndTime = datetime.now()
    year = currentDateAndTime.year
    month = currentDateAndTime.month
    day = currentDateAndTime.day
    express = "*/2 * " + str(day) + " " + str(month) + " * "
    print("getCronExpression is ", express)
    return express
	


def update():
    # 更新 YAML 文件中 apiVersion 数据
    config["apiVersion"] = "batch/v1"
    config["kind"] = "CronJob"
	
    # 更新 YAML 文件中 metadata 数据
    del config["metadata"]["labels"]
    del config["metadata"]["selfLink"]
    del config["metadata"]["uid"]
    del config["metadata"]["resourceVersion"]
    del config["metadata"]["generation"]
    del config["metadata"]["creationTimestamp"]
    del config["metadata"]["annotations"]
    del config["metadata"]["managedFields"]
    del config["metadata"]["name"]

    config["metadata"]["generateName"] = "snapshot-manager-"

    del config["spec"]["replicas"]
    del config["spec"]["selector"]
    del config["spec"]["progressDeadlineSeconds"]
    del config["spec"]["revisionHistoryLimit"]
    del config["spec"]["strategy"]

    config["spec"]["template"]["metadata"]["labels"]['app'] = "snapshot-manager"
    config["spec"]["template"]["spec"]["affinity"]['podAntiAffinity'][
        'preferredDuringSchedulingIgnoredDuringExecution'][0]['podAffinityTerm']['labelSelector']['matchExpressions'][
        0]['values'] = ['snapshot-manager']

    container_ele = config["spec"]["template"]["spec"]["containers"][0]
    container_ele['args'] = ['--is-cluster-mode=true --cluster-type=lease --is-snapshot-mode=true']

    worker_tag = registry + "worker:" + tag
    worker_init_tag = registry + "worker-init:" + tag
    runtime_manager_tag = registry + "runtime-manager:" + tag
    worker_manager_tag = registry + "worker-manager:" + tag

    container_ele["name"] = "snapshot-manager"
    container_ele["image"] = worker_manager_tag
    del container_ele["ports"]

    for value in container_ele["env"]:
        if value['name'] == "FUNCTION_ACTIVE_PORT":
            value['value'] = "58866"
        if value['name'] == "WORKER_INIT_TAG":
            value['value'] = worker_init_tag
        if value['name'] == "WORKER_TAG":
            value['value'] = worker_tag
        if value['name'] == "RUNTIME_MANAGER_TAG":
            value['value'] = runtime_manager_tag

    del container_ele["livenessProbe"]
    del container_ele["readinessProbe"]
    config["spec"]["template"]["spec"]["restartPolicy"] = "Never"
    del config["status"]
	
    # 移动层级
    temp_spec = config["spec"]
    del config["spec"]
    config["spec"] = {}
    config["spec"]["jobTemplate"] = {}
    config["spec"]["jobTemplate"]["spec"] = temp_spec
	
    # 新增schedule相关
    config["spec"]["schedule"] = getCronExpression()
    config["spec"]["concurrencyPolicy"] = "Forbid"
    config["spec"]["successfulJobsHistoryLimit"] = 0
    config["spec"]["failedJobsHistoryLimit"] = 0


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    filename = "worker-manager.yaml"
    snapshotfile = "snapshot-manager.yaml"
    args = sys.argv
    if len(args) < 2:
        print("missing parameters:python generate.py IMAGE_REGISTRY IMAGE_TAG")
        exit(1)
    registry = args[1]
    tag = args[2]
    # 加载 YAML 文件
    with open(filename, "r") as f:
        config = yaml.safe_load(f)
    update()
    # 保存更新后的 YAML 文件
    with open(snapshotfile, "w") as f:
        yaml.dump(config, f, indent=4)
