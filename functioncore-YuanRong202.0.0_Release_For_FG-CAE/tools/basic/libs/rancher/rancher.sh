#!/bin/bash
# Copyright (c) 2021 Huawei Technologies Co., Ltd
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

set -e

ARCH="$(echo "$(uname -m)")"
BASE_DIR="$(dirname "$(readlink -f "$0")")"
TOP_DIR="${BASE_DIR}/../../"
CONFIG_FILE="${TOP_DIR}/config.xml"
UTIL_FUNCTIONS="${TOP_DIR}/utils.sh"
WAIT_TIME=600
[[ ! -f "${CONFIG_FILE}" ]] && log_error "${CONFIG_FILE} is not exist" && exit 1
[[ ! -f "${UTIL_FUNCTIONS}" ]] && log_error "${UTIL_FUNCTIONS} is not exist" && exit 1

. "${UTIL_FUNCTIONS}"
init_config_var "${CONFIG_FILE}"

# Rancher 2.5.3 cannot run properly on the ARM64.
if [[ "${ARCH}" =~ "aarch64" ]];then
    RANCHER_VERSION="v2.4.3"
else
    RANCHER_VERSION="v2.5.3"
fi

cd "${BASE_DIR}"
set +e

log_info "start to init rancher ..."
[[ $(docker ps | grep -c rancher) -ge 1 ]] && log_error "rancher already running ..." && exit 1
docker run -d --restart=unless-stopped -p "${RANCHER_HTTP_PORT}":80 -p "${RANCHER_HTTPS_PORT}":443 --privileged rancher/rancher:${RANCHER_VERSION}
[[ $? -ne 0 ]] && log_error "can't start rancher server, please check the network" && exit 1
function is_rancher_ready() {
    urltoken=$(curl -s "https://${RANCHER_ADDR}/v3-public/localProviders/local?action=login" -H 'content-type: application/json' \
        --data-binary '{"username":"admin","password":"admin"}' --insecure \
        | grep -c token)
    [[ ${urltoken} -ge 1 ]] && return 0
    return 1
}
wait_func_return_true "is_rancher_ready" "${WAIT_TIME}"
[[ $? -ne 0 ]] && log_error "rancher is not ready after ${WAIT_TIME}, please check the config.xml or network" && exit 1

# get urltoken
urltoken1=$(curl -s "https://${RANCHER_ADDR}/v3-public/localProviders/local?action=login" -H 'content-type: application/json' \
    --data-binary '{"username":"admin","password":"admin"}' --insecure \
    | python -c "import json; import sys; obj=json.load(sys.stdin); print(obj['token'])")
# set password
curl -s "https://${RANCHER_ADDR}/v3/users?action=changepassword" -H 'content-type: application/json' -H "Authorization: Bearer ${urltoken1}" \
    --data-binary "{\"currentPassword\":\"admin\",\"newPassword\":\"${RANCHER_PASSWD}\"}" --insecure
# get new urltoken
urltoken2=$(curl -s "https://${RANCHER_ADDR}/v3-public/localProviders/local?action=login" -H 'content-type: application/json' \
    --data-binary "{\"username\":\"admin\",\"password\":\"${RANCHER_PASSWD}\"}" --insecure \
    | python -c "import json; import sys; obj=json.load(sys.stdin); print(obj['token'])")
urltoken3=$(curl -s "https://${RANCHER_ADDR}/v3/token" -H 'content-type: application/json' -H "Authorization: Bearer ${urltoken2}" \
    --data-binary '{"type":"token","description":"automation","name":""}' --insecure \
    | python -c "import json; import sys; obj=json.load(sys.stdin); print(obj['token'])")
# set url
curl -s "https://${RANCHER_ADDR}/v3/settings/server-url" -H 'content-type: application/json' -H "Authorization: Bearer ${urltoken3}" -X PUT \
    --data-binary '{"name":"server-url","value":"https://'"${RANCHER_ADDR}"'"}' --insecure
log_succ "succeed to init rancher with token:${urltoken3}"

# get cluster id and add master node
# Number of processes in the pod: "pod-max-pids": 2048 (1024 + 1024 (reserved for yuanrong component))
curl -s "https://${RANCHER_ADDR}/v3/cluster" -H 'content-type: application/json' -H "Authorization: Bearer ${urltoken3}" \
    -d@cluster_${ARCH}.json --insecure > ./faask8s.json
clusterid=$(python -c "import json; import sys; obj=json.load(sys.stdin); print(obj['id'])" < faask8s.json)
[[ "${clusterid}X" == "X" ]] && log_error "get rancher cluster id failed" && exit 1
curl -s "https://${RANCHER_ADDR}/v3/clusters/${clusterid}/projects" -H 'content-type: application/json' -H "Authorization: Bearer ${urltoken3}" -X GET --insecure > projectresponse.json
curl -s "https://${RANCHER_ADDR}/v3/clusterregistrationtoken" -H 'content-type: application/json' -H "Authorization: Bearer ${urltoken3}" \
    --data-binary '{"type":"clusterRegistrationToken","clusterId":"'"${clusterid}"'"}' --insecure > clustertoken.json
nodecommand=$(python -c "import json; import sys; obj=json.load(sys.stdin); print(obj['nodeCommand'])" < clustertoken.json)
${nodecommand} --etcd --controlplane --worker --node-name master-server

# wait all containers ready
wait_func_return_true "is_docker_number 15" "${WAIT_TIME}"
[[ $? -ne 0 ]] && log_error "rancher agent is not started after ${WAIT_TIME}s, please check in rancher web"
curl -s "https://${RANCHER_ADDR}/v3/catalogs?action=refresh" -H 'content-type: application/json' -H "Authorization: Bearer ${urltoken3}" --insecure > rancher_refresh.json

function is_kube_config_generated() {
    curl -X POST -s "https://${RANCHER_ADDR}/v3/clusters/${clusterid}?action=generateKubeconfig" -H 'content-type: application/json' -H "Authorization: Bearer ${urltoken3}" --insecure \
     | python -c "import json; import sys; obj=json.load(sys.stdin); print (obj['config'])" > config
    [[ $(wc -l < config) -le 30 ]] && return 1
    return 0
}
wait_func_return_true "is_kube_config_generated" "${WAIT_TIME}"
[[ $? -ne 0 ]] && log_error "kubectl config is not ready after ${WAIT_TIME}s, please check in rancher web" && exit 1
[[ ! -d ~/.kube ]] && mkdir -p ~/.kube
cp -f config ~/.kube/

function is_node_ready() {
    [[ $(kubectl get node 2>/dev/null | grep -c Ready) -lt 1 ]] && return 1
    return 0
}
wait_func_return_true "is_node_ready" "${WAIT_TIME}"
[[ $? -ne 0 ]] && log_error "master node is not ready for ${WAIT_TIME}s, please check in rancher web"

kubectl patch deployment coredns -n kube-system --patch '{"spec": {"template": {"spec": {"containers": [{"name": "coredns","resources":{"limits":{"cpu":"1","memory":"1Gi"},"requests":{"cpu":"1","memory":"1Gi"}}}]}}}}'
log_succ "finished to install rancher: addr=https://${RANCHER_ADDR}"

exit 0
