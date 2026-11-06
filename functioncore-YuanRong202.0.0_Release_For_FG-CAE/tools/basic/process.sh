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
MODULE_DIR="${BASE_DIR}/libs"
RESOURCE_DIR="${BASE_DIR}/sources"
CONFIG_FILE="${BASE_DIR}/config.xml"
UTIL_FUNCTIONS="${BASE_DIR}/utils.sh"
TIME_WAIT=10s

[[ ! -f "${UTIL_FUNCTIONS}" ]] && echo "${UTIL_FUNCTIONS} is not exist" && exit 1
. "${UTIL_FUNCTIONS}"

set +e

# install components
function install_outcluster() {
    cd "${RESOURCE_DIR}"
    if [[ "$1" == "nfs" || "$1" == "all" ]]; then
        bash ./nfs.sh
        [[ $? -ne 0 ]] && log_error "failed to install nfs" && exit 1
    fi
    if [[ "$1" == "ntp" || "$1" == "all" ]]; then
        bash ./ntp.sh
        [[ $? -ne 0 ]] && log_error "failed to install ntp" && exit 1
    fi
}

function install_rancher() {
    log_info "Install rancher ..."
    cd "${MODULE_DIR}/rancher"
    bash rancher.sh
    [[ $? -ne 0 ]] && log_error "failed to install rancher" && exit 1
    if [[ "${ARCH}" =~ "aarch64" ]];then
        # The coredns, flannel, and local-dns of rke cannot run on ARM64.
        kubectl set image daemonset node-local-dns *=k8s.gcr.io/dns/k8s-dns-node-cache:1.17.0 -n kube-system
        kubectl set image deployment coredns *=coredns/coredns:1.6.5 -n kube-system
        kubectl set image daemonset kube-flannel kube-flannel=quay.io/coreos/flannel:v0.13.1-rc2 -n kube-system
    fi
    kubectl scale deployment coredns --replicas=0 -n kube-system
    kubectl scale deployment coredns --replicas=1 -n kube-system
    # using local image
    kubectl patch deployment metrics-server -n kube-system -p '{"spec":{"template":{"spec":{"containers":[{"imagePullPolicy":"IfNotPresent","name":"metrics-server"}]}}}}'
}

function uninstall_all() {
    bash "${BASE_DIR}"/clear.sh
}

function install_mysql() {
    log_info "Install mysql ..."
    mysql_password="$(xmllint --xpath "string(//rancher/mysql_password)" "${CONFIG_FILE}")"
    [[ "${mysql_password}X" == "passwordX" ]] && log_error "please configure ${CONFIG_FILE} mysql_password first!" && exit 1
    cd "${MODULE_DIR}/mysql/${ARCH}"
    if [[ "${ARCH}" =~ "aarch64" ]];then
        helm install --set password="${mysql_password}" mysql ./
    else
        helm install --set auth.rootPassword="${mysql_password}" mysql ./
        kubectl wait --for=condition=Available statefulset/mysql --timeout="${TIME_WAIT}"
    fi
}

function uninstall_mysql() {
    helm uninstall mysql
}

function install_nfs_client() {
    log_info "Install nfs client ..."
    cd "${MODULE_DIR}/nfs-client/${ARCH}"
    helm install nfs-client-provisioner ./ --set nfs.server="${NFS_SERVER_IP}"
    kubectl wait --for=condition=Available deploy/nfs-client-provisioner --timeout="${TIME_WAIT}"
}

function uninstall_nfs_client() {
    helm uninstall nfs-client-provisioner
}

function install_docker_registry() {
    log_info "Install docker registry ..."
    cd "${MODULE_DIR}/"
    helm install docker-registry ./docker-registry/
    kubectl wait --for=condition=Available deploy/docker-registry --timeout="${TIME_WAIT}"
}

function uninstall_docker_registry() {
    helm uninstall docker-registry
}

function install_mysql() {
    log_info "Install mysql ..."
    mysql_password="$(xmllint --xpath "string(//rancher/mysql_password)" "${CONFIG_FILE}")"
    [[ "${mysql_password}X" == "passwordX" ]] && log_error "please configure ${CONFIG_FILE} mysql_password first!" && exit 1
    cd "${MODULE_DIR}/mysql/${ARCH}"
    if [[ "${ARCH}" =~ "aarch64" ]];then
        helm install --set password="${mysql_password}" mysql ./
    else
        helm install --set auth.rootPassword="${mysql_password}" mysql ./
        kubectl wait --for=condition=Available statefulset/mysql --timeout="${TIME_WAIT}"
    fi
}

function uninstall_mysql() {
    helm uninstall mysql
}

function install_ds_core_etcd() {
    log_info "Install runtime etcd ..."
    # shellcheck disable=SC2155
    core_etcd_password="$(xmllint --xpath "string(//rancher/core_etcd_password)" "${CONFIG_FILE}")"
    [[ "${core_etcd_password}X" == "passwordX" ]] && log_error "please configure ${CONFIG_FILE} core_etcd_password first!" && exit 1
    cd "${MODULE_DIR}/ds-core-etcd/${ARCH}"
    helm install --set auth.rbac.rootPassword="${core_etcd_password}" ds-core-etcd ./
    kubectl wait --for=condition=Available StatefulSet/ds-core-etcd --timeout="${TIME_WAIT}"
}

function uninstall_ds_core_etcd() {
    helm uninstall ds-core-etcd
    sleep 5
    pvc=$(kubectl get pvc | grep ds-core-etcd | awk '{print $1}')
    kubectl delete pvc "${pvc}"
}

function install_minio() {
    log_info "Install minio ..."
    minio_accessKey="$(xmllint --xpath "string(//rancher/S3_AK)" "${CONFIG_FILE}")"
    minio_secretKey="$(xmllint --xpath "string(//rancher/S3_SK)" "${CONFIG_FILE}")"
    [[ "${minio_accessKey}X" == "accessKeyX" ]] && log_error "please configure ${CONFIG_FILE} S3_AK first!" && exit 1
    [[ "${minio_secretKey}X" == "secretKeyX" ]] && log_error "please configure ${CONFIG_FILE} S3_SK first!" && exit 1
    cd "${MODULE_DIR}/"
    helm install --set accessKey="${minio_accessKey}" --set secretKey="${minio_secretKey}" minio ./minio/
    kubectl wait --for=condition=Available deploy/minio --timeout="${TIME_WAIT}"
}

function uninstall_minio() {
    helm uninstall minio
}

function install_prometheus() {
    log_info "Install prometheus ..."
    cd "${MODULE_DIR}/"
    [[ $(kubectl get namespace | grep -c monitor) -lt 1 ]] && kubectl create namespace monitor
    helm install prometheus prometheus/ -n monitor
    kubectl wait --for=condition=Available deploy/prometheus-server --timeout="${TIME_WAIT}" -n monitor
}

function uninstall_prometheus() {
    helm uninstall prometheus -n monitor
}

function install_grafana() {
    log_info "Install grafana ..."
    cd "${MODULE_DIR}/"
    prometheusport=$(kubectl get svc -n monitor | grep prometheus-server | awk 'NR==1{print $5}' | awk -F'[:/]' '{print $2}')
    lokiport=$(kubectl get svc -n monitor | grep loki | awk 'NR==1{print $5}' | awk -F'[:/]' '{print $1}')
    sed "s/prometheus-server-ip/${PROMETHEUS_IP}:${prometheusport}/g" grafana/examp.yaml > grafana/values.yaml
    if [[ ${lokiport} != "" ]]; then
      sed -i "s/loki-server-ip/loki:${lokiport}/g" grafana/values.yaml
    fi
    helm install grafana grafana/ -n monitor
    kubectl wait --for=condition=Available deploy/grafana --timeout="${TIME_WAIT}" -n monitor
}

function uninstall_grafana() {
    helm uninstall grafana -n monitor
}

function install_redis_cluster() {
    log_info "Install redis ..."
    redis_password="$(xmllint --xpath "string(//rancher/redis_password)" "${CONFIG_FILE}")"
    [[ "${redis_password}X" == "passwordX" ]] && log_error "please configure ${CONFIG_FILE} redis_password first!" && exit 1
    cd "${MODULE_DIR}/redis/${ARCH}"
    # redis cluster has 1 manager pod, 3 master pods and 3 slave pods
    helm install --set password="${redis_password}" redis-cluster ./ -f values-production.yaml # --debug
    kubectl wait --for=condition=Available StatefulSet/redis-cluster --timeout="${TIME_WAIT}"
}

function uninstall_redis_cluster() {
    helm uninstall redis-cluster
    kubectl get job | grep redis-cluster | awk '{print $1}' | xargs kubectl delete job
    kubectl get pvc | grep redis-cluster | awk '{print $1}' | xargs kubectl delete pvc
    kubectl get pv | grep redis-cluster | awk '{print $1}' | xargs kubectl delete pv
}

function install_keycloak() {
    log_info "Install keycloak ..."
    keycloak_password="$(xmllint --xpath "string(//rancher/keycloak_password)" "${CONFIG_FILE}")"
    [[ "${keycloak_password}X" == "passwordX" ]] && log_error "please configure ${CONFIG_FILE} keycloak_password first!" && exit 1
    cd "${MODULE_DIR}/keycloak"
    if [[ "${ARCH}" =~ "aarch64" ]];then
        echo "Temporarily use x86 keycloak"
    else
        helm install --set password="${keycloak_password}" keycloak ./
    fi
    pod="$(kubectl get pod | grep keycloak | awk '{print $1}')"
    kubectl wait --for=condition=Ready pod/${pod} --timeout="${TIME_WAIT}"
    localip="$(kubectl get pod -o wide | grep Running | grep keycloak | awk '{print $6}')"
    if [ -z "$localip" ]; then
       echo -e "\033[31m  When keycloak is ready, please run install command again to ensure that you can login keycloak console (sh process.sh -o install -m keycloak) \033[0m"
    else
       kubectl exec ${pod} -- ./opt/jboss/keycloak/bin/kcadm.sh config credentials --server http://"${localip}:8080"/auth --realm master --user admin --password ${keycloak_password}
       kubectl exec ${pod} -- ./opt/jboss/keycloak/bin/kcadm.sh update realms/master -s sslRequired=NONE
    fi
}

function uninstall_keycloak() {
    if [[ "${ARCH}" =~ "aarch64" ]];then
        echo "Temporarily use x86 keycloak"
    else
        helm uninstall keycloak
    fi
}

function install_log_plugin() {
    log_info "Install es ..."
    cd "${MODULE_DIR}/log-plugin/${ARCH}"
    helm install es-master ./es/elasticsearch/ -n default -f ./es/es-master.yaml
    helm install es-data ./es/elasticsearch/ -n default -f ./es/es-data.yaml
    helm install es-client ./es/elasticsearch/ -n default -f ./es/es-client.yaml

    log_info "Install kibana ..."
    helm install kibana ./es/kibana -n default -f ./es/es-kibana.yaml
    log_info "Install fluent-bit ..."
    kubectl apply -f ./fluent-bit/fluent-bit-config.yaml
    kubectl apply -f ./fluent-bit/fluent-bit-deployment.yaml
    kubectl apply -f ./fluent-bit/es-index-clean.yaml

    kubectl wait --for=condition=Available StatefulSet/es-aka-master --timeout="${TIME_WAIT}"
    kubectl wait --for=condition=Available StatefulSet/es-aka-client --timeout="${TIME_WAIT}"
    kubectl wait --for=condition=Available StatefulSet/es-aka-data --timeout="${TIME_WAIT}"
    kubectl wait --for=condition=Available deploy/kibana-kibana --timeout="${TIME_WAIT}"
    kubectl wait --for=condition=Available DaemonSet/fluent-bit --timeout="${TIME_WAIT}"
}

function uninstall_log_plugin() {
    kubectl delete configmaps fluent-bit-component-config
    kubectl delete daemonsets.apps fluent-bit
    kubectl delete cronjobs.batch es-index-delete

    helm uninstall kibana
    helm uninstall es-master
    helm uninstall es-data
    helm uninstall es-client
}

function install_new_log_plugin() {
    log_info "Install LPG ..."
    cd "${MODULE_DIR}/new-log-plugin"
    helm install promtail ./promtail/ -n monitor
    helm install loki ./loki/ -n monitor

    kubectl wait --for=condition=Available StatefulSet/loki --timeout="${TIME_WAIT}" -n monitor
    kubectl wait --for=condition=Available DaemonSet/promtail --timeout="${TIME_WAIT}" -n monitor
}

function uninstall_new_log_plugin() {
    helm uninstall loki -n monitor
    helm uninstall promtail -n monitor
}


function install_local_cache_service() {
    log_info "Install local cache service ..."
    cd "${MODULE_DIR}/local-cache-service"
    if [[ "${ARCH}" =~ "aarch64" ]];then
        MINIO_ADDR="$(kubectl get svc | grep minio | awk '{print $3}' | head -n 1)"
        helm install local-cache-service ./ --set params.ADDR="${MINIO_ADDR}:9000"
    else
        helm install local-cache-service ./
    fi
    kubectl wait --for=condition=Available deploy/local-cache-service --timeout="${TIME_WAIT}"
}

function uninstall_local_cache_service() {
    helm uninstall local-cache-service
}

function install_redis_local() {
    log_info "Install local redis ..."
    redis_password="$(xmllint --xpath "string(//rancher/redis_password)" "${CONFIG_FILE}")"
    [[ "${redis_password}X" == "passwordX" ]] && log_error "please configure ${CONFIG_FILE} redis_password first!" && exit 1
    cd "${MODULE_DIR}/redis-local"
    helm install redis-local ./ --set global.redis.password=${redis_password}
    kubectl wait --for=condition=Available StatefulSet/redis-local-master --timeout="${TIME_WAIT}"
}

function uninstall_redis_local() {
    helm uninstall redis-local
    kubectl get pvc | grep redis-local | awk '{print $1}' | xargs kubectl delete pvc
    kubectl get pv | grep redis-local | awk '{print $1}' | xargs kubectl delete pv
}


# ----------------------------------------------------------------------
# funcname: usage
# description:  help of process.sh
# parameters:   void
# return value: void
# ----------------------------------------------------------------------
usage()
{
    echo -e "Usage: bash process.sh [-o install/uninstall] [-m component_name/all] [-t timeout] [-h help]"
    echo -e "Options:"
    echo -e "     -o install/uninstall, install component or uninstall component."
    echo -e "     -m component_name/all, such as rancher, nfs-client, docker-registry, ds-core-etcd, minio, prometheus, grafana,"
    echo -e "                                    redis-cluster, log-plugin, keycloak, local-cache-service, pm-service, quick."
    echo -e "     -t timeout, waiting time of deploying component, default 10s."
    echo -e "     -h usage help"
    echo -e "      "
    echo -e "example:"
    echo -e "   bash process.sh -o install -m all"
    echo -e "   bash process.sh -o uninstall -m all"
    echo -e "   bash process.sh -o install -m redis-cluster -t 30s"
    echo -e "   bash process.sh -o uninstall -m redis-cluster"
    echo -e "   bash process.sh -h"
    echo -e ""
}

function uninstall_module() {
    case $1 in
    "rancher" | "all")
        uninstall_all
        ;;
    "nfs-client")
        uninstall_nfs_client
        ;;
    "docker-registry")
        uninstall_docker_registry
        ;;
    "mysql")
        uninstall_mysql
        ;;
    "ds-core-etcd")
        uninstall_ds_core_etcd
        ;;
    "minio")
        uninstall_minio
        ;;
    "prometheus")
        uninstall_prometheus
        ;;
    "grafana")
        uninstall_grafana
        ;;
    "redis-cluster")
        uninstall_redis_cluster
        ;;
    "keycloak")
        uninstall_keycloak
        ;;
    "log-plugin")
        uninstall_log_plugin
        ;;
    "local-cache-service")
        uninstall_local_cache_service
        ;;
    "new-log-plugin")
        uninstall_new_log_plugin
        ;;
    "redis-local")
        uninstall_redis_local
        ;;
    esac
}

# install modules
function install_module() {
    case $1 in
    "pm-service")
        install_outcluster "all"
        ;;
    "rancher")
        install_rancher
        ;;
    "nfs-client")
        install_nfs_client
        ;;
    "docker-registry")
        install_docker_registry
        ;;
    "mysql")
        install_mysql
        ;;
    "ds-core-etcd")
        install_ds_core_etcd
        ;;
    "minio")
        install_minio
        ;;
    "prometheus")
        install_prometheus
        ;;
    "grafana")
        install_grafana
        ;;
    "redis-cluster")
        install_redis_cluster
        ;;
    "log-plugin")
        install_log_plugin
        ;;
    "keycloak")
        install_keycloak
        ;;
    "local-cache-service")
        install_local_cache_service
        ;;
    "new-log-plugin")
        install_new_log_plugin
        ;;
    "redis-local")
        install_redis_local
        ;;
    "all")
        install_outcluster "all"
        install_rancher
        install_nfs_client
        install_docker_registry
        install_ds_core_etcd
        install_minio
        install_keycloak
        install_prometheus
        install_new_log_plugin
        install_grafana
        install_redis_cluster
        install_log_plugin
        install_local_cache_service
        install_mysql
        ;;
    "quick")
        install_outcluster "all"
        install_rancher
        install_nfs_client
        install_ds_core_etcd
        install_minio
        install_redis_cluster
        ;;
    esac
}

while getopts "o:m:t:h" opt; do
    case $opt in
    o)
        OPTION=$OPTARG
        ;;
    m)
        MODULE=$OPTARG
        ;;
    t)
        TIME_WAIT=$OPTARG
        ;;
    h)
        usage
        exit 0
        ;;
    *)
        log_error "Unknown parameter"
        echo -e ""
        usage
        exit 1
        ;;
    esac
done

[[ ! -d "${MODULE_DIR}" ]] && log_error "${MODULE_DIR} is not dir" && exit 1
[[ ! -d "${RESOURCE_DIR}" ]] && log_error "${RESOURCE_DIR} is not dir" && exit 1
[[ ! -f "${CONFIG_FILE}" ]] && log_error "${CONFIG_FILE} is not exist" && exit 1
[[ "${ARCH}" != "aarch64" && "${ARCH}" != "x86_64" ]] && log_error "Not supported ${ARCH}" && exit 1
[[ "$(which xmllint 2>/dev/null)X" == "X" ]] && log_error "xmllint is not installed, please install libxml2 first" && exit 1
[[ "$(which python 2>/dev/null)X" == "X" ]] && log_error "python is not installed, please install python first" && exit 1
[[ "$(which docker 2>/dev/null)X" == "X" ]] && log_error "docker is not installed, please install docker first" && exit 1
[[ "$(which kubectl 2>/dev/null)X" == "X" ]] && log_error "kubectl is not installed, please install kubectl first" && exit 1
[[ "$(which helm 2>/dev/null)X" == "X" ]] && log_error "helm is not installed, please install helm first" && exit 1

init_config_var "${CONFIG_FILE}"
[[ "${LOCAL_IP}X" == "localipX" ]] && log_error "please configure ${CONFIG_FILE} first!" && exit 1

if [ "${OPTION}" == "install" ]; then
    install_module "${MODULE}"
fi

if [ "${OPTION}" == "uninstall" ]; then
    uninstall_module "${MODULE}"
fi

exit 0
