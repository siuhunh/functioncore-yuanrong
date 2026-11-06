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

function with_backoff() {
    local max_attempts=9
    local timeout=1
    local attempt=0

    while [[ "$attempt" < "$max_attempts" ]]; do
        if "$@"; then
            return 0
        fi
        echo "failure! retrying in after ${timeout}s..." 1>&2
        sleep "$timeout"
        timeout=$((timeout * 2))
        attempt=$((attempt + 1))
    done
    echo "failed to exe command "$@" for the last time!" 1>&2
    return 1
}

function lookup_ip() {
    with_backoff nslookup "$1" | grep Address | grep -v "#53" | awk '{print $2}'
}

filePath="/home/sn/worker-init/has_worker_init"
if [ -f "$filePath" ]; then
  exit 0
fi

if [ "${ENABLE_MINIO}" == "true" ]; then
    minio_ip=$(lookup_ip "minio")
fi
if [ "${ENABLE_LOCAL_CACHE}" == "true" ]; then
    cache_ip=$(lookup_ip "local-cache-service")
fi

iptables -P INPUT ACCEPT
iptables -P FORWARD ACCEPT
iptables -P OUTPUT ACCEPT

for dns_server in `grep nameserver /etc/resolv.conf | awk '{print $2}'`
do
    iptables -A OUTPUT -d "${dns_server}"/32 -p udp -j ACCEPT
    iptables -A OUTPUT -d "${dns_server}"/32 -p tcp  -j ACCEPT
done

if [ -n "${minio_ip}" ]; then
    iptables -A OUTPUT -d "${minio_ip}"/32 -p tcp -m tcp --dport 9000 -j ACCEPT
fi

if [ -n "${cache_ip}" ]; then
    iptables -A OUTPUT -d "${cache_ip}"/32 -p tcp -m tcp --dport 9000 -j ACCEPT
fi

cache_manager_ip=$(lookup_ip "cache-manager.${SYSTEM_NAMESPACE}.svc.cluster.local")
if [ -n "${cache_manager_ip}" ]; then
    iptables -A OUTPUT -d "${cache_manager_ip}"/32 -p tcp -m tcp --dport 9993 -j ACCEPT
fi

# 6831 is jaeger-agent port, for tracing
iptables -A OUTPUT -p tcp -m tcp --dport 6831 -j ACCEPT
iptables -A OUTPUT -p udp -m udp --dport 6831 -j ACCEPT

# 31501 is ds-worker port, for runtime
iptables -A OUTPUT -p tcp -m tcp --dport 31501 -j ACCEPT
iptables -A OUTPUT -p tcp -m tcp --dport 32569 -j ACCEPT
iptables -A OUTPUT -p tcp -m tcp --dport 32570 -j ACCEPT
iptables -A OUTPUT -d "${SVC_CIDR}" -p icmp --icmp-type 8 -j DROP
iptables -A OUTPUT -d "${SVC_CIDR}" -p udp -j DROP
iptables -A OUTPUT -d "${SVC_CIDR}" -p tcp -m tcp --tcp-flags SYN SYN -m state --state NEW -j DROP
iptables -A OUTPUT -d "${SVC_CIDR}" -j ACCEPT

if [ "${BUSINESS_TYPE}" == "CAE" ]; then
    # disabling communication between cae instance and some FG components
    #frontend
    iptables -A OUTPUT -d "${POD_CIDR}" -p tcp -m tcp --tcp-flags SYN SYN -m state --state NEW --dport 8888 -j DROP
    #worker-manager
    iptables -A OUTPUT -d "${POD_CIDR}" -p tcp -m tcp --tcp-flags SYN SYN -m state --state NEW --dport 58866 -j DROP
    #worker-manager
    iptables -A OUTPUT -d "${POD_CIDR}" -p tcp -m tcp --tcp-flags SYN SYN -m state --state NEW --dport 58867 -j DROP
    #state-manager
    iptables -A OUTPUT -d "${POD_CIDR}" -p tcp -m tcp --tcp-flags SYN SYN -m state --state NEW --dport 8087 -j DROP
    #faas-scheduler
    iptables -A OUTPUT -d "${POD_CIDR}" -p tcp -m tcp --tcp-flags SYN SYN -m state --state NEW --dport 8889 -j DROP
    #faas-scheduler
    iptables -A OUTPUT -d "${POD_CIDR}" -p tcp -m tcp --tcp-flags SYN SYN -m state --state NEW --dport 9994 -j DROP
    #autoscaler
    iptables -A OUTPUT -d "${POD_CIDR}" -p tcp -m tcp --tcp-flags SYN SYN -m state --state NEW --dport 58877 -j DROP
else
    # disabling communication between all pods
    iptables -A OUTPUT -d "${POD_CIDR}" -p icmp --icmp-type 8 -j DROP
    iptables -A OUTPUT -d "${POD_CIDR}" -p udp -j DROP
    iptables -A OUTPUT -d "${POD_CIDR}" -p tcp -m tcp --tcp-flags SYN SYN -m state --state NEW -j DROP
fi
iptables -A OUTPUT -d "${POD_CIDR}" -j ACCEPT

arr=$(echo "${HOST_CIDR}" | tr "," "\n")
for value in $arr; do
    iptables -A OUTPUT -d "${value}" -p icmp --icmp-type 8 -j DROP
    iptables -A OUTPUT -d "${value}" -p udp -j DROP
    iptables -A OUTPUT -d "${value}" -p tcp -m tcp --tcp-flags SYN SYN -m state --state NEW -j DROP
    iptables -A OUTPUT -d "${value}" -j ACCEPT
done

iptables-save

exit 0
