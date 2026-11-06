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

echo "This file will clear all information of clusters and Docker will be left only. Continue to execute this file?"
read -p "yes:y no:n": flag
[[ "x${flag}" != "xy" ]] && exit 1

[[ "$(docker ps -aq)X" != "X" ]] && docker ps -qa | xargs docker rm -f
# avoid container restart by k8s, delete once more
sleep 5
[[ "$(docker ps -aq)X" != "X" ]] && docker ps -qa | xargs docker rm -f
[[ "$(docker volume ls -q)X" != "X" ]] && docker volume ls -q | xargs docker volume rm

# shellcheck disable=SC2016
for m in $(sudo tac /proc/mounts | sudo awk '{print $2}' | sudo grep "/var/lib/kubelet\|/var/lib/rancher"); do
    sudo umount "${m}"
done
for m in $(mount | grep tmpfs | grep '/var/lib/kubelet' | awk '{ print $3 }'); do
    sudo umount "${m}"
done

rm -rf /etc/ceph \
    /etc/cni \
    /etc/kubernetes \
    /opt/cni \
    /opt/rke \
    /run/secrets/kubernetes.io \
    /run/calico \
    /run/flannel \
    /var/lib/calico \
    /var/lib/etcd \
    /var/lib/cni \
    /var/lib/kubelet \
    /var/lib/rancher/rke/log \
    /var/log/containers \
    /var/log/pods \
    /var/run/calico

iptables -L -t nat
iptables -L -t mangle
iptables -F  # flush all chains
iptables -X  # delete all non-default chains
systemctl restart docker # restore docker chains
systemctl restart sshd   # restore ssh chains
iptables -L

exit 0
