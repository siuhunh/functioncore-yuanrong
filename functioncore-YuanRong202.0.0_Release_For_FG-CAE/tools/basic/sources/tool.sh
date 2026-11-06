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

ARCH="$(echo "$(uname -m)")"
if [[ "${ARCH}" =~ "aarch64" ]];then
    MACHINE="arm64"
    KUBERNETES_VERSION="v1.17.5"
else
    MACHINE="amd64"
    KUBERNETES_VERSION="v1.19.4"
fi

curl -LO https://storage.googleapis.com/kubernetes-release/release/${KUBERNETES_VERSION}/bin/linux/${MACHINE}/kubelet
curl -LO https://storage.googleapis.com/kubernetes-release/release/${KUBERNETES_VERSION}/bin/linux/${MACHINE}/kubectl
curl -LO https://storage.googleapis.com/kubernetes-release/release/${KUBERNETES_VERSION}/bin/linux/${MACHINE}/kubeadm
curl -LO ftp://100.115.104.108/release/SN/helm/${MACHINE}/helm -u root

chmod +x kubelet && mv kubelet /usr/local/bin/
chmod +x kubectl && mv kubectl /usr/local/bin/
chmod +x kubeadm && mv kubeadm /usr/local/bin/
chmod +x helm && mv helm /usr/local/bin/