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

read -e -p "please input ca key password: " -s CA_PASSWORD
echo
read -e -p "please input k8s cluster ip: " CLUSTER_IP
echo

MODULE_NAME=keycloak

bash gen-cert-in-k8s.sh $MODULE_NAME $CA_PASSWORD $CLUSTER_IP

cp ca/chain-ca.pem certs/$MODULE_NAME/ca.pem
