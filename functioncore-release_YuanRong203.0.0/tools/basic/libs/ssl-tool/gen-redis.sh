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

usage()
{
    echo -e "This script is used to generate redis cert and key file"
    echo -e "Usage: bash gen-redis.sh [-p password of CA file] [-c cluster ip of k8s] [-h help]"
    echo -e "-p    password of CA file"
    echo -e "-c    main node ip of k8s cluster"
    echo -e "-h    help"
    echo -e "example: bash gen-redis.sh -h"
    echo -e "example: bash gen-redis.sh -p {your_password} -c {your_k8s_cluster_main_node_ip}"
}

while getopts "p:c:h" opt; do
    case "$opt" in
        p)
            CA_PASSWORD="$OPTARG"
            ;;
        c)
            CLUSTER_IP="$OPTARG"
            ;;
        h)
            usage
            exit 0
            ;;
        *)
            echo "$opt is not an option"
            exit 1
            ;;
    esac
done

if [ -z "$CA_PASSWORD" ]; then
    echo -e "please input ca key password: \c"
    read -rs CA_PASSWORD
    echo ""
fi

if [ -z "$CLUSTER_IP" ]; then
    read -rp "please input k8s cluster ip: " CLUSTER_IP
fi

MODULE_NAME=redis-cluster

bash gen-cert-in-k8s.sh "$MODULE_NAME" "$CA_PASSWORD" "$CLUSTER_IP"

cp ca/chain-ca.pem certs/$MODULE_NAME/ca.pem
mv "certs/${MODULE_NAME}/${MODULE_NAME}.cert.pem" "certs/${MODULE_NAME}/cert.pem"
mv "certs/${MODULE_NAME}/${MODULE_NAME}.key.pem" "certs/${MODULE_NAME}/key.pem"
exit 0