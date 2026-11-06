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

MODULE_NAME=$1

if [ -z "$2" ] ; then
    unset CA_PASS
    read -p "Enter CA pass: " -s CA_PASS ; echo
else
    CA_PASS=$2
fi

rm -rf certs/"${MODULE_NAME}"
mkdir -p certs/"${MODULE_NAME}"

openssl req -newkey rsa:2048 -nodes -keyout "certs/${MODULE_NAME}/${MODULE_NAME}.key.pem" -days 732 \
    -out "certs/${MODULE_NAME}/${MODULE_NAME}.csr" \
    -subj "/C=CN/ST=GD/L=SZ/O=huawei/OU=${MODULE_NAME}/CN=huawei.com/emailAddress=cloudeye@huawei.com"

echo Sign certificate request with CA
cat etc/signing-ca.conf > "certs/${MODULE_NAME}/${MODULE_NAME}.conf"

CLUSTER_IP=$3
echo "subjectAltName = @alt_names
[ alt_names ]
DNS.1 = ${MODULE_NAME}
DNS.2 = ${MODULE_NAME}.default
DNS.3 = ${MODULE_NAME}.default.svc
DNS.4 = ${MODULE_NAME}.default.svc.cluster
DNS.5 = ${MODULE_NAME}.default.svc.cluster.local
IP.1 = ${CLUSTER_IP}" >> "certs/${MODULE_NAME}/${MODULE_NAME}.conf"

openssl ca \
    -in "certs/${MODULE_NAME}/${MODULE_NAME}.csr" \
    -notext \
    -out "certs/${MODULE_NAME}/${MODULE_NAME}.crt" \
    -config "certs/${MODULE_NAME}/${MODULE_NAME}.conf" \
    -batch \
    -passin pass:"$CA_PASS" \
    -extensions server_ext

cat "certs/${MODULE_NAME}/${MODULE_NAME}.crt" ca/chain-ca.pem  > "certs/${MODULE_NAME}/${MODULE_NAME}.cert.pem"

rm "certs/${MODULE_NAME}/${MODULE_NAME}.conf"
rm "certs/${MODULE_NAME}/${MODULE_NAME}.crt"
rm "certs/${MODULE_NAME}/${MODULE_NAME}.csr"

echo All Done for "${MODULE_NAME}"
