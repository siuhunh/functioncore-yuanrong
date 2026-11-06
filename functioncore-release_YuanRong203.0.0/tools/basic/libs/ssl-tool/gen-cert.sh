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

NODE_NAME=$1

if [ -z "$2" ] ; then
    unset CA_PASS
    read -p "Enter CA pass: " -s CA_PASS ; echo
else
    CA_PASS=$2
fi

ETCD_NODES=$3
if [ "x$ETCD_NODES" != "x" ]; then
    SAN="subjectAltName=IP:"`echo $ETCD_NODES|sed 's/,/,IP:/g'`
else
    SAN=""
fi


openssl req -newkey rsa:2048 -nodes -keyout certs/$NODE_NAME.key.pem -days 732 -out certs/$NODE_NAME.csr -subj "/C=CN/ST=GD/L=SZ/O=huawei/OU=${NODE_NAME}/CN=huawei.com/emailAddress=cloudeye@huawei.com"

echo Sign certificate request with CA
cat etc/signing-ca.conf >  certs/$NODE_NAME.conf
echo $SAN >> certs/$NODE_NAME.conf

openssl ca \
    -in certs/$NODE_NAME.csr \
    -notext \
    -out certs/$NODE_NAME.crt \
    -config certs/$NODE_NAME.conf \
    -batch \
    -passin pass:$CA_PASS \
    -extensions server_ext
#    -extfile certs/$NODE_NAME.ext

cat certs/$NODE_NAME.crt ca/chain-ca.pem  > certs/$NODE_NAME.cert.pem

echo All done for $NODE_NAME
