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

echo "please input ca key password"
read CA_PASSWORD
echo "please input all etcd nodes"
read  ETCD_NODES
bash clear.sh

bash gen-ca.sh $CA_PASSWORD
bash gen-cert.sh member $CA_PASSWORD $ETCD_NODES
bash gen-cert.sh server $CA_PASSWORD $ETCD_NODES

mkdir -p ssl
cp ca/chain-ca.pem ssl/ca.pem
cp certs/server.cert.pem ssl/cert.pem
cp certs/server.key.pem ssl/key.pem
cp certs/member.cert.pem ssl/member.cert.pem
cp certs/member.key.pem ssl/member.key.pem
