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

cat /etc/profile
protoc
set -e

BASE_DIR=$(cd "$(dirname "$0")"; pwd)
PKG_DIR=${BASE_DIR}/../../
GRPC_DIR=${PKG_DIR}/common/grpc/pb

if [ -d $GRPC_DIR ];then
    rm $GRPC_DIR -rf
fi
mkdir -p $GRPC_DIR
protoc --proto_path="${PKG_DIR}/common/protobuf" --go_out="$GRPC_DIR" --go-grpc_out="$GRPC_DIR" "${PKG_DIR}/common/protobuf/scheduler.proto"
exit 0
