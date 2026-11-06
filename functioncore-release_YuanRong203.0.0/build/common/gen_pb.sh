#!/bin/bash
# Copyright (c) 2024 Huawei Technologies Co., Ltd
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

BASE_DIR=$(cd "$(dirname "$0")"; pwd)
COMMON_DIR="${BASE_DIR}"/../../pkg/common/
if ! protoc --proto_path="${COMMON_DIR}/stream/pb" --go_out="${COMMON_DIR}/stream/pb" --go_opt=paths=source_relative --go-grpc_out="${COMMON_DIR}/stream/pb" --go-grpc_opt=paths=source_relative "${COMMON_DIR}/stream/pb/stream.proto"; then
  echo "protoc stream.proto to golang file failed!!!"
  exit 1
fi

