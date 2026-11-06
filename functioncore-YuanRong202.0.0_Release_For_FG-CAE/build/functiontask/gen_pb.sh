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

GO_OUT="--go_out=../functiontask/task"

BASE_DIR=$(cd "$(dirname "$0")"; pwd)
PROJECT_DIR="${BASE_DIR}"/../../
cd "${PROJECT_DIR}"pkg/common/
protos=$(find . -iname "*.proto" | grep -v "stream.proto")

function gen_code() {
  case "$1" in
  "task")
    gen_code "bus"
    for f in ${protos[*]}; do
      echo -e "TASK: Processing ${f}"
      protoc -I=protobuf/ "${GO_OUT}" "${f}"
    done
    echo "TASK: Replacing import path..."
    sed -i 's/"api/"functioncore\/pkg\/functiontask\/task\/api/g' ../functiontask/task/adaptor/adaptor.pb.go
    [[ -d "${PROJECT_DIR}"pkg/functiontask/task ]] && rm -f "${PROJECT_DIR}"pkg/functiontask/task/*.pb.go
    echo "TASK: Done!"
    ;;
  "bus")
    cd "${PROJECT_DIR}"pkg/functionbus/script
    if ! sh gen_pb.sh; then
      echo "bus generates pb failed"
      exit 1
    fi
    echo "BUS: Done!"
    cd "${PROJECT_DIR}"pkg/common/
    ;;
  esac
}

if [ $# -eq 1 ]; then
  gen_code "$1"
else
  gen_code "all"
fi

exit 0
