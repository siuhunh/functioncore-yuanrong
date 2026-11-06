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

CUR_DIR=$(dirname "$(readlink -f "$0")")
ROOT_PATH=${CUR_DIR}/../..
OUTPUT_PATH=${CUR_DIR}/output

# prepare test environment:
# 1.generate proto file
# 2.create output path
prepare_environment()
{
    cd "${ROOT_PATH}"/build/functiontask
    bash gen_pb.sh task

    rm -rf "${OUTPUT_PATH}"
    mkdir -p "${OUTPUT_PATH}"
}

# run go test and report
run_gocover_report()
{
    cd "${ROOT_PATH}"/pkg/functiontask/task
    go test -v -gcflags=all=-l -coverprofile="${OUTPUT_PATH}"/task.cover -coverpkg=functioncore/pkg/functiontask/...,functioncore/test/functiontask/test/.... ./...

    cd "${ROOT_PATH}"/test/functiontask/test
    go test -v -gcflags=all=-l -coverprofile="${OUTPUT_PATH}"/test.cover -coverpkg=functioncore/pkg/functiontask/..., functioncore/test/functiontask/test/... ./...

    cd "${ROOT_PATH}"/pkg/functiontask/taskimpl
    go test -v -gcflags=all=-l -coverprofile="${OUTPUT_PATH}"/taskimpl.cover -coverpkg=functioncore/pkg/functiontask/..., functioncore/test/functiontask/test/... ./...

    cd "${OUTPUT_PATH}"
    echo "mode: ${GOCOVER_MODE}" > coverage.out && cat ./*.cover | grep -v mode: | grep -v pb.go | sort -r | \
    awk '{if($1 != last) {print $0;last=$1}}' >> coverage.out

    gocov convert coverage.out > coverage.json
    gocov report coverage.json > CoverResult.txt
    gocov-html coverage.json > coverage.html
}

prepare_environment
run_gocover_report
exit 0
