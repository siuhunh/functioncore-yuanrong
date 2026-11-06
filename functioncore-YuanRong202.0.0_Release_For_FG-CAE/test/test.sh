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

#!/bin/bash
set -e

CUR_DIR=$(dirname "$(readlink -f "$0")")
ROOT_PATH=${CUR_DIR}/..

export GO111MODULE=on
export GOPROXY="https://go.cloudartifact.dgg.dragon.tools.huawei.com/artifactory/cbu-go-public/"
export NO_PROXY=${NO_PROXY},*.huawei.com
export GONOSUMDB=*
# go1.22.1 需要这个环境变量，go1.22.2 及以上版本修复了这个问题，详细见issue：https://github.com/golang/go/issues/65653
# export GOEXPERIMENT=nocoverageredesign

# coverage mode
# set: 每个语句是否执行？
# count: 每个语句执行了几次？
# atomic: 类似于count, 但表示的是并行程序中的精确计数
export GOCOVER_MODE="set"

# test module name
MODULE_LIST=(\
"common" \
"functionbus" \
"functiontask" \
"worker" \
"worker-manager" \
"runtime-manager" \
"frontend-invoke" \
"cache-manager" \
"state-manager" \
"faas-scheduler" \
"autoscaler" \
"debug-agent"
)

# $1: source file name, In the format of xxx.go
# $2: target file name, In the format of xxx_mock.go
function generate_mock()
{
    if ! mockgen -destination "$2" -source "$1" -package mock; then
        log_error "Failed to generate mock file."
        return 1;
    fi
}
export -f generate_mock

prepare_environment()
{
    # generate functiontask pb files
    cd "${ROOT_PATH}"/build/functiontask/
    if ! bash gen_pb.sh task; then
        log_error "Failed to generate functiontask protobuf!!!"
        exit 1
    fi

    # generate worker pb files
    cd "${ROOT_PATH}"/pkg/worker/script
    if ! bash pb_gen.sh; then
        log_error "Failed to generate protobuf!!!"
        exit 1
    fi
    cd "${ROOT_PATH}"
    go mod tidy
}
prepare_environment

# create source code link, go cover report dependent on GOPATH src
link_source_code()
{
    rm -rf "${GOPATH}/pkg"
    rm -rf "${GOPATH}/src/functioncore"

    mkdir -p "${GOPATH}"/src/
    ln -s "${ROOT_PATH}" "${GOPATH}"/src/functioncore
}
link_source_code

if [ "${1}" = "all" ]; then
    for i in "${!MODULE_LIST[@]}";
    do
        if ! sh -x "${CUR_DIR}/${MODULE_LIST[$i]}/test.sh"; then
            echo "Failed to test ${MODULE_LIST[$i]}"
            exit 1
        fi
        echo "Succeed to test ${MODULE_LIST[$i]}"
    done
    echo "Succeed to test all module"
elif [[ "${MODULE_LIST[@]}" =~ "${1}" ]]; then
    if ! sh -x "${CUR_DIR}/${1}/test.sh"; then
        echo "Failed to test ${1}"
        exit 1
    fi
    echo "Succeed to test ${1}"
else
    echo "Please input parameters 'module name: ${MODULE_LIST[@]}' or 'all' "
    exit 1
fi

exit 0
