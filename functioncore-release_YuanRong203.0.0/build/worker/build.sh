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

BASE_DIR=$(cd "$(dirname "$0")"; pwd)
OUTPUT_DIR="${BASE_DIR}/../../../output/functioncore/"
PROJECT_DIR="${BASE_DIR}/../../pkg/worker"

. "${BASE_DIR}"/../build_function.sh
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${BASE_DIR}/../../../datasystem/output/sdk/go/lib
echo LD_LIBRARY_PATH=$LD_LIBRARY_PATH

function generate_mock()
{
    if ! mockgen -destination "$2" -source "$1" -package mock; then
        log_error "Failed to generate mock file"
        return 1;
    fi

    return 0
}

function worker_module_compile() {
    log_info "worker module build"
    mkdir -p "${OUTPUT_DIR}"/worker/bin \
        "${OUTPUT_DIR}"/worker/config \
        "${OUTPUT_DIR}"/worker/datasystem \
        "${OUTPUT_DIR}"/workerinit/scripts \
        "${OUTPUT_DIR}"/workercustom/bin \
        "${OUTPUT_DIR}"/workercustom/config
    if [ -d "${OUTPUT_DIR}/cppruntime" ];then
        mkdir -p "${OUTPUT_DIR}"/cli/simulator/pkg/runtime/nodejs \
            "${OUTPUT_DIR}"/cli/simulator/pkg/runtime/python \
            "${OUTPUT_DIR}"/cli/simulator/pkg/runtime/java \
            "${OUTPUT_DIR}"/cli/simulator/pkg/runtime/cpp \
            "${OUTPUT_DIR}"/cli/simulator/fdk
    fi

    # Build worker
    cd "${PROJECT_DIR}" || die "${PROJECT_DIR} not exist"
    [ -d mock ] && rm -rf mock
    mkdir -p mock
    cd "${PROJECT_DIR}"/mock
    #generate_mock "${PROJECT_DIR}"/runtime/runtime.go runtime_mock.go
    #generate_mock "${PROJECT_DIR}"/runtime/registry.go registry_mock.go

    cd "${PROJECT_DIR}" || die "${PROJECT_DIR} not exist"
    if ! CC='gcc -fstack-protector-strong -D_FORTIFY_SOURCE=2 -O2' make build-worker; then
        log_error "Failed to build worker!!!"
        return 1
    fi
    chmod -R 700 runtime/
    # copy to output
    cd "${PROJECT_DIR}" || die "${PROJECT_DIR} not exist"
    cp -ar ../../cmd/worker/entrypoint ../../cmd/worker/user_setup "${OUTPUT_DIR}"/worker/bin
    cp -ar build/_output/bin/worker "${OUTPUT_DIR}"/worker/bin/
    cp -ar ../../cmd/worker/sudo_init.sh "${OUTPUT_DIR}"/worker/bin
    cp -ar ../../cmd/worker/script "${OUTPUT_DIR}"/worker/bin
    cp -ar ../../cmd/worker/Dockerfile-worker "${OUTPUT_DIR}"/worker/
    # copy custom runtime config yaml to worker config dir for function graph
    cp -ar ../../build/runtimemanager/config/custom-runtime-config.yaml "${OUTPUT_DIR}"/worker/config/custom-runtime-config.yaml

    cp -ar script/copy_secret.sh "${OUTPUT_DIR}"/workerinit/scripts/
    cp -ar script/iptables.sh "${OUTPUT_DIR}"/workerinit/scripts/
    cp -ar ../../cmd/worker/Dockerfile-init "${OUTPUT_DIR}"/workerinit/

    if [ -d "${OUTPUT_DIR}/cppruntime" ];then
        cp -ar ../cli/local/config/* "${OUTPUT_DIR}"/cli/simulator/
        cp -ar ../cli/local/scripts/start_runtimer.sh "${OUTPUT_DIR}"/cli/simulator/
        cp -ar runtime/nodejs/runtime.go "${OUTPUT_DIR}"/cli/simulator/pkg/runtime/nodejs/
        cp -ar runtime/python/runtime.go "${OUTPUT_DIR}"/cli/simulator/pkg/runtime/python/
        cp -ar runtime/java/runtime.go "${OUTPUT_DIR}"/cli/simulator/pkg/runtime/java/
        cp -ar ../../cmd/worker/Dockerfile-cli "${OUTPUT_DIR}"/cli/simulator/
    fi

    cp -ar ../../cmd/worker/entrypoint ../../cmd/worker/user_setup "${OUTPUT_DIR}"/workercustom/bin
    cp -ar build/_output/bin/worker "${OUTPUT_DIR}"/workercustom/bin/
    cp -ar ../../cmd/worker/sudo_init.sh "${OUTPUT_DIR}"/workercustom/bin
    cp -ar ../../cmd/worker/Dockerfile-custom "${OUTPUT_DIR}"/workercustom/
    cp -ar "${BASE_DIR}"/config/custom-runtime-config.yaml "${OUTPUT_DIR}"/workercustom/config

    # copy datasystem go sdk so file lib dir to worker
    cp -ar ${BASE_DIR}/../../../datasystem/output/sdk/go/lib "${OUTPUT_DIR}"/worker/datasystem/lib
    cd "${OUTPUT_DIR}"/worker/datasystem/lib
    rm -rf libcrypto.so libcrypto.so.1.1 libssl.so libssl.so.1.1
    cd -

    return 0
}

while getopts 'm' opt; do
    case "${opt}" in
    m)
        worker_module_compile
        ;;
    *)
        log_error "invalid command"
        usage
        exit 1
        ;;
    esac
done

exit 0
