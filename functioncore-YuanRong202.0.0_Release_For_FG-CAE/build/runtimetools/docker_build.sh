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
OUTPUT_DIR="${BASE_DIR}"/../../../output/functioncore/runtime-tools
RUNTIME_MANAGER_OUTPUT_DIR="${BASE_DIR}"/../../../output/functioncore/runtime-manager

. "${BASE_DIR}"/../build_function.sh

function runtimetools_module_compile() {
    log_info "docker build module runtime-tools"
    get_base_image

    rm -rf "${OUTPUT_DIR}"/home
    rm -rf "${OUTPUT_DIR}"/opt
    mkdir -p "${OUTPUT_DIR}"/home/snuser/bin/fastfreeze
    mkdir -p "${OUTPUT_DIR}"/home/snuser/runtime/java
    mkdir -p "${OUTPUT_DIR}"/home/snuser/config
    mkdir -p "${OUTPUT_DIR}"/home/sn/bin
    mkdir -p "${OUTPUT_DIR}"/home/sn/config
    mkdir -p "${OUTPUT_DIR}"/opt/huawei

    cd "${RUNTIME_MANAGER_OUTPUT_DIR}"/script
    if [ -f fastfreeze-v1.3.0.tar.xz ]; then
      tar -xvf fastfreeze-v1.3.0.tar.xz
      rm -rf fastfreeze/bin/openssl
      rm -rf fastfreeze-v1.3.0.tar.xz
      cp -arf "${RUNTIME_MANAGER_OUTPUT_DIR}"/script/fastfreeze "${OUTPUT_DIR}"/opt/huawei/
    else
      echo "WARNING: no fastfreeze"
      mkdir "${OUTPUT_DIR}"/opt/huawei/fastfreeze
    fi

    cd "${RUNTIME_MANAGER_OUTPUT_DIR}"
    if [ -f apm-javaagent-2.4.1.2.tar ]; then
          tar -xf apm-javaagent-2.4.1.2.tar
          cp -arf "${RUNTIME_MANAGER_OUTPUT_DIR}"/apm-javaagent "${OUTPUT_DIR}"/home/snuser/
        else
          echo "WARNING: no apm-javaagent"
          mkdir "${OUTPUT_DIR}"/home/snuser/apm-javaagent
        fi

    cp -ar "${RUNTIME_MANAGER_OUTPUT_DIR}"/javaruntime/bin/entrypoint_runtime_java "${OUTPUT_DIR}"/home/snuser/bin/entrypoint
    cp -ar "${RUNTIME_MANAGER_OUTPUT_DIR}"/javaruntime/pkg/runtime/java/runtime-core-*.jar "${OUTPUT_DIR}"/home/snuser/runtime/java/
    if [ -f "${RUNTIME_MANAGER_OUTPUT_DIR}"/RunTime-2.0.6.jar ]; then
      cp -ar "${RUNTIME_MANAGER_OUTPUT_DIR}"/RunTime-2.0.6.jar "${OUTPUT_DIR}"/home/snuser/runtime/java/
    else
      echo "WARNING: no RunTime-2.0.6.jar"
    fi
    cp -ar "${RUNTIME_MANAGER_OUTPUT_DIR}"/javaruntime/log4j2.xml "${OUTPUT_DIR}"/home/snuser/config/
    cp -ar "${RUNTIME_MANAGER_OUTPUT_DIR}"/config/runtime.json "${OUTPUT_DIR}"/home/snuser/config/
    cp -ar "${RUNTIME_MANAGER_OUTPUT_DIR}"/script/*.sh "${OUTPUT_DIR}"/home/snuser/bin/fastfreeze/
    cp -ar "${OUTPUT_DIR}"/entrypoint-cae "${OUTPUT_DIR}"/home/sn/bin/entrypoint
    cp -ar "${RUNTIME_MANAGER_OUTPUT_DIR}"/bin/runtime-manager "${OUTPUT_DIR}"/home/sn/bin/
    cp -ar "${RUNTIME_MANAGER_OUTPUT_DIR}"/config/custom-runtime-config.yaml "${OUTPUT_DIR}"/home/sn/config/

    cd "${OUTPUT_DIR}" || die "${OUTPUT_DIR} not exist"
    if ! docker build --build-arg BASE_IMAGE="${BASE_IMAGE}" -t runtime-tools:"${VERSION}" -f Dockerfile .; then
        log_error "Failed to docker build runtime-tools!!!"
        exit 1
    fi

    return 0
}

while getopts 'v:p:h' opt; do
    case "${opt}" in
    v)
        VERSION="${OPTARG}"
        runtimetools_module_compile
        ;;
    p)
        exit 0
        ;;
    h)
        usage
        exit 0
        ;;
    *)
        log_error "invalid command"
        usage
        exit 1
        ;;
    esac
done

exit 0
