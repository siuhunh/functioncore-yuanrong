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

# global environment
CUR_DIR=$(dirname "$(readlink -f "$0")")
ROOT_PATH=${CUR_DIR}/../..
. "$ROOT_PATH"/build/build_function.sh
SRC_PATH=${ROOT_PATH}/pkg/common
OUTPUT_PATH=${CUR_DIR}/output

prepare_data_system_lib_path()
{
    # copy data system go sdk so file lib dir to worker
    if [ ! -d "${OUTPUT_PATH}"/datasystem ]; then
        mkdir -p "${OUTPUT_PATH}"/datasystem
    fi
    cp -ar ${ROOT_PATH}/../datasystem/output/sdk/go/lib "${OUTPUT_PATH}"/datasystem/lib
    cd "${OUTPUT_PATH}"/datasystem/lib
    rm -rf libcrypto.so libcrypto.so.1.1 libssl.so libssl.so.1.1
    cd -
    # export library path
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:"${OUTPUT_PATH}"/datasystem/lib
    echo LD_LIBRARY_PATH=$LD_LIBRARY_PATH
    cp "${OUTPUT_PATH}"/datasystem/lib/* /usr/lib64
}

# run go test and report
run_gocover_report()
{
    rm -rf "${OUTPUT_PATH}"
    mkdir -p "${OUTPUT_PATH}"

    cd ${SRC_PATH}
    go test -v -gcflags=all=-l -covermode="${GOCOVER_MODE}" -coverprofile="$OUTPUT_PATH/common.cover" -coverpkg="./..." "./..."

    if [ $? -ne 0 ]; then
        log_error "failed to go test common"
        exit 1
    fi

    # export llt coverage result
    cd "$OUTPUT_PATH"
    echo "mode: ${GOCOVER_MODE}" > coverage.out && cat ./*.cover | grep -v mode: | grep -v pb.go | sort -r | \
    awk '{if($1 != last) {print $0;last=$1}}' >> coverage.out

    gocov convert coverage.out > coverage.json
    gocov report coverage.json > CoverResult.txt
    gocov-html coverage.json > coverage.html
}

# generate proto files for stream
cd "${ROOT_PATH}"/build/common/
 if ! bash gen_pb.sh; then
    log_error "Failed to generate proto files for stream!"
    exit 1
fi

prepare_data_system_lib_path
run_gocover_report
exit 0