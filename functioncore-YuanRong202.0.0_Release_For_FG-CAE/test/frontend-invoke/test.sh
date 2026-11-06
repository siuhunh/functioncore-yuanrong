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
SRC_PATH=${ROOT_PATH}/pkg/frontend-invoke
OUTPUT_PATH=${CUR_DIR}/output

# run go test and report
run_gocover_report()
{
    rm -rf "${OUTPUT_PATH}"
    mkdir -p "${OUTPUT_PATH}"

    if ! sh "${SRC_PATH}"/../faas-scheduler/script/gen_pb.sh; then
        log_error "Failed to gen faas-scheduler pb!!"
        return 1
    fi

    cd ${SRC_PATH}
    go test -v -gcflags=all=-l -covermode="atomic" -coverprofile="$OUTPUT_PATH/frontend.cover" -coverpkg=./... ./...

    if [ $? -ne 0 ]; then
        log_error "failed to go test frontend-invoke"
        exit 1
    fi

    # export llt coverage result
    cd "$OUTPUT_PATH"
    echo "mode: atomic" > coverage.out && cat ./*.cover | grep -v mode: | grep -v pb.go | sort -r | \
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

run_gocover_report
exit 0
