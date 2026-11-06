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
SRC_PATH=${ROOT_PATH}/pkg/state-manager
OUTPUT_PATH=${CUR_DIR}/output

# prepare test environment:
# 1.generate proto file
# 2.create output path
prepare_environment()
{
    mkdir -p /home/sn/repo
    mkdir -p /home/sn/config
    cp "${ROOT_PATH}/test/functioncore/log.json" /home/sn/config/log.json

    cd "${SRC_PATH}"
    [[ -d mock ]] && rm -rf mock
    if ! mkdir mock; then
        log_error "Failed to mkdir mock"
        exit 1
    fi

    go get github.com/golang/mock/mockgen@v1.6.0
    go install github.com/golang/mock/mockgen@v1.6.0

    cd "${SRC_PATH}"/mock
    $(generate_mock "${SRC_PATH}"/scheduler/predicate/abstract.go predicate_mock.go)
    $(generate_mock "${SRC_PATH}"/scheduler/priority/abstract.go priority_mock.go)

    rm -rf "${OUTPUT_PATH}"
    mkdir -p "${OUTPUT_PATH}"
}

# run go test and report
run_gocover_report()
{
    cd "${SRC_PATH}"

    if ! go test -gcflags "all=-N -l" -v -covermode="${GOCOVER_MODE}" -coverprofile="$OUTPUT_PATH"/state-manager.cover -coverpkg=./... ./...; then
        log_error "Failed to go test state-manager"
        exit 1
    fi

    # export llt coverage result
    cd ${OUTPUT_PATH}
    echo "mode: ${GOCOVER_MODE}" > coverage.out && cat ./*.cover | grep -v mode: | grep -v pb.go | grep -v mock.go | sort -r | \
    awk '{if($1 != last) {print $0;last=$1}}' >> coverage.out

    gocov convert coverage.out > coverage.json
    gocov report coverage.json > CoverResult.txt
    gocov-html coverage.json > coverage.html
}

prepare_environment
run_gocover_report
exit 0
