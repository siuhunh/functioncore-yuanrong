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

# global environment
CUR_DIR=$(dirname "$(readlink -f "$0")")
ROOT_PATH=${CUR_DIR}/../..
. "$ROOT_PATH"/build/build_function.sh
SRC_PATH=${ROOT_PATH}/pkg/functionstate/
OUTPUT_PATH=$CUR_DIR/output

# start redis server process should be replaced by docker mode in future.
start_redis() {
    cd $ROOT_PATH/tools/functionstate
    echo "start redis-server"
    if [ -n "$(pidof redis-server)" ];then
        kill -9 $(pidof redis-server)
    fi

    chmod 555 redis-server
    ./redis-server &
}

# stop redis server process should be replaced by docker mode in future.
stop_redis() {
    if [ -n "$(pidof redis-server)" ];then
        kill -9 $(pidof redis-server)
    fi

    # double check
    if [ -n "$(pidof redis-server)" ]
    then
        echo "stop redis-server fail"
        echo 1
    else
        echo "stop redis-server success"
        echo 0
    fi
}

# run go test and report
run_gocover_report()
{
    rm -rf "${OUTPUT_PATH}"
    mkdir -p "${OUTPUT_PATH}"

    cd ${SRC_PATH}
    go test -v -covermode="${GOCOVER_MODE}" -coverprofile="$OUTPUT_PATH/funcitonstate.cover" -coverpkg="./..." "./..."

    if [ $? -ne 0 ]; then
        log_error "failed to go test functionstate"
        exit 1
    fi

    # export llt coverage result
    cd ${OUTPUT_PATH}
    echo "mode: ${GOCOVER_MODE}" > coverage.out && cat ./*.cover | grep -v mode: | grep -v pb.go | sort -r | \
    awk '{if($1 != last) {print $0;last=$1}}' >> coverage.out

    gocov convert coverage.out > coverage.json
    gocov report coverage.json > CoverResult.txt
    gocov-html coverage.json > coverage.html
}

start_redis
run_gocover_report

exit 0