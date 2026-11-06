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
OUTPUT_DIR="${BASE_DIR}"/../../output
. "${BASE_DIR}"/build_function.sh

mkdir -p "${BASE_DIR}"/../../output/euleros_x86

cd "${OUTPUT_DIR}" || log_error "${OUTPUT_DIR} does not exist"
if [ -d "IndependentProjectChart" ]; then
    mv ./IndependentProjectChart/ServerlessNative*.tgz ./functioncore/ServerlessNative_deploy.tgz
    mv ./IndependentProjectChart/Workflow*.tgz ./workflow/Workflow.tgz
    rm -rf IndependentProjectChart
fi
if [ -d "functioncore" ]; then
    tar -zcf functioncore.tar.gz functioncore 
    rm -rf functioncore
    mv ./functioncore.tar.gz ./euleros_x86/
fi
if [ -d "workflow" ]; then
    tar -zcf workflow.tar.gz workflow 
    rm -rf workflow
    mv ./workflow.tar.gz ./euleros_x86/
fi
if [ -d "event-bridge" ];then
	tar -zcf eventbridge.tar.gz event-bridge
	rm -rf event-bridge
	mv eventbridge.tar.gz ./euleros_x86/
fi

exit 0