#!/bin/bash
# Copyright (c) 2025 Huawei Technologies Co., Ltd
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

IMAGE_PATH=$1
ON_APP_READY=$2

echo "number $#"
if [ $# == 3 ]; then
  export CRIU_OPTS="$3"
fi
echo "CRIU_OPTS: $CRIU_OPTS"

echo "start to restore by faststart"
/opt/huawei/faststart/faststart restore --image-url="file:$IMAGE_PATH" --on-app-ready="$ON_APP_READY && kill -12 1000"