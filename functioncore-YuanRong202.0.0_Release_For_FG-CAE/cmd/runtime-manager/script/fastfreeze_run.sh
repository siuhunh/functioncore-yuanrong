#!/bin/bash
# Copyright (c) 2022 Huawei Technologies Co., Ltd
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

# java runtime by fastfreeze
set -e

JAVA_OPTS=$1
IMAGE_PATH=$2
PRESERVE_PATH=$3

echo "number $#"
if [ $# == 4 ]; then
  export CRIU_OPTS="$4"
fi
echo "CRIU_OPTS: $CRIU_OPTS"

/opt/huawei/fastfreeze/fastfreeze run -vvvv --image-url file:$IMAGE_PATH --preserve-path "$PRESERVE_PATH" -- bash -c "$JAVA_OPTS" < /dev/null > /dev/null 2> /dev/null
echo "start java runtime by fastfreeze"