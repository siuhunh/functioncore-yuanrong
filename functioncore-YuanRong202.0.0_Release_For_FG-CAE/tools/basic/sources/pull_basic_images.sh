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

BASE_DIR="$(dirname "$(readlink -f "$0")")"

REPO="yuanrong.docker.tool.huawei.com/yuanrong_basic"
ARCH="$(uname -m)"
PREFIX="${REPO}/${ARCH}"

function ensure_succ(){
    while true; do
        # shellcheck disable=SC2068
        $@
        [[ $? -eq 0 ]] && return 0
    done
    return 1
}

# shellcheck disable=SC2002
cat "${BASE_DIR}/basic_images_${ARCH}.txt" | while read image; do
    image_tag="${PREFIX}/${image}"
    echo "pull ${image_tag} to ${image}"
    ensure_succ docker pull "${image_tag}"
    ensure_succ docker tag "${image_tag}" "${image}"
    ensure_succ docker rmi "${image_tag}"
done
