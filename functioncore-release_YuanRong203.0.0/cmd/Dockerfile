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

ARG BASE_IMAGE
FROM ${BASE_IMAGE}

ENV USER_UID=1002 \
    USER_NAME=sn \
    GROUP_ID=${USER_UID}

RUN adduser --disabled-password --gecos "" --uid ${USER_UID} ${USER_NAME} 

COPY entrypoint /home/privilege

RUN chmod 400 /home/privilege/entrypoint

ENTRYPOINT ["/bin/bash", "/home/privilege/entrypoint"]
