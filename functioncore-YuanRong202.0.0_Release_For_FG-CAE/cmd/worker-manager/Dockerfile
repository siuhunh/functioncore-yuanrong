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
    USER_NAME=sn

ENV GROUP_ID=${USER_UID} \
    GROUP_NAME=${USER_NAME} \
    HOME=/home/${USER_NAME}

ENV WORKERMANAGER=${HOME}/bin/worker-manager

RUN mkdir -p ${HOME} && \
    groupadd -g ${GROUP_ID} ${GROUP_NAME} && \
    useradd -u ${USER_UID} -g ${GROUP_ID} -s /sbin/nologin ${USER_NAME} && \
    chown -R ${USER_UID}:${GROUP_ID} ${HOME} && \
    echo "sn    ALL=NOPASSWD:/bin/bash /home/sn/bin/sudo_init.sh" >> /etc/sudoers

# install operator binary
# USER_UID GROUP_ID 1002
COPY --chown=1002:1002 bin ${HOME}/bin

RUN chmod 700 ${HOME} && \
chmod -R 500 ${HOME}/bin

USER ${USER_UID}

WORKDIR ${HOME}

ENTRYPOINT ["/bin/bash", "/home/sn/bin/entrypoint"]
