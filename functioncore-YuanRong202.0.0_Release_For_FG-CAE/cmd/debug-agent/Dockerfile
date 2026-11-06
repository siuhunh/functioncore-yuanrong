# Copyright (c) 2024 Huawei Technologies Co., Ltd
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

ENV USER_UID=1000 \
    USER_NAME=sn \
    GROUP_ID=1000 \
    GROUP_NAME=sn \
    HOME=/home/sn \
    DEBUGAGENT=${HOME}/bin/debug-agent

RUN mkdir -p ${HOME} && \
    groupmod --new-name ${GROUP_NAME} paas && \
    usermod -l ${USER_NAME} paas -g ${GROUP_ID} && \
    usermod -s /sbin/nologin ${USER_NAME} && \
    usermod -aG root ${USER_NAME} && \
    chown -R ${USER_UID}:${GROUP_ID} /home && \
    echo "sn    ALL=NOPASSWD:/bin/bash /home/sn/bin/sudo_init.sh" >> /etc/sudoers

# USER_UID GROUP_ID 1000
COPY --chown=1000:1000 bin ${HOME}/bin
COPY --chown=1000:1000 bin/debug-agent ${DEBUGAGENT}
COPY --chown=1000:1000 bin/entrypoint ${HOME}/bin/entrypoint
COPY --chown=1000:1000 bin/sudo_init.sh ${HOME}/bin/sudo_init.sh

USER ${USER_UID}:${GROUP_ID}

RUN chmod 700 ${HOME} && \
    find ${HOME}/bin -type d | xargs chmod 500 && \
    find ${HOME}/bin -type f | xargs chmod 500

WORKDIR ${HOME}

ENTRYPOINT ["/bin/bash", "/home/sn/bin/entrypoint"]