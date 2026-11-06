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

ENV USER_UID=1002 \
    USER_NAME=sn \
    GROUP_ID=1002 \
    GROUP_NAME=sn \
    HOME=/home/sn \
    DEPLOY=/home/sn/bin/slo_agent \
    LD_LIBRARY_PATH=${HOME}/lib:${LD_LIBRARY_PATH}

RUN mkdir -p ${HOME} && \
    mkdir -p ${HOME}/bin/ && \
    mkdir -p ${HOME}/lib/ && \
    groupadd -g ${GROUP_ID} ${GROUP_NAME} && \
    useradd -u ${USER_UID} -g ${GROUP_ID} -s /sbin/nologin ${USER_NAME} && \
    chown -R ${USER_UID}:${GROUP_ID} ${HOME} && \
    echo "sn    ALL=NOPASSWD:/bin/bash /home/sn/bin/sudo_init.sh" >> /etc/sudoers

COPY --chown=1002:1002 bin ${HOME}/bin
COPY --chown=1002:1002 bin/slo_agent ${DEPLOY}
COPY --chown=1002:1002 lib ${HOME}/lib

USER root

RUN chmod 700 ${HOME} && \
    find ${HOME}/bin -type d | xargs chmod 500 && \
    find ${HOME}/bin -type f | xargs chmod 500 && \
    setcap 'cap_dac_override+eip' ${DEPLOY} && \
    echo "${HOME}/lib" >> /etc/ld.so.conf.d/snlib.conf && ldconfig

USER ${USER_UID}:${GROUP_ID}

WORKDIR ${HOME}

ENTRYPOINT ["/bin/bash", "/home/sn/bin/entrypoint"]