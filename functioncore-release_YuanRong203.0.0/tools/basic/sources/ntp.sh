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

# install ntp
function install_ntp_ubuntu() {
    set +e
    [[ "$(dpkg -l | grep -c ntp)" -lt 1 ]] && sudo apt-get install ntpdate
    [[ "$(grep -c "server BLRAD01-DC.china.huawei.com" /etc/ntp.conf)" != "0" ]] && echo "server BLRAD01-DC.china.huawei.com" > /etc/ntp.conf
    sudo ntpdate BLRAD01-DC.china.huawei.com
    mv /etc/localtime /etc/localtime.bakmv
    ln -s /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
    set -e
}

function install_ntp_euleros() {
    set +e
    [[ "$(yum list installed | grep -c ntp)" -lt 1 ]] && yum install -y ntp
    systemctl stop ntpd
    ntpdate BLRAD01-DC.china.huawei.com
    systemctl start ntpd
}

if [[ "$(which dpkg 2>/dev/null)x" != "x" ]]; then
    install_ntp_ubuntu
fi

if [[ "$(which yum 2>/dev/null)x" != "x" ]]; then
    install_ntp_euleros
fi

exit 0
