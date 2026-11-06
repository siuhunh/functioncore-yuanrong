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

# install nfs
[[ ! -d "/home/disk/Volumes" ]] && mkdir -p /home/disk/Volumes
chown -R 1002:1002 /home/disk/Volumes

function install_nfs_ubuntu() {
    set +e
    [[ "$(dpkg -l | grep -c nfs-kernel-server)" -lt 1 ]] && sudo apt-get install --reinstall -y nfs-kernel-server
    cat > "/etc/exports" << EOF
/home/disk/Volumes   *(rw,sync,no_subtree_check,anonuid=1002,anongid=1002)
EOF
    /etc/init.d/rpcbind restart
    /etc/init.d/nfs-kernel-server restart
    set -e
}

function install_nfs_euleros() {
    set +e
    [[ "$(yum list installed | grep -c nfs-utils)" -lt 1 ]] && yum install -y nfs-utils
    cat > "/etc/exports" << EOF
/home/disk/Volumes   *(rw,sync,no_subtree_check,no_root_squash)
EOF
    systemctl enable rpcbind
    systemctl start rpcbind
    systemctl enable nfs-server
    systemctl restart nfs-server
    set -e
}

if [[ "$(which dpkg 2>/dev/null)x" != "x" ]]; then
    install_nfs_ubuntu
fi

if [[ "$(which yum 2>/dev/null)x" != "x" ]]; then
    install_nfs_euleros
fi

exit 0
