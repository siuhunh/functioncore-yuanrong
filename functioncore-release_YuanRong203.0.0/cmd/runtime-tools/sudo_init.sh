#!/bin/bash
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

set -e

echo "start runtime-tools"

sudo rm -rf /home/sn/caetools/*
sudo cp -ar /home/sn/runtime-tools/* /home/sn/caetools

sudo chown 1003:1003 -R /home/sn/caetools/home/snuser/bin
sudo chown 1003:1003 -R /home/sn/caetools/home/snuser/runtime/java
sudo chown 1003:1003 -R /home/sn/caetools/home/snuser/config
sudo chown 1000:1000 -R /home/sn/caetools/home/snuser/apm-javaagent
if [ "$(ls -A /home/sn/caetools/home/snuser/apm-javaagent)" ]; then
  sudo find /home/sn/caetools/home/snuser/apm-javaagent -type d | xargs chmod 750
  sudo find /home/sn/caetools/home/snuser/apm-javaagent -type f | xargs chmod 740
else
  echo "no apm-javaagent"
fi
sudo chown 1000:1000 -R /home/sn/caetools/home/sn/bin
sudo chown 1000:1000 /home/sn/caetools/home/sn/config
sudo chown 1003:1003 /home/sn/caetools/home/sn/config/*
sudo chown 1000:1000 -R /home/sn/caetools/opt/huawei/fastfreeze
if [ "$(ls -A /home/sn/caetools/opt/huawei/fastfreeze)" ]; then
  sudo chmod 755 -R /home/sn/caetools/opt/huawei/fastfreeze
else
  echo "no fastfreeze"
fi
sudo chown 1000:1000 -R /home/sn/caetools/opt/huawei/faststart
if [ "$(ls -A /home/sn/caetools/opt/huawei/faststart)" ]; then
  sudo chmod 755 -R /home/sn/caetools/opt/huawei/faststart
else
  echo "no faststart"
fi

sudo find /home/sn/caetools/home/sn/bin -type d | xargs chmod 500
sudo find /home/sn/caetools/home/sn/bin -type f | xargs chmod 500
sudo find /home/sn/caetools/home/sn/config -type d | xargs chmod 550
sudo find /home/sn/caetools/home/sn/config -type f | xargs chmod 440

sudo find /home/sn/caetools/home/snuser/runtime -type d | xargs chmod 550
sudo find /home/sn/caetools/home/snuser/runtime -type f | xargs chmod 440
sudo find /home/sn/caetools/home/snuser/bin -type d | xargs chmod 550
sudo find /home/sn/caetools/home/snuser/bin -type f | xargs chmod 440
sudo find /home/sn/caetools/home/snuser/config -type d | xargs chmod 550
sudo find /home/sn/caetools/home/snuser/config -type f | xargs chmod 440

if [ -f /home/sn/caetools/home/snuser/runtime/java/RunTime-2.0.6.jar ]; then
  chmod 600 /home/sn/caetools/home/snuser/runtime/java/RunTime-2.0.6.jar
else
  echo "no RunTime-2.0.6.jar"
fi

sudo setcap cap_dac_override,cap_fowner,cap_kill,cap_setgid,cap_setuid,cap_chown,cap_syslog,cap_sys_resource+ep /home/sn/caetools/home/sn/bin/runtime-manager

sed -i '/sudo_init.sh/d' /etc/sudoers
