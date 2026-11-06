#!/bin/bash
# Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
# Description: ETCD Scheduled Task Script: Run the monitor.sh start command every minute.
# Run the log-div.sh script at 00:00 every day.

set -e

cur_dir=$(cd "$(dirname "$0")"; pwd)
source ${cur_dir}/cluster.cfg
# etcd check and alarm
echo "*/1 * * * * root source /etc/profile; /bin/bash ${cur_dir}/monitor.sh start >/dev/null 2>&1" > ${cur_dir}/crontab.tmp
echo "0 0 * * * root source /etc/profile; /bin/bash ${cur_dir}/log-div.sh ${LOG_DIR} 7 etcd" >> ${cur_dir}/crontab.tmp
[ ${ETCD_ALARM_ENABLE} == true ] && \
echo "0 0 * * * root source /etc/profile; /bin/bash ${cur_dir}/log-div.sh ${ETCD_ALARM_PATH} 3 alarm" >> ${cur_dir}/crontab.tmp

# ntp_check
[ ${NTP_CHECK_ENABLE} == true ] && \
echo "*/1 * * * * root source /etc/profile; /bin/bash ${cur_dir}/monitor.sh ntpcheck >/dev/null 2>&1" >> ${cur_dir}/crontab.tmp && \
echo "0 0 * * * root source /etc/profile; /bin/bash ${cur_dir}/log-div.sh ${NTP_CHECK_PATH} 3 ntp_check" >> ${cur_dir}/crontab.tmp

crontab ${cur_dir}/crontab.tmp
crontab -l
rm -f ${cur_dir}/crontab.tmp
