#!/bin/bash
# Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
# Description: Deploy Script: Rotate log file and delete log before LOG_RETENTION_PERIOD days.
# if disk space is not enough, this script will delete all log before 1 day .
# delete model may not working properly when system time has been changed

set -e

LOG_DIR=$1
LOG_RETENTION_PERIOD=$2
MODULE_NAME=$3
   
Y=$(date -d "yesterday" +'%Y')
M=$(date -d "yesterday" +'%m')
D=$(date -d "yesterday" +'%d'|sed -e 's:^0::')
   
echo "[INFO][$(date +%b\ %d\ %H:%M:%S)]:Start rotate ${MODULE_NAME} log file"
   
cd "$LOG_DIR" || { echo "[ERROR][$(date +%b\ %d\ %H:%M:%S)]:Enter log path failed"; exit 1; }
LOG_BAK_DIR="${LOG_DIR}"/bak
[ ! -d "${LOG_BAK_DIR}" ] && mkdir -p "${LOG_BAK_DIR}"
LOG="${LOG_DIR}"/${MODULE_NAME}.log
chmod -R 700 "$LOG_BAK_DIR";
cd "$LOG_BAK_DIR" || { echo "[ERROR][$(date +%b\ %d\ %H:%M:%S)]:Enter log bak path failed" exit 1; }
   
# check disk space,
LOG_SIZE=$(wc -c  "$LOG" |awk '{print $1}')
# reserved 2m for log increase in log rotate
SPACE_NEED=$((LOG_SIZE/1024*2+2048))
if [ "$(df -P "${LOG_DIR}" | tail -1 | awk '{print $4 }' | cut -d'%' -f1)" -lt "$SPACE_NEED" ];then
    # if disk is not enough, only keep 1 days log
    echo "[WARNING][$(date +%b\ %d\ %H:%M:%S)]:disk space not enough, delete log file out of 1 days"
    find ./ -type f -name '*.tar' -mtime +1|xargs rm -rf;
fi
   
if [ "$(df -P "${LOG_DIR}" | tail -1 | awk '{print $4 }' | cut -d'%' -f1)" -lt "$SPACE_NEED" ];then
    echo "[ERROR][$(date +%b\ %d\ %H:%M:%S)]:disk space not enough"
    exit 1
fi
   
# rotate log
if ! cp "$LOG" "$Y$M${D}".log; then
    echo "[ERROR][$(date +%b\ %d\ %H:%M:%S)]:Copy log file failed"
    exit 1
fi
echo "" > "$LOG"
if ! tar -zcvf "$Y$M${D}".log.gz.tar "$Y$M${D}".log --remove-files;then
    echo "[ERROR][$(date +%b\ %d\ %H:%M:%S)]:Compact log file failed"
    exit 1
fi
chmod -R 600 "$Y$M${D}".log.gz.tar
   
# clear log file before LOG_RETENTION_PERIOD days
# change system time will occur this model not work properly
echo "[INFO][$(date +%b\ %d\ %H:%M:%S)]:Delete log file before ${LOG_RETENTION_PERIOD} days"
find ./ -type f -name '*.tar' -mtime +${LOG_RETENTION_PERIOD}|xargs rm -rf;
echo "[INFO][$(date +%b\ %d\ %H:%M:%S)]:Rotate ${MODULE_NAME} log file successfully"
   
exit 0
