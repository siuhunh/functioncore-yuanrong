#!/bin/bash
# Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
# Description: ETCD Script: Check etcd cluster health

set -e
  
cur_dir=$(cd "$(dirname "$0")";pwd)
source "${cur_dir}"/cluster.cfg
  
IFS="," read -r -a NODES <<< "$NODES_LIST"
LOCAL_IP=${NODES[$INDEX]}
  
# get local etcd status
if [ "$(pgrep -f "$ETCD_APP_PATH"|wc -l)" -eq 0 ]; then
    echo "[INFO][$(date +%b\ %d\ %H:%M:%S)]:local service: stopped"
    exit 1
fi
echo "[INFO][$(date +%b\ %d\ %H:%M:%S)]:local service: running"

echo "[INFO][$(date +%b\ %d\ %H:%M:%S)]:cluster info:"
if [ "$SSL_ENABLE" == "true" ]; then
    "$ETCD_CTL_PATH" --cacert "$CA_FILE" --cert "$CERT_FILE" --key "$KEY_FILE" --endpoints="$LOCAL_IP":"$CLIENT_PORT" endpoint status --cluster --write-out='table'
else
    "$ETCD_CTL_PATH" --endpoints="$LOCAL_IP":"$CLIENT_PORT" endpoint status --cluster --write-out='table'
fi
  
exit 0
