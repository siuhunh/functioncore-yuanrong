#!/bin/bash
# Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
# Description: ETCD Boot Script: Starts the etcd process.

cur_dir=$(cd "$(dirname "$0")"; pwd)
source ${cur_dir}/cluster.cfg
# there is a one-to-one correspondence between NAMES and NODES
NAMES=()
IFS="," read -r -a NODES <<< "$NODES_LIST"
LEN_NODES=${#NODES[@]}
   
for ((i=0;i<LEN_NODES;i++))
do
    NAMES[$i]=etcd$i
done
   
ETCD_NAME=${NAMES[$INDEX]}
LOCAL_IP=${NODES[$INDEX]}
   
# the directory of logs
[ ! -d $ETCD_DATA_DIR ] && CLUSTER_STATE=new

if [ ! -d "$LOG_DIR" ];then
    if ! mkdir -p "$LOG_DIR"; then
        echo "[ERROR][$(date +%b\ %d\ %H:%M:%S)]:Create log directory failed"
        exit 1
    fi
fi
           
echo "[INFO][$(date +%b\ %d\ %H:%M:%S)]:Starting etcd server: $LOCAL_IP"
if [ "$(pgrep -f "$ETCD_APP_PATH"|wc -l)" -gt 0 ]; then
    echo "[INFO][$(date +%b\ %d\ %H:%M:%S)]:Etcd has already started"
    exit 0
fi
   
(( LEN_NODES-- ))
HTTP_TYPE=http
if [ "$SSL_ENABLE" == "true" ]; then
    HTTP_TYPE=https
fi
for i in $(seq 0 "$LEN_NODES")
do
    if [ "$i" -eq 0 ]; then
        INITIAL_CLUSTER="${NAMES[$i]}=$HTTP_TYPE://${NODES[$i]}:$PEER_PORT"
    else
        INITIAL_CLUSTER=$INITIAL_CLUSTER",${NAMES[$i]}=$HTTP_TYPE://${NODES[$i]}:$PEER_PORT"
    fi
done
echo "INITIAL_CLUSTER"${INITIAL_CLUSTER}

if [ "$SSL_ENABLE" == "true" ]; then
    nohup "$ETCD_APP_PATH" --name "$ETCD_NAME" \
        --data-dir "$ETCD_DATA_DIR" \
        --client-cert-auth \
        --trusted-ca-file "$CA_FILE" \
        --cert-file "$CERT_FILE" \
        --key-file "$KEY_FILE" \
        --peer-client-cert-auth \
        --peer-trusted-ca-file "$PEER_CA_FILE" \
        --peer-cert-file "$PEER_CERT_FILE" \
        --peer-key-file "$PEER_KEY_FILE" \
        --auto-compaction-retention "$AUTO_COMPACTION" \
        --quota-backend-bytes "$BACKEND_BYTES" \
        --advertise-client-urls https://"$LOCAL_IP":"$CLIENT_PORT" \
        --listen-client-urls https://"$LOCAL_IP":"$CLIENT_PORT" \
        --initial-advertise-peer-urls https://"$LOCAL_IP":"$PEER_PORT" \
        --listen-peer-urls https://"$LOCAL_IP":"$PEER_PORT" \
        --initial-cluster-token "$CLUSTER_NAME" \
        --initial-cluster "$INITIAL_CLUSTER" \
        --initial-cluster-state "$CLUSTER_STATE"  \
        --log-package-levels etcdmain=CRITICAL,etcdsever=INFO >> "$LOG_DIR"/etcd.log 2>&1 &
else
    nohup "$ETCD_APP_PATH" --name "$ETCD_NAME" \
        --data-dir "$ETCD_DATA_DIR" \
        --auto-compaction-retention "$AUTO_COMPACTION" \
        --quota-backend-bytes "$BACKEND_BYTES" \
        --advertise-client-urls http://"$LOCAL_IP":"$CLIENT_PORT" \
        --listen-client-urls http://"$LOCAL_IP":"$CLIENT_PORT" \
        --initial-advertise-peer-urls http://"$LOCAL_IP":"$PEER_PORT" \
        --listen-peer-urls http://"$LOCAL_IP":"$PEER_PORT" \
        --initial-cluster-token "$CLUSTER_NAME" \
        --initial-cluster "$INITIAL_CLUSTER" \
        --initial-cluster-state "$CLUSTER_STATE"  \
        --log-package-levels etcdmain=CRITICAL,etcdsever=INFO >> "$LOG_DIR"/etcd.log 2>&1 &
fi
   
   
echo "[INFO][$(date +%b\ %d\ %H:%M:%S)]:Checking etcd cluster startus..."
# health check
httpCode=""
countDown=0
while [[ "${countDown}" -lt 10 && "$httpCode"x != "200"x && "$httpCode"x != "401"x ]]
do
    sleep 0.1
    (( countDown++ ))
    if [ "$SSL_ENABLE" == "true" ]; then
        httpCode=$(curl -m 3 -o /dev/null -s -w '%{http_code}' --cacert "$CA_FILE" --cert "$CERT_FILE" --key "$KEY_FILE" https://"$LOCAL_IP":"$CLIENT_PORT"/health)
    else
        httpCode=$(curl -m 3 -o /dev/null -s -w '%{http_code}' http://"$LOCAL_IP":"$CLIENT_PORT"/health)
    fi
    echo "[INFO][$(date +%b\ %d\ %H:%M:%S)]:Checking cluster health, return status code: $httpCode. try: $countDown"
done

if [[ "$httpCode"x != "200"x && "$httpCode"x != "401"x ]]; then
    if [ "$(pgrep -f "$ETCD_APP_PATH"|wc -l)" -gt 0 ]; then
        echo "[ERROR][$(date +%b\ %d\ %H:%M:%S)]:Etcd service is unavailable, please check if other etcd nodes have been started or get error detail in log file."
        exit 1
    fi
    echo "[ERROR][$(date +%b\ %d\ %H:%M:%S)]:Start etcd failed, get error detail in log file."
    exit 1   
fi
   
echo "[INFO][$(date +%b\ %d\ %H:%M:%S)]:Start etcd successfully"
          
exit 0
