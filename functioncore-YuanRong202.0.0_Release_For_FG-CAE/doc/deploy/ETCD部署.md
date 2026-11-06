[TOC]

# ETCD部署

## 多节点部署etcd集群（现网环境）

下面说明当前现网部署etcd集群的步骤：
* 1. 在本地机器上从 http://mirrors.tools.huawei.com/etcd/v3.4.16/etcd-v3.4.16-linux-amd64.tar.gz 下载 etcd 安装包，并上传到 OBS
* 2. 在 etcd 节点上创建/home/etcd目录，在节点上然后从OBS下载etcd-v3.4.16-linux-amd64.tar.gz 安装包，并解压到 /home/etcd 目录
* 3. /home/etcd/etcd-v3.4.16-linux-amd64 目录下已经有 etcd 安装文件，并在此目录下创建如下三个文件，然后在该节点上执行启动脚本 start.sh
* 4. 在其他etcd节点上重复上述三个步骤。
* 5. 再在每个 etcd 节点上执行 status.sh 脚本，检查etcd集群的状态。没有报错并能够显示出所有安装的 etcd 节点即可。

- **配置文件 cluster.cfg**
    ```shell
    # Index of local ip in NODE_LIST[0..max-1], for NODES and NAMES
    INDEX=0
    # Node list of cluster
    # keep same in a cluster
    NODES_LIST=127.0.0.1,127.0.0.2,127.0.0.3
     
    # etcd root path
    ETCD_STORE_BASE_DIR=/home/etcd/etcd-v3.4.10-linux-amd64
    # etcd data path
    ETCD_DATA_BASE_DIR=${ETCD_STORE_BASE_DIR}/data
    # ectd log path
    LOG_DIR=${ETCD_STORE_BASE_DIR}/logs
    # ectd backup data path
    ETCD_BACK_DIR=${ETCD_STORE_BASE_DIR}/backup
     
    
    CLIENT_PORT=2379
    
    PEER_PORT=2380
    
    CLUSTER_STATE=new
     
    # Enable client cert authentication
    SSL_ENABLE=false
     
    # Raise alarms when backend size exceeds the given quota
    # keep same in a cluster
    BACKEND_BYTES=8589934592
     
    # time interval of auto compaction retention
    AUTO_COMPACTION=1
     
    # cluster name
    # keep same in a cluster
    CLUSTER_NAME=etcd-cluster-yuanrong01 # Please change this cluster name as need
   ```

- **集群启动脚本 start.sh**
    ```shell
    #!/bin/bash

    CURR_DIR=$(cd "$(dirname "$0")";pwd)
    source "$CURR_DIR"/cluster.cfg
    ETCD_APP_PATH="$CURR_DIR"/../etcd
    
    
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
    
    # the directory of etcd data and logs
    ETCD_DATA_DIR=$ETCD_DATA_BASE_DIR/etcd$INDEX
    if [ ! -d "$ETCD_DATA_DIR" ];then
    if ! mkdir -p "$ETCD_DATA_DIR"; then
    echo "[ERROR][$(date +%b\ %d\ %H:%M:%S)]:Create data directory  failed"
    exit 1
    fi
    chmod 700 "$ETCD_DATA_DIR"
    fi
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
    
    if [ "$SSL_ENABLE" == "true" ]; then
    nohup "$ETCD_APP_PATH" --name "$ETCD_NAME" \
    --data-dir "$ETCD_DATA_DIR" \
    --cert-file "$CERT_FILE" \
    --key-file "$KEY_FILE" \
    --cipher-suites TLS_RSA_WITH_AES_128_GCM_SHA256,TLS_RSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,TLS_AES_128_GCM_SHA256,TLS_AES_256_GCM_SHA384 \
    --peer-client-cert-auth \
    --peer-trusted-ca-file="$PEER_CA_FILE" \
    --peer-cert-file="$PEER_CERT_FILE" \
    --peer-key-file="$PEER_KEY_FILE" \
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
    while [[ "${countDown}" -lt 10 && "$httpCode" != "200" && "$httpCode" != "401" ]]
    do
    sleep 0.1
    (( countDown++ ))
    if [ "$SSL_ENABLE" == "true" ]; then
    httpCode=$(curl -m 3 -o /dev/null -s -w '%{http_code}' --cacert "$CERT_FILE" https://"$LOCAL_IP":"$CLIENT_PORT"/health)
    else
    httpCode=$(curl -m 3 -o /dev/null -s -w '%{http_code}' http://"$LOCAL_IP":"$CLIENT_PORT"/health)
    fi
    echo "[INFO][$(date +%b\ %d\ %H:%M:%S)]:Checking cluster health, return status code: $httpCode. try: $countDown"
    done
    
    
    #find "$ETCD_DATA_DIR" -name key.pem -print0 | xargs -0 rm
    
    if [[ "$httpCode" != "200" && "$httpCode" != "401" ]]; then
    if [ "$(pgrep -f "$ETCD_APP_PATH"|wc -l)" -gt 0 ]; then
    echo "[ERROR][$(date +%b\ %d\ %H:%M:%S)]:Etcd service is unavailable, please check if other etcd nodes have been started or get error detail in log file."
    exit 1
    fi
    echo "[ERROR][$(date +%b\ %d\ %H:%M:%S)]:Start etcd failed, get error detail in log file."
    exit 1   
    fi
    
    echo "[INFO][$(date +%b\ %d\ %H:%M:%S)]:Start etcd successfully"
    
    exit 0
    ```

- **查看集群状态脚本 start.sh**
    ```shell
    #!/bin/bash
    
    CURR_DIR=$(cd "$(dirname "$0")";pwd)
    source "$CURR_DIR"/cluster.cfg
    
    ETCD_APP_PATH="$CURR_DIR"/../etcd
    ETCD_CTL_PATH="$ETCD_STORE_BASE_DIR"/etcdctl
    IFS="," read -r -a NODES <<< "$NODES_LIST"
    LOCAL_IP=${NODES[$INDEX]}
    
    # get local etcd status
    if [ "$(pgrep -f "$ETCD_APP_PATH"|wc -l)" -eq 0 ]; then
    echo "[INFO][$(date +%b\ %d\ %H:%M:%S)]:local service: stopped"
    exit 1
    fi
    echo "[INFO][$(date +%b\ %d\ %H:%M:%S)]:local service: running"
    
    # get cluster status
    echo "[INFO][$(date +%b\ %d\ %H:%M:%S)]:cluster info:"
    if [ "$SSL_ENABLE" == "true" ]; then
    "$ETCD_CTL_PATH" --cacert "$CERT_FILE" --endpoints="$LOCAL_IP":"$CLIENT_PORT" endpoint status --cluster --write-out='table'
    else
    "$ETCD_CTL_PATH" --endpoints="$LOCAL_IP":"$CLIENT_PORT" endpoint status --cluster --write-out='table'
    fi
    
    exit 0
    ```

* 6. 设置账户和密码
    ```shell
    #只需要在一个节点上执行即可
    ./etcdctl --endpoints=http://127.0.0.1:2379 user add root #创建了 root 用户后，设置 root 用户密码
    ./etcdctl --endpoints=http://127.0.0.1:2379 role add root
    ./etcdctl --endpoints=http://127.0.0.1:2379 user grant-role root root
    ./etcdctl --endpoints=http://127.0.0.1:2379 auth enable   #打开鉴权
    ```

## etcd 开启双向认证（当前只有开发环境开启了双向认证）

- **1. 开启客户端和 etcd 的双向认证**
     开启客户端和 etcd 的双向认证，需要增加以下启动项：

     ```sh
      --client-cert-auth --trusted-ca-file=${SERVER_PATH}/ca.crt --cert-file=${SERVER_PATH}/server.crt 
                         --key-file=${SERVER_PATH}/server.key \
       # ${SERVER_PATH}为 etcd证书存放路径
     ```

     且需要更新脚本中以下内容：

     ```sh
     CLUSTER=${NAME_1}=https://${HOST_1}:2380,${NAME_2}=https://${HOST_2}:2380,${NAME_3}=https://${HOST_3}:2380
        --initial-advertise-peer-urls https://${THIS_IP}:2380 --listen-peer-urls https://${THIS_IP}:2380 \
        --advertise-client-urls https://${THIS_IP}:2379 --listen-client-urls https://${THIS_IP}:2379 \
     ```

     客户端检验：
     ```sh
    curl --cacert ${CLIENT_PATH}/ca.crt --cert ${CLIENT_PATH}/client.crt --key ${CLIENT_PATH}/client.key \
         -L https://127.0.0.1:2379/v2/keys/foo -XPUT -d value=bar -v
    # ${CLIENT_PATH}为客户端证书存放路径
     ```
- **2. 开启客户端和 etcd 的单向认证**
  
    开启客户端和 etcd 的单向认证，需要增加以下启动项：
    ```sh
    --cert-file=${CLIENT_PATH}/server.crt --key-file=${CLIENT_PATH}/server.key \  
    ```
    
    脚本更新内容同上
    
    客户端检验：
    ```sh
    curl --cacert ${CLIENT_PATH}/ca.crt https://127.0.0.1:2379/v2/keys/foo -XPUT -d value=bar -v
    ```

- **3. 开启集群内双向对等认证 **
  
    开启集群内双向对等认证，需要增加以下启动项：
    ```sh
    --peer-client-cert-auth --peer-trusted-ca-file=${SERVER_PATH}/ca.crt --peer-cert-file=${SERVER_PATH}/member1.crt 
        --peer-key-file=${SERVER_PATH}/member2.key \
    ```


## etcd 数据的迁移
1. snapshot备份之前的 etcd 数据

    ETCD集群的数据在每个节点存储相同的数据，所以只需在某一节点生成快照即可。
    在--data-dir=${DATAPATH}指定的数据存储目录执行以下指令：
    ```sh
    ENDPOINTS=${THIS_IP} # 指定当前节点 IP
    etcdctl --endpoints=${ENDPOINTS} --user ${USER_NAME}:${PASSWORD} snapshot save snapshot.db
    ```
    如果原先 ETCD集群开通 https，需要指定证书路径：
    ```sh
    etcdctl --cacert="${CA_PATH} " --cert="${CERT_PATH} " --key="${KEY_PATH}" --endpoints=${ENDPOINTS} --user ${USER_NAME}:${PASSWORD} snapshot save snapshot.db
    ```
    生成数据快照 snapshot.db

2. 迁移数据快照
  
    ETCD新集群的每个节点都需要做相同的数据还原操作，生成的 snapshot.db传递到节点指定目录。
    ```sh
    scp snapshot.db root@${NODE_IP}:${PATH}
    ```

3. snapshot 恢复原始数据

    新部署的 etcd 集群数据存储目录--data-dir=${DATAPATH}下为空，否则恢复数据时会报错。

    ```sh
    # 指定当前节点 IP
    ENDPOINTS=${THIS_IP}
    # 指定的数据目录需和集群启动脚本中的目录相同
    etcdctl --endpoints=${ENDPOINTS} snapshot restore snapshot.db --data-dir=${DATAPATH}
    ```

    另：如果原 etcd 集群中添加了用户、角色、开启鉴权，新 etcd 集群会保持一致。
    --data-dir=${DATAPATH}恢复数据后执行部署脚本启动 etcd。
