#!/bin/bash
# Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
# Description: ETCD Monitoring Script: Checks whether the etcd process is started.
# If the etcd process is not started, an alarm is generated and the etcd process will be started.
# If the process is started, clear the etcd alarm if it exists.

set -e

umask 0077
cur_dir=$(cd "$(dirname "$0")"; pwd)
source ${cur_dir}/cluster.cfg
etcdProcessName="${ETCD_APP_PATH}"

function check_stopped_process()
{
    local t_process=`ps -A -o stat,pid,ppid,cmd |grep "${etcdProcessName}" |grep -v "grep"|grep -e '^[Tt]'|awk '{print $2}'`
    if [ "X${t_process}" != "X" ]; then
        local pids=$(ps -A -o stat,pid,ppid,cmd | grep "${etcdProcessName}" |grep -v "grep"|grep -e '^[Tt]'|awk '{print $2}')
        for pid in ${pids[@]}
        do
            kill -9 ${pid}
            echo "kill pid: ${pid}, because the status is T or t"
        done
    fi
}

function start()
{
    echo "begin start..."

    check_stopped_process

    local pids=$(ps -wwef |grep ${etcdProcessName} |grep -v grep |awk '{print $2}')
    if [ ! -z "${pids}" ]; then
	      [ ${ETCD_ALARM_ENABLE} == true ] && \
	      [ -f ${ETCD_ALARM_PATH}/reported ] && \
	      ${ETCD_ALARM_PATH}/etcdAlarm --alarmType "clear" --config ${ETCD_ALARM_PATH}/config.json >> ${ETCD_ALARM_PATH}/alarm.log && \
	      rm -f ${ETCD_ALARM_PATH}/reported
        echo "The etcd process  exists"
        return 1
    fi

    [ ${ETCD_ALARM_ENABLE} == true ] && \
    [ ! -f ${ETCD_ALARM_PATH}/reported ] && \
    ${ETCD_ALARM_PATH}/etcdAlarm --alarmType "report" --config ${ETCD_ALARM_PATH}/config.json >> ${ETCD_ALARM_PATH}/alarm.log && \
    touch ${ETCD_ALARM_PATH}/reported

    bash ${ETCD_BIN_PATH}/start.sh
    
    echo "end start..."
}

function stop()
{
    echo "begin stop..."
    local pids=$(ps -wwef |grep ${etcdProcessName} |grep -v grep |awk '{print $2}')
        
    for pid in ${pids}; do
        echo "etcd exist, kill ${pid}"
        kill -9 ${pid}
    done
    echo "end stop"
}

function restart()
{       
    stop
    sleep 1
    start
}

function ntpcheck()
{
	if [ "$(pgrep -f "${NTP_APP_PATH}" |wc -l)" -gt 0 ]; then
    		echo "[INFO][$(date +%b\ %d\ %H:%M:%S)]:Ntp_check has already started"
	else
		nohup ${NTP_APP_PATH} --config ${NTP_CHECK_PATH}/ntp_check_config.json >> ${NTP_CHECK_PATH}/ntp_check.log &
fi
}

function etcd_action()
{
    case "${1}" in
        start)
        start
        ;;
        stop)
        stop
        ;;
       
        restart)
        restart
        ;;

        ntpcheck)
        ntpcheck
        ;;
        *)
        echo "Usage: $0 {start|stop|restart|ntpcheck}"
    esac
}

etcd_action ${1}
