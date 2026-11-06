#!/bin/bash
# Copyright (c) 2023 Huawei Technologies Co., Ltd
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
# ----------------------------------------------------------------------
# funcname: usage
# description:  the build Instructions
# parameters:   void
# return value: void
# ----------------------------------------------------------------------
set -e
YELLOW_ZONE_VERSION_URL="http://10.244.181.91:8080/view/7_FG_VersionBuild/job/FunctionGraph_CMC_Download_Compile/lastSuccessfulBuild/artifact/output/imageTag"
GREEN_ZONE_VERSION_URL="http://7.219.11.5:8080/job/FunctionGraph_CMC_Download_Compile/lastSuccessfulBuild/artifact/output/imageTag"
REGISTRY="cd-docker-hub.szxy5.artifactory.cd-cloud-artifact.tools.huawei.com/yuanrong_euleros_x86/"
GREEN_ZONE_REGISTRY="registry-cbu.huawei.com/functiongraph/"
VERSION=""

get_version()
{
        VERSION_URL=$1
        curl $VERSION_URL -o imageTag 2>/dev/null
        VERSION=`cat imageTag|tr -d '"'`
        echo $VERSION
}

generate()
{
        REGISTRY=$1
        TAG=$2
        echo "$REGISTRY $TAG"
        kubectl get deploy worker-manager -o yaml > worker-manager.yaml
        python3 generate.py $REGISTRY $TAG
        #echo "*/10 * * * *  root /bin/bash `pwd`/delete_snapshot_pod.sh" | sudo tee -a /etc/crontab > /dev/null
}

usage()
{
    echo -e "Usage: ./generate_deployment.sh [-g] [-t image_tag] [-r image_registry] [-h help]"
    echo -e "Options:"
    echo -e "     -g generate snapshot-manager k8s deployment file for Green Zone"
    echo -e "     -t use personal image tag. Default is the image tag from daily version build,
                      see Yellow Zone daily version: http://10.244.181.91:8080/view/7_FG_VersionBuild/job/FunctionGraph_CMC_Download_Compile/
                      see Green Zone daily verison: http://7.219.11.5:8080/job/FunctionGraph_CMC_Download_Compile/"
    echo -e "     -r use personal image registry. Default is the image registry from daily version build project,
                      see Yellow Zone image registry: cd-docker-hub.szxy5.artifactory.cd-cloud-artifact.tools.huawei.com/yuanrong_euleros_x86/
                      see Green Zone image registry: registry-cbu.huawei.com/functiongraph/"
    echo -e "     -h usage help"
    echo -e "      "
    echo -e "Example:"
    echo -e "   Generate the snapshot deployment file for Yellow Zone with latest daily version: sh generate_deployment.sh"
    echo -e "   Generate the snapshot deployment file for Green Zone with latest daily version: sh generate_deployment.sh -g"
    echo -e "   Generate the snapshot deployment file with personal image:
                    sh generate_deployment.sh -r yuanrong.docker.tool.huawei.com/k00591264/ -t k0059126424990"
    echo -e ""
}

while getopts 'gt:r:h' opt; do
    case "${opt}" in
    t)
        VERSION="${OPTARG}"
        ;;
    r)
        REGISTRY="${OPTARG}"
        ;;
    g)
        get_version $GREEN_ZONE_VERSION_URL
        generate $GREEN_ZONE_REGISTRY $VERSION
        exit 0
        ;;
    h)
        usage
        exit 0
        ;;
    *)
        log_error "invalid command"
        usage
        exit 1
        ;;
    esac
done

if [ -z "$VERSION" ]; then
     get_version $YELLOW_ZONE_VERSION_URL
fi

generate $REGISTRY $VERSION
exit 0