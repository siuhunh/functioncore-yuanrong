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

set -eu
set -o pipefail

install_dir=~/.cli
CLI_NAME=""
CA_PATH=""

# clear and remove cli
[ -d "${install_dir}" ] && rm -rf ${install_dir}
mkdir -p ${install_dir}

usage()
{
    echo -e "Usage: ./install.sh [-p CLI_CA_PATH] [-h help]"
    echo -e "Options:"
    echo -e "     -m absolute path of the certificate, such as /user/ca.crt"
    echo -e "Example:"
    echo -e "   sh  build.sh -p \"/user/ca.crt\""
    echo -e ""
}

while getopts 'p:h' opt; do
    case "${opt}" in
    p)
        CA_PATH="${OPTARG}"
        ;;
    h)
        usage
        exit 0
        ;;
    *)
        echo "invalid command"
        exit 1
        ;;
    esac
done

files=$(ls ./cli/bin)
for filename in $files; do
    if [[ "$filename" != *"exe" ]]; then
        CLI_NAME=$filename
    fi
    break
done

if [ -z "$CLI_NAME" ]; then
    echo "there is no incorrect cli installation file"
    exit 1
fi

# install cli
cp -r ./cli/* ${install_dir}
if [ ! -d ~/bin ]; then
    mkdir ~/bin
fi

# configuring the certificate path
if [[ "$CA_PATH" != "" ]]; then
    sed -i "s/http:/https:/" ${install_dir}/resource/config.json
    sed -i "s!\"caPath\": \"\"!\"caPath\": \"${CA_PATH}\"!" ${install_dir}/resource/config.json
fi

ln -sf ${install_dir}/bin/"${CLI_NAME}" ~/bin/"${CLI_NAME}"

echo -e "The CLI is installed successfully."
exit 0
