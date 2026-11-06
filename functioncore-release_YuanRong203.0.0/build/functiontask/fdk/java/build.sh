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
if [ ! -d "$1" ]; then
    echo "please pass through a directory as first argument."
    exit 1
fi
cd "$1"
rm -rf target
sn_jar_num=$(find "$1" -name "sn-core-runtime-sdk*.jar" -type f | wc -l)
if [ "${sn_jar_num}" -ne 1 ]; then
    echo "cannot find or find more than 1 sn-core-runtime-sdk*.jar in $1"
    exit 1
fi
sn_jar_name=$(find "$1" -name "sn-core-runtime-sdk*.jar")
sn_jar=$(basename "${sn_jar_name}")
sn_version_tmp="${sn_jar//sn-core-runtime-sdk-/}"
sn_version="${sn_version_tmp//\.jar/}"
sn_jar_path=${sn_jar_name}
echo "sn jar: ${sn_jar}"
echo "sn jar path: ${sn_jar_path}"
echo "sn_version: ${sn_version}"
mvn install:install-file -Dfile="${sn_jar_path}" -DgroupId=com.huawei.sn -DartifactId=sn-core-runtime-sdk -Dversion="${sn_version}" -Dpackaging=jar
mvn clean package -Dsn.runtime.sdk.java.version="${sn_version}"
rm -rf dep-opensource-*
exit 0
