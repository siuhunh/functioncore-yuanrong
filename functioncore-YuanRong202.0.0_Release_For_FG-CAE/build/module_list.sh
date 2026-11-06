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

#module compile list
function get_module_list()
{
    MODULE_LIST=([1]="workermanager" [2]="frontend" [3]="worker" [4]="functiontask" [5]="cachemanager"
        [6]="statemanager" [7]="faasscheduler" [8]="autoscaler" [9]="debugagent" [10]="runtimetools"
        [11]="webterminal")
}

#image build list
function get_image_list()
{
    IMAGES_LIST=([1]="workermanager" [2]="frontend" [3]="worker" [4]="functiontask" [5]="cachemanager"
        [6]="statemanager" [7]="faasscheduler" [8]="autoscaler" [9]="debugagent" [10]="runtimetools"
        [11]="webterminal")
}
