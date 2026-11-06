#
# Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
#
# -*- coding:utf-8 -*-
import json
def handler (event, context):
    log = context.getLogger()
    log.info("key1: %s",context.getUserData("key1"))
    return {
        "statusCode": 200,
        "isBase64Encoded": False,
        "body": json.dumps(event),
        "headers": {
            "Content-Type": "application/json"
        }
    }