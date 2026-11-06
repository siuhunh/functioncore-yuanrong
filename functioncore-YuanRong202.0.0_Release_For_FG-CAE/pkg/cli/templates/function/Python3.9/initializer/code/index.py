#
# Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
#
# -*- coding:utf-8 -*-
import json

is_initializer = False

def initializer(context):
    global is_initializer
    is_initializer = True

def handler (event, context):
    log = context.getLogger()
    log.info("is_initializer: %s",is_initializer)
    return {
        "statusCode": 200,
        "isBase64Encoded": False,
        "body": json.dumps(event),
        "headers": {
            "Content-Type": "application/json"
        }
    }