/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

var isInitializer = false;

exports.initializer = function (context, callback) {
    isInitializer = true;
    callback(null, '');
};

exports.handler = async (event, context) => {
    const log = context.getLogger();
    log.info("isInitializer %s", isInitializer)
    const output =
    {
        'statusCode': 200,
        'headers':
        {
            'Content-Type': 'application/json'
        },
        'isBase64Encoded': false,
        'body': JSON.stringify(event),
    }
    return output;
}
