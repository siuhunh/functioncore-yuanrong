/*
 * Copyright (c) 2021 Huawei Technologies Co., Ltd
 *
 * This software is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package com.huawei.example;


import com.huawei.services.runtime.Context;

/**
 * helloworld function
 *
 * @since 2020/1/20
 */
public class JavaHandler {

    public Response handleRequest(Request event, Context context) {
        Response res = new Response();
        String name = event.getName();
        if (name == null) {
            res.setResponse("hello,world!");
        } else {
            res.setResponse("hello," + name);
        }
        return res;
    }

}
