/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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

// Package utils -
package utils

const (
	ETCDFuncKeyForTest   = "/sn/functions/business/yrk/tenant/fb6a00ff7ae54a5fbb8ff855d0841d00/function/0@default@puresleep-java-10ms/version/latest"
	ETCDFuncValueForTest = `{
	   "funcMetaData": {
	       "layers": [],
	       "name": "0@default@puresleep-java-10ms",
	       "description": "",
	       "functionUrn": "sn:cn:yrk:fb6a00ff7ae54a5fbb8ff855d0841d00:function:0@default@puresleep-java-10ms",
	       "tenantId": "fb6a00ff7ae54a5fbb8ff855d0841d00",
	       "reversedConcurrency": 0,
	       "tags": null,
	       "created": "2022-06-07 04:20:23.895 UTC",
	       "functionUpdateTime": "2022-06-07T12:20:23+08:00",
	       "functionVersionUrn": "sn:cn:yrk:fb6a00ff7ae54a5fbb8ff855d0841d00:function:0@default@puresleep-java-10ms:latest",
	       "revisionId": "20220607042023895",
	       "codeSize": 32696,
	       "codeSha512": "a0a03391b763b06a4638b51dd03adc9292006f0119773237273a4224f67f937e1ac055e28ea094ecee46ee82a26064bf0e707d3553a1ce34e47f93e6454a7c2c",
	       "handler": "com.huawei.demo.TriggerTests::apigTest",
	       "runtime": "java8",
	       "timeout": 15,
	       "version": "latest",
	       "versionDescription": "",
	       "deadLetterConfig": "",
	       "latestVersionUpdateTime": "2022-06-07T12:20:23+08:00",
	       "publishTime": "",
	       "businessId": "yrk",
	       "functionType": "",
	       "id": "943d69aa-ab6a-450c-80c2-10f4b07a4244",
	       "func_id": "d8b5294d-0eab-4bf7-b623-33ec1bf379f7",
	       "func_name": "puresleep-java-10ms",
	       "domain_id": "833fc78659514766be1dc2e2819d72bf",
	       "project_name": "cn-north-7",
	       "service": "default",
	       "dependencies": "",
	       "isStatefulFunction": false,
	       "type": "v2",
	       "enable_auth_in_header": false
	   },
	   "s3MetaData": {
	       "appId": "",
	       "bucketId": "functionstorage",
	       "objectId": "functions/fb6a00ff7ae54a5fbb8ff855d0841d00/default/puresleep-java-10ms/latest/pure-sleep10ms.jar",
	       "bucketUrl": "https://obs.cn-north-7.ulanqab.huawei.com",
	       "code_type": "jar",
	       "code_url": "",
	       "code_filename": "pure-sleep10ms.jar",
	       "func_code": {
	           "file": "",
	           "link": ""
	       }
	   },
	   "envMetaData": {
	       "environment": "",
	       "encrypted_user_data": ""
	   },
	   "resourceMetaData": {
	       "cpu": 4000,
	       "memory": 8192,
	       "enable_dynamic_memory": false,
	       "customResources": ""
	   },
	   "instanceMetaData": {
	       "maxInstance": -1,
	       "minInstance": 0,
	       "concurrentNum": 100,
	       "instanceType": "",
	       "idleMode": false
	   },
	   "extendedMetaData": {
	       "image_name": "latest-220607122023@spbyt",
	       "role": {
	           "xrole": "",
	           "app_xrole": ""
	       },
	       "func_vpc": null,
	       "endpoint_tenant_vpc": null,
	       "mount_config": null,
	       "strategy_config": {
	           "concurrency": -1,
	           "concurrentNum": 1
	       },
	       "extend_config": "",
	       "initializer": {
	           "initializer_handler": "",
	           "initializer_timeout": 0
	       },
	       "heartbeat": null,
	       "enterprise_project_id": "0",
	       "log_tank_service": {
	           "logGroupId": "3eb52dd5-11be-4530-ab57-63e1a480bc9d",
	           "logStreamId": "7f69706e-1815-45d5-a50c-25105bc0eb90"
	       },
	       "tracing_config": {
	           "enabled": false,
	           "tracing_ak": "",
	           "tracing_sk": ""
	       },
	       "custom_container_config": {
	           "control_path": "",
	           "image": "",
	           "command": null,
	           "args": null,
	           "working_dir": "",
	           "uid": 0,
	           "gid": 0
	       }
	   }
	}`

	ETCDInsMetaKeyForTest   = "/instances/business/yrk/cluster/cluster001/tenant/fb6a00ff7ae54a5fbb8ff855d0841d00/function/0@default@puresleep-java-10ms/version/latest"
	ETCDInsMetaValueForTest = `{
    "instanceMetaData": {
        "maxInstance": -1,
        "minInstance": -1,
        "instanceType": "",
        "idleMode": false
    }
}`
	ETCDTenantMetaKeyForTest   = "/sn/quota/cluster/cluster001/tenant/7e1ad6a6-cc5c-44fa-bd54-25873f72a86a/instancemetadata"
	ETCDTenantMetaValueForTest = `{
    "tenantInstanceMetaData": {
        "maxOnDemandInstance": 1000,
        "maxReversedInstance": 1000
    }
}`
	ETCDDefaultTenantMetaKeyForTest = "/sn/quota/cluster/cluster001/default/instancemetadata"
	DNSExampleResponseValue         = `{
    "links": {
        "self": "https://Endpoint/v2/recordsets",
        "next": "https://Endpoint/v2/recordsets?id=&limit=11&marker=2c9eb155587194ec01587224c9f9014a"
    },
    "recordsets": [
        {
            "id": "2c9eb155587194ec01587224c9f9014a",
            "name": "example.com.",
            "type": "SOA",
            "ttl": 300,
            "records": [
                "ns1.hotrot.de. xx.example.com. (1 7200 900 1209600 300)"
            ],
            "status": "ACTIVE",
            "links": {
                "self": "https://Endpoint/v2/zones/2c9eb155587194ec01587224c9f90149/recordsets/2c9eb155587194ec01587224c9f9014a"
            },
            "zone_id": "2c9eb155587194ec01587224c9f90149",
            "zone_name": "example.com.",
            "create_at": "2016-11-17T11:56:03.439",
            "update_at": "2016-11-17T11:56:03.827",
            "default": true,
            "project_id": "e55c6f3dc4e34c9f86353b664ae0e70c"
        },
        {
            "id": "2c9eb155587194ec01587224c9f9014c",
            "name": "example.com.",
            "type": "NS",
            "ttl": 172800,
            "records": [
                "ns2.hotrot.de.",
                "ns1.hotrot.de."
            ],
            "status": "ACTIVE",
            "links": {
                "self": "https://Endpoint/v2/zones/2c9eb155587194ec01587224c9f90149/recordsets/2c9eb155587194ec01587224c9f9014c"
            },
            "zone_id": "2c9eb155587194ec01587224c9f90149",
            "zone_name": "example.com.",
            "create_at": "2016-11-17T11:56:03.439",
            "update_at": "2016-11-17T11:56:03.827",
            "default": true,
            "project_id": "e55c6f3dc4e34c9f86353b664ae0e70c"
        },
        {
            "id": "2c9eb155587228570158722996ca0002",
            "name": "example.org.",
            "type": "SOA",
            "ttl": 300,
            "records": [
                "ns1.hotrot.de. xx.example.org. (1 7200 900 1209600 300)"
            ],
            "status": "ACTIVE",
            "links": {
                "self": "https://Endpoint/v2/zones/2c9eb155587228570158722996c50001/recordsets/2c9eb155587228570158722996ca0002"
            },
            "zone_id": "2c9eb155587228570158722996c50001",
            "zone_name": "example.org.",
            "create_at": "2016-11-17T12:01:17.996",
            "update_at": "2016-11-17T12:56:03.827",
            "default": true,
            "project_id": "e55c6f3dc4e34c9f86353b664ae0e70c"
        },
        {
            "id": "2c9eb155587228570158722996ca0004",
            "name": "example.org.",
            "type": "NS",
            "ttl": 172800,
            "records": [
                "ns2.hotrot.de.",
                "ns1.hotrot.de."
            ],
            "status": "ACTIVE",
            "links": {
                "self": "https://Endpoint/v2/zones/2c9eb155587228570158722996c50001/recordsets/2c9eb155587228570158722996ca0004"
            },
            "zone_id": "2c9eb155587228570158722996c50001",
            "zone_name": "example.org.",
            "create_at": "2016-11-17T12:01:17.996",
            "update_at": "2016-11-17T12:56:03.827",
            "default": true,
            "project_id": "e55c6f3dc4e34c9f86353b664ae0e70c"
        },
        {
            "id": "2c9eb155587228570158722b6ac30007",
            "name": "www.example.com.",
            "description": "This is an example record set.",
            "type": "A",
            "ttl": 300,
            "records": [
                "192.168.10.2",
                "192.168.10.1"
            ],
            "status": "ACTIVE",
            "links": {
                "self": "https://Endpoint/v2/zones/2c9eb155587194ec01587224c9f90149/recordsets/2c9eb155587228570158722b6ac30007"
            },
            "zone_id": "2c9eb155587194ec01587224c9f90149",
            "zone_name": "example.com.",
            "create_at": "2016-11-17T12:03:17.827",
            "update_at": "2016-11-17T12:56:03.827",
            "default": false,
            "project_id": "e55c6f3dc4e34c9f86353b664ae0e70c"
        }
    ],
    "metadata": {
        "total_count": 5
    }
}
`
	MockPullTriggerEtcdKey         = "/sn/functions/business/yrk/tenant/c53626012ba84727b938ca8bf03108ef/function/0@default@012c53de-195c-48b1-a708-a121fdc585bd-fgs-c769dd/version/latest"
	MockPullTriggerPatPodsEnvValue = `[{"containerCidr":"","hostVmCidr":"192.168.0.0/16","patContainerIP":"10.0.0.212","patVmIP":"192.168.244.209","patPortIP":"","patMacAddr":"","patGateway":"192.169.1.1","patPodName":"cff-pat-pod-91e7c127-4563-438d-a6f","tenantCidr":"192.169.1.0/24","natSubnetList":{"exclude":["100.64.0.0/10"],"include":["192.169.1.0/24"]},"isDeleted":false,"isNewCreated":false},{"containerCidr":"","hostVmCidr":"192.168.0.0/16","patContainerIP":"10.0.0.120","patVmIP":"192.168.91.213","patPortIP":"","patMacAddr":"","patGateway":"192.169.1.1","patPodName":"cff-pat-pod-a19aa3dc-2360-4953-bf3","tenantCidr":"192.169.1.0/24","natSubnetList":{"exclude":["100.64.0.0/10"],"include":["192.169.1.0/24"]},"isDeleted":false,"isNewCreated":false}]`

	ETCDWorkerManagerKeyForTest        = "/sn/worker-manager/instances/cluster001/10.247.13.123/worker-manager-6bc89d77fb-58w2d"
	ETCDInvalidWorkerManagerKeyForTest = "/sn/worker-manager/cluster001/10.247.13.123/worker-manager-6bc89d77fb-58w2d"

	ETCDCaeAvailableClusterKeyForTest        = "/cae/avaiablecluster/business/yrk/tenant/c53626012ba84727b938ca8bf03108ef/function/0@default@test-cae-test/version/latest"
	ETCDInvalidCaeAvailableClusterKeyForTest = "/cae/avaiablecluster/business/yrk/tenant/function/0@default@test-cae-test/version/latest"
	ETCDCaeAvailableClusterValueForTest      = `["cluster001", "cluster002"]`

	ETCDCAEFuncValueForTest = `{
	   "funcMetaData": {
	       "layers": [],
	       "name": "0@default@puresleep-java-10ms",
	       "description": "",
	       "functionUrn": "sn:cn:yrk:fb6a00ff7ae54a5fbb8ff855d0841d00:function:0@default@puresleep-java-10ms",
	       "tenantId": "fb6a00ff7ae54a5fbb8ff855d0841d00",
	       "reversedConcurrency": 0,
	       "tags": null,
	       "created": "2022-06-07 04:20:23.895 UTC",
	       "functionUpdateTime": "2022-06-07T12:20:23+08:00",
	       "functionVersionUrn": "sn:cn:yrk:fb6a00ff7ae54a5fbb8ff855d0841d00:function:0@default@puresleep-java-10ms:latest",
	       "revisionId": "20220607042023895",
	       "codeSize": 32696,
	       "codeSha512": "a0a03391b763b06a4638b51dd03adc9292006f0119773237273a4224f67f937e1ac055e28ea094ecee46ee82a26064bf0e707d3553a1ce34e47f93e6454a7c2c",
	       "handler": "com.huawei.demo.TriggerTests::apigTest",
	       "runtime": "java8",
	       "timeout": 15,
	       "version": "latest",
	       "versionDescription": "",
	       "deadLetterConfig": "",
	       "latestVersionUpdateTime": "2022-06-07T12:20:23+08:00",
	       "publishTime": "",
	       "businessId": "yrk",
	       "functionType": "",
	       "id": "943d69aa-ab6a-450c-80c2-10f4b07a4244",
	       "func_id": "d8b5294d-0eab-4bf7-b623-33ec1bf379f7",
	       "func_name": "puresleep-java-10ms",
	       "domain_id": "833fc78659514766be1dc2e2819d72bf",
	       "project_name": "cn-north-7",
	       "service": "default",
	       "dependencies": "",
	       "isStatefulFunction": false,
	       "type": "v2",
	       "enable_auth_in_header": false,
           "businessType": "CAE"
	   },
	   "s3MetaData": {
	       "appId": "",
	       "bucketId": "functionstorage",
	       "objectId": "functions/fb6a00ff7ae54a5fbb8ff855d0841d00/default/puresleep-java-10ms/latest/pure-sleep10ms.jar",
	       "bucketUrl": "https://obs.cn-north-7.ulanqab.huawei.com",
	       "code_type": "jar",
	       "code_url": "",
	       "code_filename": "pure-sleep10ms.jar",
	       "func_code": {
	           "file": "",
	           "link": ""
	       }
	   },
	   "envMetaData": {
	       "environment": "",
	       "encrypted_user_data": ""
	   },
	   "resourceMetaData": {
	       "cpu": 4000,
	       "memory": 8192,
	       "enable_dynamic_memory": false,
	       "customResources": ""
	   },
	   "instanceMetaData": {
	       "maxInstance": -1,
	       "minInstance": 0,
	       "concurrentNum": 100,
	       "instanceType": "",
	       "idleMode": false
	   },
	   "extendedMetaData": {
	       "image_name": "latest-220607122023@spbyt",
	       "role": {
	           "xrole": "",
	           "app_xrole": ""
	       },
	       "func_vpc": null,
	       "endpoint_tenant_vpc": null,
	       "mount_config": null,
	       "strategy_config": {
	           "concurrency": -1,
	           "concurrentNum": 1
	       },
	       "extend_config": "",
	       "initializer": {
	           "initializer_handler": "",
	           "initializer_timeout": 0
	       },
	       "heartbeat": null,
	       "enterprise_project_id": "0",
	       "log_tank_service": {
	           "logGroupId": "3eb52dd5-11be-4530-ab57-63e1a480bc9d",
	           "logStreamId": "7f69706e-1815-45d5-a50c-25105bc0eb90"
	       },
	       "tracing_config": {
	           "enabled": false,
	           "tracing_ak": "",
	           "tracing_sk": ""
	       },
	       "custom_container_config": {
	           "control_path": "",
	           "image": "",
	           "command": null,
	           "args": null,
	           "working_dir": "",
	           "uid": 0,
	           "gid": 0
	       }
	   }
	}`
)
