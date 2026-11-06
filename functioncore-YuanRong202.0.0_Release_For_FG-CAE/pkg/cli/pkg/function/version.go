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

package function

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"functioncore/pkg/cli/config"
	"functioncore/pkg/cli/constant"
	"functioncore/pkg/cli/pkg/client"
	"functioncore/pkg/cli/pkg/verify"
)

const versionIndex = 6

// PublishVersion -
func PublishVersion(c client.Client, functionName, serviceName, version, tenantID string) error {
	if err := verify.CheckServiceName(serviceName); err != nil {
		return err
	}
	if err := verify.CheckFunctionName(functionName); err != nil {
		return err
	}
	if err := verify.CheckVersion(version); err != nil {
		return err
	}
	if err := publishFuncMeta(c, functionName, serviceName, version, tenantID); err != nil {
		return err
	}
	if err := publishInstanceMeta(c, functionName, serviceName, version, tenantID); err != nil {
		return err
	}
	return nil
}

func publishInstanceMeta(c client.Client, functionName, serviceName, version, tenantID string) error {
	instanceKey := getInstanceKey(config.FConfig.CLIConfig.ClusterID, tenantID, serviceName,
		functionName, "latest")
	values, err := c.GetValuesByKey(instanceKey)
	if err != nil {
		return err
	}

	versionInstanceKey := getInstanceKey(config.FConfig.CLIConfig.ClusterID, tenantID, serviceName,
		functionName, version)
	if err := c.PutKeyWithValue(versionInstanceKey, values[0]); err != nil {
		fmt.Printf("failed to put instanceData value to etcd, %s", err)
		return err
	}
	return nil
}

func publishFuncMeta(c client.Client, functionName, serviceName, version, tenantID string) error {
	funcKey := getFuncKey(tenantID, serviceName, functionName, constant.LatestVersion)
	values, err := c.GetValuesByKey(funcKey)
	if err != nil {
		return err
	}
	if len(values) == 0 {
		return errors.New("the function does not exist")
	}
	funcMetaData := &EtcdData{}
	if err = json.Unmarshal([]byte(values[0]), funcMetaData); err != nil {
		return err
	}
	splits := strings.Split(funcMetaData.ObjectID, "-")
	if len(splits) < versionIndex {
		return errors.New("objectId is not correct")
	}
	splits[versionIndex] = version
	dtsObjectID := strings.Join(splits, "-")
	if err := c.CopyObject(dtsObjectID, funcMetaData.ObjectID); err != nil {
		return err
	}
	versionURN := funcMetaData.FunctionUrn + ":" + version
	funcMetaData.FunctionVersionUrn = versionURN
	funcMetaData.ObjectID = dtsObjectID
	funcMetaData.Version = version
	data, err := json.Marshal(funcMetaData)
	if err != nil {
		return err
	}
	versionFuncKey := getFuncKey(tenantID, serviceName, functionName, version)
	if err := c.PutKeyWithValue(versionFuncKey, string(data)); err != nil {
		fmt.Printf("failed to put funcMetaData value to etcd, %s", err)
		return err
	}
	return nil
}
