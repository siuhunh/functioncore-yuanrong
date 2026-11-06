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

// Package domain is used for reseting domain name
package domain

import (
	"encoding/json"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/utils"
)

// ResetDomainName -
func ResetDomainName(body []byte) error {
	log.GetLogger().Infof("receive reset domain name request body: %s", string(body))
	extra := map[string]string{}
	err := json.Unmarshal(body, &extra)
	if err != nil {
		log.GetLogger().Errorf("failed to unmarshal body: %s", err.Error())
		return err
	}
	domainNames := map[string]string{}
	err = json.Unmarshal([]byte(extra[constants.HeaderDomainName]), &domainNames)
	if err != nil {
		log.GetLogger().Errorf("failed to unmarshal domain names: %s", err.Error())
		return err
	}
	err = utils.ReplaceByDNS("/etc/hosts", domainNames)
	if err != nil {
		log.GetLogger().Errorf("failed to replace DNS for /etc/hosts file with error: %s", err.Error())
		return err
	}
	log.GetLogger().Infof("succeed to reset domain names: %+v", domainNames)
	return nil
}
