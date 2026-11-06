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

package taskcontext

// ServiceIDContext represents context for service id.
type ServiceIDContext interface {
	SetServiceID(serviceID string)
	ServiceID() string
}

// ServiceIDImpl implements context for service id.
type ServiceIDImpl struct {
	serviceID string
}

// SetServiceID sets context's serviceID.
func (c *ServiceIDImpl) SetServiceID(serviceID string) {
	c.serviceID = serviceID
}

// ServiceID returns context's serviceID.
func (c *ServiceIDImpl) ServiceID() string {
	return c.serviceID
}
