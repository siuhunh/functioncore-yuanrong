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

package podhandler

// Manager -
type Manager struct {
	handlers map[PodType]PodHandler
}

// CreatePodHandlerManager -
func CreatePodHandlerManager() *Manager {
	return &Manager{
		handlers: map[PodType]PodHandler{},
	}
}

// AddPodHandler -
func (m *Manager) AddPodHandler(podType PodType, handler PodHandler) {
	m.handlers[podType] = handler
}

// FilterByPodCreateReq -
func (m *Manager) FilterByPodCreateReq(req *PodCreateReq) PodHandler {
	for _, handler := range m.handlers {
		if handler.FilterByPodCreateReq(req) {
			return handler
		}
	}
	return nil
}

// FilterByPodName -
func (m *Manager) FilterByPodName(podName string) PodHandler {
	for _, handler := range m.handlers {
		if handler.FilterByPodName(podName) {
			return handler
		}
	}
	return nil
}
