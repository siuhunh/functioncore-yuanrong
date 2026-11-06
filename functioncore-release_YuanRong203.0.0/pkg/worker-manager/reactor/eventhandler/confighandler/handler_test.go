/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
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

package confighandler

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/worker-manager/config"
	evs "functioncore/pkg/worker-manager/eventsource"
	"functioncore/pkg/worker-manager/poolmanager"
	"functioncore/pkg/worker-manager/utils"
)

func Test_CaeConfigHandler(t *testing.T) {
	convey.Convey("Test_CaeConfigHandler Added ok", t, func() {
		pm := &poolmanager.PoolMgr{}
		pool := &poolmanager.Pool{
			Deployment: &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{{
								Name: utils.RuntimeContainerName,
							}},
						},
					},
				},
			},
		}
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(poolmanager.GetPoolManager, func() *poolmanager.PoolMgr {
				return pm
			}),
			gomonkey.ApplyFunc(utils.UpdateRuntimeImageByK8s, func(deployName string, image string) error {
				return nil
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(pm), "GetPoolList", func(p *poolmanager.PoolMgr) map[string]*poolmanager.Pool {
				return map[string]*poolmanager.Pool{
					"cae-300-128": pool,
				}
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(pool), "IsCaePool", func(p *poolmanager.Pool) bool {
				return true
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		handler := NewEventHandler()
		bytes, _ := json.Marshal(&config.CaeConfig{
			CaeBaseImage: "aprilyolies",
		})
		convey.So(pool.Deployment.Spec.Template.Spec.Containers[0].Image, convey.ShouldEqual, "")
		err := handler.HandleEvent(evs.Event{
			Kind:      utils.CaeConfigEventKind,
			EventType: evs.Added,
			Object: &etcd3.Event{
				Value: bytes,
			},
		})
		convey.So(err, convey.ShouldBeNil)
		convey.So(pool.Deployment.Spec.Template.Spec.Containers[0].Image, convey.ShouldEqual, "aprilyolies")
	})
	convey.Convey("Test_CaeConfigHandler event.Kind != utils.CaeConfigEventKind", t, func() {
		pool := &poolmanager.Pool{
			Deployment: &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{{
								Name: utils.RuntimeContainerName,
							}},
						},
					},
				},
			},
		}
		handler := NewEventHandler()
		bytes, _ := json.Marshal(&config.CaeConfig{
			CaeBaseImage: "aprilyolies",
		})
		err := handler.HandleEvent(evs.Event{
			EventType: evs.Added,
			Object: &etcd3.Event{
				Value: bytes,
			},
		})
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(pool.Deployment.Spec.Template.Spec.Containers[0].Image, convey.ShouldEqual, "")
	})
	convey.Convey("Test_CaeConfigHandler CaeConfigHandler assert etcd3.Event failed", t, func() {
		pool := &poolmanager.Pool{
			Deployment: &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{{
								Name: utils.RuntimeContainerName,
							}},
						},
					},
				},
			},
		}
		handler := NewEventHandler()
		err := handler.HandleEvent(evs.Event{
			Kind:      utils.CaeConfigEventKind,
			EventType: evs.Added,
			Object:    "",
		})
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(pool.Deployment.Spec.Template.Spec.Containers[0].Image, convey.ShouldEqual, "")
	})
	convey.Convey("Test_CaeConfigHandler CaeConfigHandler unmarshal etcd3.Event failed", t, func() {
		pool := &poolmanager.Pool{
			Deployment: &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{{
								Name: utils.RuntimeContainerName,
							}},
						},
					},
				},
			},
		}
		handler := NewEventHandler()
		bytes, _ := json.Marshal(&config.CaeConfig{
			CaeBaseImage: "aprilyolies",
		})
		bytes = append(bytes, 'x')
		err := handler.HandleEvent(evs.Event{
			Kind:      utils.CaeConfigEventKind,
			EventType: evs.Added,
			Object: &etcd3.Event{
				Value: bytes,
			},
		})
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(pool.Deployment.Spec.Template.Spec.Containers[0].Image, convey.ShouldEqual, "")
	})
	convey.Convey("Test_CaeConfigHandler CaeConfigHandler update deployment failed", t, func() {
		pm := &poolmanager.PoolMgr{}
		pool := &poolmanager.Pool{
			Deployment: &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{{
								Name: utils.RuntimeContainerName,
							}},
						},
					},
				},
			},
		}
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(poolmanager.GetPoolManager, func() *poolmanager.PoolMgr {
				return pm
			}),
			gomonkey.ApplyFunc(utils.UpdateRuntimeImageByK8s, func(deployName string, image string) error {
				return errors.New("CaeConfigHandler update deployment failed")
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(pm), "GetPoolList", func(p *poolmanager.PoolMgr) map[string]*poolmanager.Pool {
				return map[string]*poolmanager.Pool{
					"cae-300-128": pool,
				}
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(pool), "IsCaePool", func(p *poolmanager.Pool) bool {
				return true
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		handler := NewEventHandler()
		bytes, _ := json.Marshal(&config.CaeConfig{
			CaeBaseImage: "aprilyolies",
		})
		err := handler.HandleEvent(evs.Event{
			Kind:      utils.CaeConfigEventKind,
			EventType: evs.Added,
			Object: &etcd3.Event{
				Value: bytes,
			},
		})
		convey.So(err, convey.ShouldBeNil)
		convey.So(pool.Deployment.Spec.Template.Spec.Containers[0].Image, convey.ShouldEqual, "")
	})
}
