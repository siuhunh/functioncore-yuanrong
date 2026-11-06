/*
 * Copyright (c) 2023 Huawei Technologies Co., Ltd
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

package wmhealth

import (
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"

	"functioncore/pkg/common/k8sclient"
)

func Test_initLeaseInformer(t *testing.T) {
	addr1 := "10.0.0.1:58866"
	leases := []*v1.Lease{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "state-manager"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "worker-manager"},
			Spec:       v1.LeaseSpec{HolderIdentity: &addr1},
		},
	}

	fakeClient := fake.NewSimpleClientset(leases[0], leases[1])
	fakeInformer := informers.NewSharedInformerFactory(fakeClient, 0)

	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc(k8sclient.GetKubeConfig, func() (*rest.Config, error) {
			return &rest.Config{}, nil
		}),
		gomonkey.ApplyFunc(kubernetes.NewForConfig, func(*rest.Config) (*kubernetes.Clientset, error) {
			return &kubernetes.Clientset{}, nil
		}),
		gomonkey.ApplyFunc(informers.NewSharedInformerFactoryWithOptions, func(client kubernetes.Interface, defaultResync time.Duration, options ...informers.SharedInformerOption) informers.SharedInformerFactory {
			return fakeInformer
		}),
	}
	defer func() {
		for i := range patches {
			patches[i].Reset()
		}
	}()

	stopCh := make(chan struct{})
	err := initLeaseInformer(stopCh)
	assert.Nil(t, err)
	close(stopCh)
}

func Test_generateInformerHandler(t *testing.T) {
	addr1 := "10.0.0.1:58866"
	addr2 := "10.0.0.2:58866"
	addr3 := "10.0.0.3:58866"
	emptyLease := &v1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-manager"},
	}
	lease1 := &v1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-manager"},
		Spec:       v1.LeaseSpec{HolderIdentity: &addr1},
	}
	lease2 := &v1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-manager"},
		Spec:       v1.LeaseSpec{HolderIdentity: &addr2},
	}

	stateLease := &v1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: "state-manager"},
		Spec:       v1.LeaseSpec{HolderIdentity: &addr3},
	}

	updateWmAddr("")
	handlers := generateInformerHandler()
	handlers.OnAdd(*lease1, true)
	assert.Equal(t, "", wmAddr)
	handlers.OnAdd(emptyLease, true)
	assert.Equal(t, "", wmAddr)
	handlers.OnAdd(lease1, true)
	assert.Equal(t, addr1, wmAddr)
	handlers.OnAdd(stateLease, true)
	assert.Equal(t, addr1, wmAddr)

	filter := handlers.FilterFunc
	handlers.FilterFunc = func(obj interface{}) bool {
		return true
	}
	handlers.OnUpdate(lease1, *lease1)
	assert.Equal(t, addr1, wmAddr)
	handlers.OnUpdate(lease1, emptyLease)
	assert.Equal(t, addr1, wmAddr)
	handlers.OnUpdate(lease1, lease2)
	assert.Equal(t, addr2, wmAddr)

	handlers.FilterFunc = filter
	handlers.OnDelete(stateLease)
	assert.Equal(t, addr2, wmAddr)
	handlers.OnDelete(lease2)
	assert.Equal(t, "", wmAddr)
}
