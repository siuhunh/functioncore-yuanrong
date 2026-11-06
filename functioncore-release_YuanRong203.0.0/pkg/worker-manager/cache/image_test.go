/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

package cache

import (
	"context"
	"errors"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"

	"functioncore/pkg/common/types"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
	"functioncore/pkg/worker-manager/utils"
	mockUtils "functioncore/test/utils"
)

var fakeImageManagerPool, _ = ants.NewPool(defaultImageCacheCurrency, ants.WithPreAlloc(true))

func TestImageCache_createJobSpec(t *testing.T) {
	ic := &ImageCache{
		funcSvcID: "funcA",
		funcMeta: &faasfunction.FunctionService{
			Spec: faasfunction.FunctionServiceSpec{
				Configuration: faasfunction.ConfigurationSpec{
					CustomContainerConfig: types.CustomContainerConfig{Image: "test_image"},
				},
			},
		},
		crSignature: "111",
		namespace:   "default",
		secretName:  "cache-secret-111",
	}

	job := ic.createJobSpec("target_node")

	assert.Equal(t, "test_image", job.Spec.Template.Spec.Containers[0].Image)
	assert.Equal(t, "target_node", job.Spec.Template.Spec.NodeSelector["kubernetes.io/hostname"])
	assert.Equal(t, "cache-secret-111", job.Spec.Template.Spec.ImagePullSecrets[2].Name)
}

func TestImageCache_generateSecretData(t *testing.T) {
	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc(utils.CreateSecretDataWithUserAuth, func(userEnc, pwdEnc, image string) ([]byte, error) {
			if userEnc == "" {
				return nil, errors.New("failed to create")
			}
			return []byte("some data"), nil
		}),
		gomonkey.ApplyFunc(utils.RequestSWR, func(ctx context.Context, path, method string, agency utils.UserAgency,
			extra utils.SWRErrRespHandlers) ([]byte, error) {
			return []byte("some data"), nil
		}),
		gomonkey.ApplyMethod(reflect.TypeOf(&sync.Once{}), "Do", func(_ *sync.Once, f func()) {}),
	}

	defer func() {
		for i := range patches {
			patches[i].Reset()
		}
		disableSWRAddon.disabled = false
	}()

	tests := []struct {
		name        string
		useUserAuth bool
		user        string
		pwd         string
		disableSWR  bool

		expectedErr   bool
		expectedEmpty bool
	}{
		{
			name:        "use user auth but user and pwd empty",
			useUserAuth: true,
			user:        "",
			pwd:         "",
			disableSWR:  false,

			expectedErr:   false,
			expectedEmpty: true,
		},
		{
			name:        "use user auth and create failed",
			useUserAuth: true,
			user:        "",
			pwd:         "some pwd",
			disableSWR:  false,

			expectedErr: true,
		},
		{
			name:       "no user auth, with swr disabled",
			disableSWR: true,

			expectedErr:   false,
			expectedEmpty: true,
		},
		{
			name:       "no user auth, with swr enable",
			disableSWR: false,

			expectedErr:   false,
			expectedEmpty: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			disableSWRAddon.disabled = tt.disableSWR
			meta := &faasfunction.FunctionService{
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						Role: faasfunction.Role{AppXRole: "app1"},
						CustomContainerConfig: types.CustomContainerConfig{
							UseUserAuth: tt.useUserAuth,
							User:        tt.user,
							Pwd:         tt.pwd,
						},
					},
				},
			}
			ic := &ImageCache{
				funcMeta: meta,
			}
			data, err := ic.generateSecretData()
			if tt.expectedErr {
				assert.Error(t, err, tt.name)
				assert.Nil(t, data, tt.name)
			} else {
				assert.Nil(t, err, tt.name)
				assert.Equal(t, tt.expectedEmpty, len(data) == 0, tt.name)
			}
		})
	}
}

func TestImageCache_createSecretSpec(t *testing.T) {
	imageCache := &ImageCache{}

	patches := []*gomonkey.Patches{
		gomonkey.ApplyPrivateMethod(reflect.TypeOf(&ImageCache{}), "generateSecretData", func() ([]byte, error) {
			if imageCache.funcSvcID == "generate_data_error" {
				return nil, errors.New("failed to generate data")
			}
			if imageCache.funcSvcID == "empty_data" {
				return []byte{}, nil
			}
			if imageCache.funcSvcID == "nil_data" {
				return nil, nil
			}
			return []byte("some_data"), nil
		}),
	}

	defer func() {
		for i := range patches {
			patches[i].Reset()
		}
	}()

	tests := []struct {
		name       string
		secretName string
		funcSvcID  string

		expectedErr  bool
		expectedData map[string][]byte
		expectedType corev1.SecretType
	}{
		{
			name:        "create data error",
			funcSvcID:   "generate_data_error",
			expectedErr: true,
		},
		{
			name:         "empty data",
			funcSvcID:    "empty_data",
			expectedErr:  false,
			expectedData: map[string][]byte{},
			expectedType: corev1.SecretTypeOpaque,
		},
		{
			name:         "nil data",
			funcSvcID:    "nil_data",
			secretName:   "cache-secret-111",
			expectedErr:  false,
			expectedData: map[string][]byte{},
			expectedType: corev1.SecretTypeOpaque,
		},
		{
			name:         "some data",
			funcSvcID:    "function1",
			expectedErr:  false,
			expectedData: map[string][]byte{corev1.DockerConfigJsonKey: []byte("some_data")},
			expectedType: corev1.SecretTypeDockerConfigJson,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			imageCache.secretName = tt.secretName
			imageCache.funcSvcID = tt.funcSvcID

			secret, err := imageCache.createSecretSpec()
			if tt.expectedErr {
				assert.Error(t, err, tt.name)
				assert.Nil(t, secret, tt.name)
			} else {
				assert.Nil(t, err, tt.name)
				assert.Equal(t, tt.expectedData, secret.Data, tt.name)
				assert.Equal(t, tt.expectedType, secret.Type, tt.name)
			}
		})
	}
}

func TestImageManager_create_delete_job(t *testing.T) {
	kubeClient := fake.NewSimpleClientset()
	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)
	jobInformer := informerFactory.Batch().V1().Jobs()
	secretInformer := informerFactory.Core().V1().Secrets()

	im, err := NewImageManager("tag", kubeClient, jobInformer, secretInformer)
	assert.Nil(t, err)

	assert.Nil(t, im.deleteJob(""), "delete job with empty name")

	jobSpec := &batchv1.Job{}
	jobSpec.Name = "test-job"

	assert.Error(t, im.deleteJob(jobSpec.Name), "delete non-exist job")

	_, err = im.createJob(jobSpec)
	assert.Nil(t, err, "create job")

	assert.Nil(t, im.deleteJob(jobSpec.Name), "delete exist job")
}

func TestNewImageManager_JobHandler(t *testing.T) {
	kubeClient := fake.NewSimpleClientset()
	funcSvcID := "func001"
	ic := &ImageCache{
		funcSvcID: funcSvcID,
		jobs:      map[string]string{},
		funcMeta: &faasfunction.FunctionService{
			Spec: faasfunction.FunctionServiceSpec{
				Configuration: faasfunction.ConfigurationSpec{
					CustomContainerConfig: types.CustomContainerConfig{Image: "test_image"},
				},
			},
		},
		crSignature: "111",
	}
	im := &ImageManager{
		kubeClient: kubeClient,
		pool:       fakeImageManagerPool,
	}
	im.imageCaches.Store(funcSvcID, ic)

	handler := im.generateJobHandler().(cache.FilteringResourceEventHandler)

	// test filters
	handler.OnAdd("", true)
	handler.OnAdd(&batchv1.Job{}, true)
	handler.FilterFunc = func(obj interface{}) bool { return true }
	handler.OnAdd("", true)
	handler.OnUpdate("", "")
	handler.OnDelete("")

	assert.Equal(t, 0, len(ic.jobs))

	targetNode := "test_node"
	job := ic.createJobSpec(targetNode)
	job.Name = "cache-job-abc"

	// test job add or update
	deleteJob := job.DeepCopy()
	deleteJob.DeletionTimestamp = &metav1.Time{}
	handler.OnUpdate(deleteJob, deleteJob)
	assert.Equal(t, 0, len(ic.jobs))

	completeJob := job.DeepCopy()
	completeJob.Status.Conditions = append(completeJob.Status.Conditions, batchv1.JobCondition{
		Status: corev1.ConditionTrue,
		Type:   batchv1.JobComplete,
	})
	handler.OnAdd(completeJob, true)
	assert.Equal(t, 0, len(ic.jobs))

	failedJob := job.DeepCopy()
	failedJob.Status.Conditions = append(failedJob.Status.Conditions, batchv1.JobCondition{
		Status: corev1.ConditionTrue,
		Type:   batchv1.JobFailed,
	})
	handler.OnAdd(failedJob, true)
	assert.Equal(t, 0, len(ic.jobs))

	otherFuncJob := job.DeepCopy()
	otherFuncJob.Annotations["funcSvcID"] = "func002"
	handler.OnAdd(otherFuncJob, true)
	assert.Equal(t, 0, len(ic.jobs))

	signatureChangedJob := job.DeepCopy()
	signatureChangedJob.Annotations["crSignature"] = "222"
	handler.OnAdd(signatureChangedJob, true)
	assert.Equal(t, 0, len(ic.jobs))

	handler.OnAdd(job, true)
	assert.Equal(t, 1, len(ic.jobs))
	assert.Equal(t, job.Name, ic.jobs[targetNode])

	// test job delete
	handler.OnDelete(otherFuncJob)
	assert.Equal(t, 1, len(ic.jobs))
	assert.Equal(t, job.Name, ic.jobs[targetNode])

	handler.OnDelete(signatureChangedJob)
	assert.Equal(t, 1, len(ic.jobs))
	assert.Equal(t, job.Name, ic.jobs[targetNode])

	otherJob := job.DeepCopy()
	otherJob.Name = "testjob"
	handler.OnDelete(otherJob)
	assert.Equal(t, 1, len(ic.jobs))
	assert.Equal(t, job.Name, ic.jobs[targetNode])

	otherNodeJob := job.DeepCopy()
	otherNodeJob.Annotations["node"] = "node002"
	handler.OnDelete(otherNodeJob)
	assert.Equal(t, 1, len(ic.jobs))
	assert.Equal(t, job.Name, ic.jobs[targetNode])

	handler.OnDelete(job)
	assert.Equal(t, 1, len(ic.jobs))
	assert.Equal(t, "", ic.jobs[targetNode])
}

func TestNewImageManager_SecretHandler(t *testing.T) {
	kubeClient := fake.NewSimpleClientset()
	funcSvcID := "func001"
	ic := &ImageCache{
		funcSvcID: funcSvcID,
		funcMeta: &faasfunction.FunctionService{
			Spec: faasfunction.FunctionServiceSpec{
				Configuration: faasfunction.ConfigurationSpec{
					CustomContainerConfig: types.CustomContainerConfig{
						UseUserAuth: true,
					},
				},
			},
		},
	}
	im := &ImageManager{
		kubeClient: kubeClient,
		pool:       fakeImageManagerPool,
	}
	im.imageCaches.Store(funcSvcID, ic)

	secret, err := ic.createSecretSpec()
	assert.Nil(t, err)
	secret.Name = "cache-secret-001"

	handler := im.generateSecretHandler().(cache.FilteringResourceEventHandler)

	handler.OnAdd("", true)
	handler.OnAdd(&corev1.Secret{}, true)
	handler.FilterFunc = func(obj interface{}) bool { return true }
	handler.OnAdd("", true)
	handler.OnUpdate("", "")
	handler.OnDelete("")

	assert.Equal(t, "", ic.secretName)

	deleteSecret := secret.DeepCopy()
	deleteSecret.DeletionTimestamp = &metav1.Time{}
	handler.OnUpdate(deleteSecret, deleteSecret)
	assert.Equal(t, "", ic.secretName)

	otherSecret := secret.DeepCopy()
	otherSecret.Annotations["funcSvcID"] = "func002"
	handler.OnAdd(otherSecret, true)
	assert.Equal(t, "", ic.secretName)

	handler.OnAdd(secret, true)
	assert.Equal(t, secret.Name, ic.secretName)

	secret.Name = "cache-secret-002"
	handler.OnAdd(secret, true)
	assert.Equal(t, secret.Name, ic.secretName)

	handler.OnDelete(otherSecret)
	assert.Equal(t, secret.Name, ic.secretName)

	handler.OnDelete(secret)
	assert.Equal(t, "", ic.secretName)
}

func TestImageManager_UpdateImageCache(t *testing.T) {
	im := &ImageManager{
		kubeClient: fake.NewSimpleClientset(),
		pool:       fakeImageManagerPool,
	}
	patches := []*gomonkey.Patches{
		gomonkey.ApplyPrivateMethod(reflect.TypeOf(&ImageManager{}), "cacheImages", func(ic *ImageCache, targetNodes []string) {
			return
		}),
	}

	defer func() {
		for i := range patches {
			patches[i].Reset()
		}
	}()

	tests := []struct {
		name string

		funcSvcID   string
		crSignature string
		funcMeta    *faasfunction.FunctionService
		targetNodes []string

		expectedCached bool
	}{
		{
			name:     "funcMeta is nil",
			funcMeta: nil,

			expectedCached: false,
		},
		{
			name:        "create new cache",
			funcSvcID:   "func001",
			crSignature: "1",
			funcMeta:    &faasfunction.FunctionService{},
			targetNodes: []string{"node1", "node2"},

			expectedCached: true,
		},
		{
			name:        "signature not changed",
			funcSvcID:   "func001",
			crSignature: "1",
			funcMeta:    &faasfunction.FunctionService{},
			targetNodes: []string{"node1", "node2"},

			expectedCached: true,
		},
		{
			name:        "func updated",
			funcSvcID:   "func001",
			crSignature: "2",
			funcMeta:    &faasfunction.FunctionService{},
			targetNodes: []string{"node2", "node3"},

			expectedCached: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			im.updateImageCache(tt.funcSvcID, tt.crSignature, tt.funcMeta, tt.targetNodes)
			icIf, ok := im.imageCaches.Load(tt.funcSvcID)
			assert.Equal(t, tt.expectedCached, ok)
			if ok {
				ic := icIf.(*ImageCache)
				jobs := make(map[string]string)
				for i := range tt.targetNodes {
					jobs[tt.targetNodes[i]] = ""
				}
				assert.Equal(t, jobs, ic.jobs)
			}
		})
	}
	count := 0
	im.imageCaches.Range(func(key, value any) bool {
		count++
		return true
	})
	im.imageNodeCached = map[string]time.Time{
		"node5": time.Now(),
	}
	assert.NotZero(t, count)
	assert.NotZero(t, len(im.imageNodeCached))

	im.Disable()
	count = 0
	im.imageCaches.Range(func(key, value any) bool {
		count++
		return true
	})
	assert.Zero(t, count)
	assert.Zero(t, len(im.imageNodeCached))
	im.Enable()
}

func TestImageManager_cacheImages(t *testing.T) {
	tests := []struct {
		name        string
		ic          *ImageCache
		targetNodes []string

		expectedJobs int
	}{
		{
			name:         "no target nodes",
			ic:           &ImageCache{jobs: map[string]string{}},
			targetNodes:  nil,
			expectedJobs: 0,
		},
		{
			name: "failed to create secret",
			ic: &ImageCache{
				namespace: "default",
				jobs:      map[string]string{},
				funcMeta: &faasfunction.FunctionService{
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							CustomContainerConfig: types.CustomContainerConfig{
								UseUserAuth: true,
								User:        "test",
							},
						},
					},
				},
			},
			targetNodes:  []string{"node1", "node2"},
			expectedJobs: 0,
		},
		{
			name: "failed to create or update secret",
			ic: &ImageCache{
				namespace:  "default",
				jobs:       map[string]string{},
				secretName: "non-secret",
				funcMeta: &faasfunction.FunctionService{
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							CustomContainerConfig: types.CustomContainerConfig{
								UseUserAuth: true,
							},
						},
					},
				},
			},
			targetNodes:  []string{"node1", "node2"},
			expectedJobs: 0,
		},
		{
			name: "succeed to create new secret",
			ic: &ImageCache{
				namespace: "default",
				jobs:      map[string]string{},
				funcMeta: &faasfunction.FunctionService{
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							CustomContainerConfig: types.CustomContainerConfig{
								UseUserAuth: true,
							},
						},
					},
				},
			},
			targetNodes:  []string{"node1", "node2"},
			expectedJobs: 2,
		},
		{
			name: "succeed to recreate secret",
			ic: &ImageCache{
				jobs:       map[string]string{},
				secretName: "cache-secret-003",
				funcMeta: &faasfunction.FunctionService{
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							CustomContainerConfig: types.CustomContainerConfig{
								UseUserAuth: true,
							},
						},
					},
				},
			},
			targetNodes:  []string{"node1", "node2"},
			expectedJobs: 2,
		},
		{
			name: "succeed to update secret",
			ic: &ImageCache{
				jobs:       map[string]string{},
				secretName: "cache-secret-002",
				funcMeta: &faasfunction.FunctionService{
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							CustomContainerConfig: types.CustomContainerConfig{
								UseUserAuth: true,
							},
						},
					},
				},
			},
			targetNodes:  []string{"node1", "node2"},
			expectedJobs: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			curSecrets := []runtime.Object{
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "cache-secret-001",
						Namespace: "default",
					},
					Type: corev1.SecretTypeOpaque,
				},
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "cache-secret-002",
						Namespace: "default",
					},
					Type: corev1.SecretTypeOpaque,
				},
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "cache-secret-003",
						Namespace: "default",
					},
					Type: corev1.SecretTypeDockercfg,
				},
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			kubeClient := fake.NewSimpleClientset(curSecrets...)
			informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)
			secretInformer := informerFactory.Core().V1().Secrets()
			go secretInformer.Informer().Run(ctx.Done())
			cache.WaitForCacheSync(nil, secretInformer.Informer().HasSynced)

			im := &ImageManager{
				kubeClient:      kubeClient,
				secretInformer:  secretInformer,
				namespace:       "default",
				pool:            fakeImageManagerPool,
				imageNodeCached: map[string]time.Time{},
			}

			im.cacheImages(tt.ic, tt.targetNodes)
			assert.Equal(t, tt.expectedJobs, len(tt.ic.jobs), tt.name)
		})
	}
}

func TestImageManager_DeleteImageCache(t *testing.T) {
	curJobs := []runtime.Object{
		&batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name: "cache-job-001",
			},
		},
		&batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name: "cache-job-002",
			},
		},
		&batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name: "cache-job-003",
			},
		},
	}

	ic := &ImageCache{
		secretName: "cache-secret-001",
		jobs: map[string]string{
			"node0": "cache-job-000",
			"node1": "cache-job-001",
			"node2": "cache-job-002",
		},
	}

	funcSvcID := "func001"
	im := &ImageManager{
		kubeClient: fake.NewSimpleClientset(curJobs...),
		pool:       fakeImageManagerPool,
	}
	im.imageCaches.Store(funcSvcID, ic)
	im.imageCaches.Store("not_func", "")

	im.deleteImageCache("func002")
	im.deleteImageCache("not_func")
	im.deleteImageCache(funcSvcID)
}

func Test_parseImageRefreshInterval(t *testing.T) {
	defer os.Unsetenv("IMAGE_REFRESH_INTERVAL")

	tests := []struct {
		name     string
		env      string
		expected time.Duration
	}{
		{
			name:     "env empty",
			env:      "",
			expected: defaultImageRefreshInterval,
		},
		{
			name:     "zero",
			env:      "0",
			expected: time.Duration(0),
		},
		{
			name:     "invalid value",
			env:      "dfasdf",
			expected: defaultImageRefreshInterval,
		},
		{
			name:     "some valid",
			env:      "300",
			expected: 300 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Setenv("IMAGE_REFRESH_INTERVAL", tt.env)
			assert.Equal(t, tt.expected, parseImageRefreshInterval(), tt.name)
		})
	}
}

func Test_parseImageCacheChanSize(t *testing.T) {
	defer os.Unsetenv("IMAGE_CACHE_CURRENCY")

	tests := []struct {
		name     string
		env      string
		expected int
	}{
		{
			name:     "env empty",
			env:      "",
			expected: defaultImageCacheCurrency,
		},
		{
			name:     "zero",
			env:      "0",
			expected: defaultImageCacheCurrency,
		},
		{
			name:     "invalid value",
			env:      "dfasdf",
			expected: defaultImageCacheCurrency,
		},
		{
			name:     "some valid",
			env:      "300",
			expected: 300,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Setenv("IMAGE_CACHE_CURRENCY", tt.env)
			assert.Equal(t, tt.expected, parseImageCacheCurrency(), tt.name)
		})
	}
}

func TestImageManager_initImageNodeCache(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	date1 := time.Date(2024, 8, 4, 0, 0, 0, 0, time.UTC)

	curJobs := []runtime.Object{
		&batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				CreationTimestamp: metav1.NewTime(date1),
				Name:              "cache-job-001",
				Namespace:         "default",
				Labels: map[string]string{
					"app": appImageCache,
				},
				Annotations: map[string]string{
					"node": "node1",
				},
			},
			Spec: batchv1.JobSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Image: "image1"}}},
				},
			},
		},
		&batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				CreationTimestamp: metav1.NewTime(time.Date(2024, 8, 4, 0, 0, 0, 0, time.UTC)),
				Name:              "no_label",
				Namespace:         "default",
				Annotations: map[string]string{
					"node": "node1",
				},
			},
			Spec: batchv1.JobSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Image: "image1"}}},
				},
			},
		},
		&batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "empty_creation_timestamp",
				Namespace: "default",
				Labels: map[string]string{
					"app": appImageCache,
				},
				Annotations: map[string]string{
					"node": "node1",
				},
			},
			Spec: batchv1.JobSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Image: "image1"}}},
				},
			},
		},
	}

	expected := map[string]time.Time{
		"node1@image1": date1,
	}

	kubeClient := fake.NewSimpleClientset(curJobs...)
	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)
	jobInformer := informerFactory.Batch().V1().Jobs()
	go jobInformer.Informer().Run(ctx.Done())
	cache.WaitForCacheSync(nil, jobInformer.Informer().HasSynced)

	im := &ImageManager{
		kubeClient:      kubeClient,
		jobInformer:     jobInformer,
		namespace:       "default",
		imageNodeCached: map[string]time.Time{},
	}

	im.initImageNodeCache()
	assert.Equal(t, expected, im.imageNodeCached)
}

func Test_getTimeoutDuration(t *testing.T) {
	timeout := int64(1)
	tests := []struct {
		name        string
		want        int64
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 succeed to Get timeout",
			1,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig,
						func() (*config.FaasConfig, error) {
							return &config.FaasConfig{PoolMgrConfig: config.PoolMgrConfig{CustomContainerConfig: config.CustomContainerConfig{WaitReadyTimeout: &timeout}}}, nil
						}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		patch := tt.patchesFunc()
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, getTimeoutDuration(), "getTimeoutDuration()")
		})
		patch.ResetAll()
	}
}
