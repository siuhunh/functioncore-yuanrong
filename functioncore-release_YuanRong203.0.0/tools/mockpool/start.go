package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// StartCmd -
type StartCmd struct {
	k8sClientsetGetter *K8sClientsetGetter
	tlsIniter          *TLSIniter
	deployName         string
	workerInitImage    string
	workerImage        string
	runtimeImage       string
	output             string
}

// NewStartCmd -
func NewStartCmd(k8sClientsetGetter *K8sClientsetGetter, tlsIniter *TLSIniter) *StartCmd {
	return &StartCmd{
		k8sClientsetGetter: k8sClientsetGetter,
		tlsIniter:          tlsIniter,
	}
}

// ApplyCobra -
func (s *StartCmd) ApplyCobra(cmd *cobra.Command) {
	flags := cmd.Flags()

	flags.StringVar(
		&s.deployName,
		"deploy-name",
		"",
		"从k8s deployment中获取",
	)

	flags.StringVar(
		&s.workerInitImage,
		"worker-init-image",
		"",
		"worker-init镜像名, 默认使用池子中的",
	)
	flags.StringVar(
		&s.workerImage,
		"worker-image",
		"",
		"worker镜像名, 默认使用池子中的",
	)
	flags.StringVar(
		&s.runtimeImage,
		"runtime-image",
		"",
		"runtime镜像名, 默认使用池子中的",
	)

	flags.StringVarP(
		&s.output,
		"output",
		"o",
		"",
		"不直接创建, 生成yaml文件",
	)
}

// Run -
func (s *StartCmd) Run() error {
	cs, err := s.k8sClientsetGetter.Get()
	if err != nil {
		return errors.Wrap(err, "get k8s clientset")
	}

	var deployment *appsv1.Deployment
	if s.deployName != "" {
		deployment, err = cs.AppsV1().Deployments("default").Get(context.Background(), s.deployName, metav1.GetOptions{})
		if err != nil {
			return errors.Wrap(err, "get deployment")
		}
	} else {
		return errors.New("poolName is empty")
	}

	if err := s.tlsIniter.Init(); err != nil {
		return errors.Wrap(err, "init tls")
	}

	pod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "mock" + deployment.Name + "-",
			Namespace:    "default",
			Annotations:  nil,
			Labels:       nil,
		},
		Spec: deployment.Spec.Template.Spec,
	}

	if s.workerInitImage != "" {
		fixContainer(pod.Spec.InitContainers, "init", s.workerInitImage)
	}
	if s.workerImage != "" {
		fixContainer(pod.Spec.Containers, "worker", s.workerImage)
	}
	if s.runtimeImage != "" {
		fixContainer(pod.Spec.Containers, "runtime", s.runtimeImage)
	}

	if s.output != "" {
		output, err := marshalYAML(pod)
		if err != nil {
			return errors.Wrap(err, "yaml marshal")
		}

		dir := filepath.Dir(s.output)
		if err := os.MkdirAll(dir, 0644); err != nil {
			return errors.Wrapf(err, "mkdir all %s", dir)
		}
		f, err := os.Create(filepath.Join(dir, filepath.Base(s.output)))
		if err != nil {
			return errors.Wrap(err, "create file")
		}

		_, err = fmt.Fprint(f, string(output))
		if err != nil {
			return errors.Wrapf(err, "write to file %s", f.Name())
		}

		return nil
	}

	p, err := cs.CoreV1().Pods("default").Create(context.Background(), pod, metav1.CreateOptions{})
	if err != nil {
		return errors.Wrap(err, "create pod")
	}

	fmt.Println("succeed to create pod:", p.Name)

	return nil
}

func fixContainer(containers []v1.Container, name, imageName string) {
	for i, container := range containers {
		if container.Name == name {
			containers[i].Image = imageName
		}
	}
}

// marshalYAML marshals the object into JSON then converts JSON to YAML and returns the YAML.
// This function mimics "sigs.k8s.io/yaml".Marshal.
func marshalYAML(o interface{}) ([]byte, error) {
	j, err := json.Marshal(o)
	if err != nil {
		return nil, errors.Wrap(err, "marshaling into JSON")
	}

	var jsonObj interface{}
	err = yaml.Unmarshal(j, &jsonObj)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshaling from JSON")
	}

	b, err := yaml.Marshal(jsonObj)
	if err != nil {
		return nil, errors.Wrap(err, "marshaling into YAML")
	}
	return b, nil
}
