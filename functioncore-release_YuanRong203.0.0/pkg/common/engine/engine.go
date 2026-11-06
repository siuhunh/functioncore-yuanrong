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

// Package engine is the abstract client of container engine
package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/containerd/containerd"
	"github.com/containerd/containerd/namespaces"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/events"
	"github.com/docker/docker/client"

	"functioncore/pkg/common/logger/log"
)

const (
	devicemapperType      = "devicemapper"
	overlay2Type          = "overlay2"
	ctrSocketPath         = "/var/run/containerd/containerd.sock"
	defaultCtrNamespace   = "k8s.io"
	containerNameLabelKey = "io.kubernetes.container.name"
	podNameLabelKey       = "io.kubernetes.pod.name"
	// CtrDefaultNS -
	CtrDefaultNS = "k8s.io"
	// CtrTaskPath -
	CtrTaskPath = "/run/containerd/io.containerd.runtime.v2.task"
	// CtrStdLogPath -
	CtrStdLogPath = "/var/lib/containerd/container_logs"
)

// ContainerClient client for container interface
type ContainerClient interface {
	Info(ctx context.Context, containerID string) (ContainerInfo, error)
	Kill(ctx context.Context, containerID string) error
	Close() error
}

// ContainerInfo -
type ContainerInfo struct {
	ID            string
	ImageName     string
	ContainerName string
	RootFsPath    string
}

// NewContainerClient get a new container client
func NewContainerClient() (ContainerClient, error) {
	if IsContainerd() {
		return newCtrClient()
	}
	return newDockerClient()
}

// CtrClient -
type CtrClient struct {
	cli *containerd.Client
}

// Info -
func (c *CtrClient) Info(ctx context.Context, containerID string) (ContainerInfo, error) {
	ctx = namespaces.WithNamespace(ctx, CtrDefaultNS)
	container, err := c.cli.LoadContainer(ctx, containerID)
	if err != nil {
		log.GetLogger().Errorf("failed to load container: %s, %s", containerID, err.Error())
		return ContainerInfo{}, err
	}
	rootFsPath, err := c.GetRootFS(containerID)
	if err != nil {
		log.GetLogger().Errorf("failed to get rootfs container: %s, %s", containerID, err.Error())
		return ContainerInfo{}, err
	}
	if rootFsPath == "" {
		err = fmt.Errorf("failed to get rootfs for containerd")
		log.GetLogger().Errorf(err.Error())
		return ContainerInfo{}, err
	}
	var id, imageName, containerName string
	image, _ := container.Image(ctx)
	labels, _ := container.Labels(ctx)
	id = container.ID()
	if image != nil {
		imageName = image.Name()
	}
	if labels != nil {
		containerName = fmt.Sprintf("%s-%s", labels[podNameLabelKey], labels[containerNameLabelKey])
	}
	return ContainerInfo{
		ID:            id,
		ImageName:     imageName,
		ContainerName: containerName,
		RootFsPath:    rootFsPath,
	}, nil
}

// GetRootFS -
func (c *CtrClient) GetRootFS(containerID string) (string, error) {
	var rootFs string
	rootFs = filepath.Join(CtrTaskPath, defaultCtrNamespace, containerID, "rootfs")
	dir, err := os.Stat(rootFs)
	if err != nil {
		return "", err
	}
	if dir.IsDir() {
		return rootFs, nil
	}
	return "", nil
}

// Kill -
func (c *CtrClient) Kill(ctx context.Context, containerID string) error {
	ctx = namespaces.WithNamespace(ctx, CtrDefaultNS)
	container, err := c.cli.LoadContainer(ctx, containerID)
	if err != nil {
		log.GetLogger().Errorf("failed to load container when deleting: %s, %s", containerID, err.Error())
		return err
	}
	task, err := container.Task(ctx, nil)
	if err != nil {
		log.GetLogger().Errorf("failed to load container task, container ID: %s, %s", containerID, err.Error())
		return err
	}
	taskID := task.ID()
	_, err = task.Delete(ctx, containerd.WithProcessKill)
	if err != nil {
		log.GetLogger().Errorf("failed to kill container: %s, %s", containerID, err.Error())
		return err
	}
	log.GetLogger().Infof("success to kill container task, task ID: %s", taskID)
	return nil
}

// Close -
func (c *CtrClient) Close() error {
	return c.cli.Close()
}

func newCtrClient() (ContainerClient, error) {
	cli, err := containerd.New(ctrSocketPath)
	if err != nil {
		log.GetLogger().Errorf("failed to create containerd client: %s", err.Error())
		return nil, fmt.Errorf("failed to create containerd client: %s", err.Error())
	}
	return &CtrClient{cli: cli}, nil
}

// DockerClient -
type DockerClient struct {
	cli *client.Client
}

// Info -
func (c *DockerClient) Info(ctx context.Context, containerID string) (ContainerInfo, error) {
	containerJSON, err := c.cli.ContainerInspect(ctx, containerID)
	if err != nil {
		log.GetLogger().Errorf("failed to inspect runtime container: %s, %s", containerID, err.Error())
		return ContainerInfo{}, err
	}

	var rootFs string
	switch containerJSON.Driver {
	case devicemapperType:
		deviceName := containerJSON.GraphDriver.Data["DeviceName"]
		if deviceName == "" {
			break
		}
		tokens := strings.Split(deviceName, "-")
		uid := tokens[len(tokens)-1]
		rootFs = filepath.Join("/var/lib/docker/devicemapper/mnt", uid, "rootfs")
	case overlay2Type:
		rootFs = containerJSON.GraphDriver.Data["MergedDir"]
	default:
	}
	if rootFs == "" {
		err = fmt.Errorf("failed to get rootfs for docker")
		log.GetLogger().Errorf(err.Error())
		return ContainerInfo{}, err
	}
	return ContainerInfo{
		ID:            containerJSON.ID,
		ImageName:     containerJSON.Image,
		ContainerName: containerJSON.Name,
		RootFsPath:    rootFs,
	}, nil
}

// Kill -
func (c *DockerClient) Kill(ctx context.Context, containerID string) error {
	if err := c.cli.ContainerKill(ctx, containerID, "SIGKILL"); err != nil {
		log.GetLogger().Errorf("failed to kill container: %s, %s", containerID, err.Error())
		return err
	}
	return nil
}

// Close -
func (c *DockerClient) Close() error {
	return c.cli.Close()
}

func newDockerClient() (ContainerClient, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		log.GetLogger().Errorf("failed to create docker client: %s", err.Error())
		return nil, fmt.Errorf("failed to create docker client: %s", err.Error())
	}
	cli.NegotiateAPIVersion(context.TODO())
	return &DockerClient{cli: cli}, nil
}

// IsContainerd -
func IsContainerd() bool {
	fileInfo, err := os.Stat(ctrSocketPath)
	if err != nil && os.IsNotExist(err) {
		return false
	}
	if fileInfo == nil {
		return false
	}
	if fileInfo.IsDir() {
		return false
	}
	return true
}

// PullPrivateImage -
func (c *DockerClient) PullPrivateImage(ctx context.Context, imageName, authStr string) error {
	_, err := c.cli.ImagePull(ctx, imageName, types.ImagePullOptions{RegistryAuth: authStr})
	if err != nil {
		log.GetLogger().Warnf("failed to pull image, imageName: %s, err: %s", imageName, err.Error())
		return err
	}
	return nil
}

// Event -
func (c *DockerClient) Event(ctx context.Context) (<-chan events.Message, <-chan error) {
	return c.cli.Events(ctx, types.EventsOptions{})
}

// ImageInspectWithRaw -
func (c *DockerClient) ImageInspectWithRaw(ctx context.Context, imageName string) (types.ImageInspect, []byte, error) {
	return c.cli.ImageInspectWithRaw(ctx, imageName)
}

// ImageRemove -
func (c *DockerClient) ImageRemove(ctx context.Context, imageName string, removeOp types.ImageRemoveOptions) (
	[]types.ImageDeleteResponseItem, error) {
	return c.cli.ImageRemove(ctx, imageName, removeOp)
}

// NewTCPDockerClient docker client connect with tcp
func NewTCPDockerClient(imgDownloadNodeIP string) (*DockerClient, error) {
	cli, err := client.NewClientWithOpts(client.WithHost(fmt.Sprintf("tcp://%s:2375", imgDownloadNodeIP)))
	if err != nil {
		log.GetLogger().Errorf("failed to create tcp docker client: %s", err.Error())
		return nil, fmt.Errorf("failed to create tcp docker client: %s", err.Error())
	}
	cli.NegotiateAPIVersion(context.Background())
	return &DockerClient{cli: cli}, nil
}
