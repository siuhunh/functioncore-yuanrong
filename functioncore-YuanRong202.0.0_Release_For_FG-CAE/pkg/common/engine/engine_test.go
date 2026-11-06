package engine

import (
	"context"
	"errors"
	"github.com/docker/docker/api/types/events"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/containerd/containerd"
	types2 "github.com/containerd/containerd/api/types"
	"github.com/containerd/containerd/cio"
	"github.com/containerd/containerd/containers"
	"github.com/containerd/containerd/oci"
	dockerTypes "github.com/docker/docker/api/types"
	"github.com/docker/docker/client"
	"github.com/gogo/protobuf/types"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/smartystreets/goconvey/convey"

	mockutils "functioncore/test/utils"
)

type mockContainer struct{}

func (m mockContainer) ID() string {
	return ""
}

func (m mockContainer) Info(ctx context.Context, opts ...containerd.InfoOpts) (containers.Container, error) {
	return containers.Container{}, nil
}

func (m mockContainer) Delete(ctx context.Context, opts ...containerd.DeleteOpts) error {
	return nil
}

func (m mockContainer) NewTask(ctx context.Context, creator cio.Creator, opts ...containerd.NewTaskOpts) (containerd.Task, error) {
	return mockTask{}, nil
}

func (m mockContainer) Spec(ctx context.Context) (*oci.Spec, error) {
	return nil, nil
}

func (m mockContainer) Task(ctx context.Context, attach cio.Attach) (containerd.Task, error) {
	return mockTask{}, nil
}

func (m mockContainer) Image(ctx context.Context) (containerd.Image, error) {
	return nil, nil
}

func (m mockContainer) Labels(ctx context.Context) (map[string]string, error) {
	return nil, nil
}

func (m mockContainer) SetLabels(ctx context.Context, m2 map[string]string) (map[string]string, error) {
	return nil, nil
}

func (m mockContainer) Extensions(ctx context.Context) (map[string]types.Any, error) {
	return nil, nil
}

func (m mockContainer) Update(ctx context.Context, opts ...containerd.UpdateContainerOpts) error {
	return nil
}

func (m mockContainer) Checkpoint(ctx context.Context, s string, opts ...containerd.CheckpointOpts) (containerd.Image, error) {
	return nil, nil
}

type mockTask struct{}

func (m mockTask) ID() string {
	return ""
}

func (m mockTask) Pid() uint32 {
	return 0
}

func (m mockTask) Start(ctx context.Context) error {
	return nil
}

func (m mockTask) Delete(ctx context.Context, opts ...containerd.ProcessDeleteOpts) (*containerd.ExitStatus, error) {
	return nil, nil
}

func (m mockTask) Kill(ctx context.Context, signal syscall.Signal, opts ...containerd.KillOpts) error {
	return nil
}

func (m mockTask) Wait(ctx context.Context) (<-chan containerd.ExitStatus, error) {
	return nil, nil
}

func (m mockTask) CloseIO(ctx context.Context, opts ...containerd.IOCloserOpts) error {
	return nil
}

func (m mockTask) Resize(ctx context.Context, w, h uint32) error {
	return nil
}

func (m mockTask) IO() cio.IO {
	return nil
}

func (m mockTask) Status(ctx context.Context) (containerd.Status, error) {
	return containerd.Status{}, nil
}

func (m mockTask) Pause(ctx context.Context) error {
	return nil
}

func (m mockTask) Resume(ctx context.Context) error {
	return nil
}

func (m mockTask) Exec(ctx context.Context, s string, process *specs.Process, creator cio.Creator) (containerd.Process, error) {
	return nil, nil
}

func (m mockTask) Pids(ctx context.Context) ([]containerd.ProcessInfo, error) {
	return nil, nil
}

func (m mockTask) Checkpoint(ctx context.Context, opts ...containerd.CheckpointTaskOpts) (containerd.Image, error) {
	return nil, nil
}

func (m mockTask) Update(ctx context.Context, opts ...containerd.UpdateTaskOpts) error {
	return nil
}

func (m mockTask) LoadProcess(ctx context.Context, s string, attach cio.Attach) (containerd.Process, error) {
	return nil, nil
}

func (m mockTask) Metrics(ctx context.Context) (*types2.Metric, error) {
	return nil, nil
}

func (m mockTask) Spec(ctx context.Context) (*oci.Spec, error) {
	return nil, nil
}

type mockFile struct {
	name  string
	idDir bool
}

func (m mockFile) Name() string {
	return m.name
}
func (m mockFile) Size() int64 {
	return 20
}
func (m mockFile) Mode() os.FileMode {
	return 2
}
func (m mockFile) ModTime() time.Time {
	return time.Time{}
}
func (m mockFile) IsDir() bool {
	return m.idDir
}
func (m mockFile) Sys() interface{} {
	return mockFile{}
}

func TestCtrClient_Info(t *testing.T) {
	ctx := context.TODO()
	containerID := "container_id"

	client := &CtrClient{}

	convey.Convey("Testing CtrClient Info", t, func() {
		convey.Convey("Fail case 1: LoadContainer failed", func() {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyMethod(reflect.TypeOf(&containerd.Client{}), "LoadContainer",
					func(_ *containerd.Client, ctx context.Context, containerID string) (containerd.Container, error) {
						return nil, errors.New("LoadContainer failed")
					}),
			})
			defer patches.ResetAll()
			info, err := client.Info(ctx, containerID)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(info.ID, convey.ShouldBeEmpty)
		})
		convey.Convey("Fail case 2: GetRootFS failed", func() {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyMethod(reflect.TypeOf(&containerd.Client{}), "LoadContainer",
					func(_ *containerd.Client, ctx context.Context, containerID string) (containerd.Container, error) {
						return nil, nil
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&CtrClient{}), "GetRootFS",
					func(_ *CtrClient, containerID string) (string, error) {
						return "", errors.New("GetRootFS failed")
					}),
			})
			defer patches.ResetAll()
			info, err := client.Info(ctx, containerID)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(info.ID, convey.ShouldBeEmpty)
		})
		convey.Convey("Fail case 3: GetRootFS failed", func() {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyMethod(reflect.TypeOf(&containerd.Client{}), "LoadContainer",
					func(_ *containerd.Client, ctx context.Context, containerID string) (containerd.Container, error) {
						return nil, nil
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&CtrClient{}), "GetRootFS",
					func(_ *CtrClient, containerID string) (string, error) {
						return "", nil
					}),
			})
			defer patches.ResetAll()
			info, err := client.Info(ctx, containerID)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "failed to get rootfs for containerd")
			convey.So(info.ID, convey.ShouldBeEmpty)
		})
		convey.Convey("Success case", func() {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyMethod(reflect.TypeOf(&containerd.Client{}), "LoadContainer",
					func(_ *containerd.Client, ctx context.Context, containerID string) (containerd.Container, error) {
						return mockContainer{}, nil
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&CtrClient{}), "GetRootFS",
					func(_ *CtrClient, containerID string) (string, error) {
						return "mock-rootfs", nil
					}),
				gomonkey.ApplyFunc(containerd.Container.Image, func(_ containerd.Container, ctx context.Context) (containerd.Image, error) {
					return nil, nil
				}),
				gomonkey.ApplyFunc(containerd.Container.Labels, func(_ containerd.Container, ctx context.Context) (map[string]string, error) {
					return nil, nil
				}),
			})
			defer patches.ResetAll()
			info, err := client.Info(ctx, containerID)
			convey.So(err, convey.ShouldBeNil)
			convey.So(info.RootFsPath, convey.ShouldEqual, "mock-rootfs")
		})
	})
}

func TestCtrClient_GetRootFS(t *testing.T) {
	containerID := "container_id"

	client := &CtrClient{}

	convey.Convey("Testing CtrClient GetRootFS", t, func() {
		convey.Convey("Fail case 1: err not nil", func() {
			patches := gomonkey.ApplyFunc(os.Stat, func(name string) (os.FileInfo, error) {
				return nil, errors.New("stat error")
			})
			defer patches.Reset()
			rootFS, err := client.GetRootFS(containerID)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(rootFS, convey.ShouldEqual, "")
		})
		convey.Convey("Fail case 2: is not dir", func() {
			patches := gomonkey.ApplyFunc(os.Stat, func(name string) (os.FileInfo, error) {
				return &mockFile{idDir: false}, nil
			})
			defer patches.Reset()
			rootFS, err := client.GetRootFS(containerID)
			convey.So(err, convey.ShouldBeNil)
			convey.So(rootFS, convey.ShouldEqual, "")
		})
		convey.Convey("success case", func() {
			patches := gomonkey.ApplyFunc(os.Stat, func(name string) (os.FileInfo, error) {
				return &mockFile{idDir: true}, nil
			})
			defer patches.Reset()
			rootFS, err := client.GetRootFS(containerID)
			convey.So(err, convey.ShouldBeNil)
			convey.So(rootFS, convey.ShouldEqual, filepath.Join(CtrTaskPath, CtrDefaultNS, containerID, "rootfs"))
		})
	})
}

func TestCtrClient_Kill(t *testing.T) {
	ctx := context.TODO()
	containerID := "container_id"
	client := &CtrClient{}
	convey.Convey("Testing CtrClient Kill", t, func() {
		convey.Convey("Fail case 1: LoadContainer failed", func() {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyMethod(reflect.TypeOf(&containerd.Client{}), "LoadContainer",
					func(_ *containerd.Client, ctx context.Context, containerID string) (containerd.Container, error) {
						return nil, errors.New("LoadContainer failed")
					}),
			})
			defer patches.ResetAll()
			err := client.Kill(ctx, containerID)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "LoadContainer failed")
		})
		convey.Convey("Fail case 2: get task failed", func() {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyMethod(reflect.TypeOf(&containerd.Client{}), "LoadContainer",
					func(_ *containerd.Client, ctx context.Context, containerID string) (containerd.Container, error) {
						return mockContainer{}, nil
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&mockContainer{}), "Task",
					func(_ *mockContainer, ctx context.Context, c cio.Attach) (containerd.Task, error) {
						return nil, errors.New("get task failed")
					}),
			})
			defer patches.ResetAll()
			err := client.Kill(ctx, containerID)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "get task failed")
		})
		convey.Convey("Fail case 3: kill task failed", func() {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyMethod(reflect.TypeOf(&containerd.Client{}), "LoadContainer",
					func(_ *containerd.Client, ctx context.Context, containerID string) (containerd.Container, error) {
						return mockContainer{}, nil
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&mockTask{}), "Delete",
					func(_ *mockTask, ctx context.Context, opt ...containerd.ProcessDeleteOpts) (*containerd.ExitStatus, error) {
						return nil, errors.New("kill task failed")
					}),
			})
			defer patches.ResetAll()
			err := client.Kill(ctx, containerID)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "kill task failed")
		})
		convey.Convey("Success case", func() {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyMethod(reflect.TypeOf(&containerd.Client{}), "LoadContainer",
					func(_ *containerd.Client, ctx context.Context, containerID string) (containerd.Container, error) {
						return mockContainer{}, nil
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&mockTask{}), "Delete",
					func(_ *mockTask, ctx context.Context, opt ...containerd.ProcessDeleteOpts) (*containerd.ExitStatus, error) {
						return nil, nil
					}),
			})
			defer patches.ResetAll()
			err := client.Kill(ctx, containerID)
			convey.So(err, convey.ShouldBeNil)
		})
	})
}

func TestDockerClient_Info(t *testing.T) {
	ctx := context.TODO()
	containerID := "container_id"

	cli := &DockerClient{}

	convey.Convey("Testing DockerClient Info", t, func() {
		convey.Convey("Fail case 1: inspect error", func() {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc((*client.Client).ContainerInspect,
					func(_ *client.Client, ctx context.Context, containerID string) (dockerTypes.ContainerJSON, error) {
						return dockerTypes.ContainerJSON{ContainerJSONBase: &dockerTypes.ContainerJSONBase{}}, errors.New("inspect error")
					}),
			})
			defer patches.ResetAll()
			_, err := cli.Info(ctx, containerID)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "inspect error")
		})
		convey.Convey("Fail case 2: empty rootfs", func() {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc((*client.Client).ContainerInspect,
					func(_ *client.Client, ctx context.Context, containerID string) (dockerTypes.ContainerJSON, error) {
						return dockerTypes.ContainerJSON{ContainerJSONBase: &dockerTypes.ContainerJSONBase{
							Driver:      "overlay2",
							GraphDriver: dockerTypes.GraphDriverData{Data: map[string]string{"MergedDir": ""}}}}, nil
					}),
			})
			defer patches.ResetAll()
			_, err := cli.Info(ctx, containerID)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "failed to get rootfs for docker")
		})
		convey.Convey("Success case 1: devicemapper case", func() {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc((*client.Client).ContainerInspect,
					func(_ *client.Client, ctx context.Context, containerID string) (dockerTypes.ContainerJSON, error) {
						return dockerTypes.ContainerJSON{ContainerJSONBase: &dockerTypes.ContainerJSONBase{
							Driver:      "devicemapper",
							GraphDriver: dockerTypes.GraphDriverData{Data: map[string]string{"DeviceName": "a-b-c"}}}}, nil
					}),
			})
			defer patches.ResetAll()
			info, err := cli.Info(ctx, containerID)
			convey.So(err, convey.ShouldBeNil)
			convey.So(info.RootFsPath, convey.ShouldEqual, filepath.Join("/var/lib/docker/devicemapper/mnt", "c", "rootfs"))
		})
		convey.Convey("Success case 2: overlay2 case", func() {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc((*client.Client).ContainerInspect,
					func(_ *client.Client, ctx context.Context, containerID string) (dockerTypes.ContainerJSON, error) {
						return dockerTypes.ContainerJSON{ContainerJSONBase: &dockerTypes.ContainerJSONBase{
							Driver:      "overlay2",
							ID:          containerID,
							GraphDriver: dockerTypes.GraphDriverData{Data: map[string]string{"MergedDir": "xxx"}}}}, nil
					}),
			})
			defer patches.ResetAll()
			info, err := cli.Info(ctx, containerID)
			convey.So(err, convey.ShouldBeNil)
			convey.So(info.RootFsPath, convey.ShouldEqual, "xxx")
			convey.So(info.ID, convey.ShouldEqual, containerID)
		})
	})
}

func TestDockerClient_Kill(t *testing.T) {
	ctx := context.TODO()
	containerID := "container_id"
	cli := &DockerClient{}
	convey.Convey("Testing DockerClient Kill", t, func() {
		convey.Convey("Fail case 1: Kill failed", func() {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc((*client.Client).ContainerKill,
					func(_ *client.Client, ctx context.Context, containerID string, signal string) error {
						return errors.New("kill container failed")
					}),
			})
			defer patches.ResetAll()
			err := cli.Kill(ctx, containerID)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "kill container failed")
		})
		convey.Convey("Success case", func() {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc((*client.Client).ContainerKill,
					func(_ *client.Client, ctx context.Context, containerID string, signal string) error {
						return nil
					}),
			})
			defer patches.ResetAll()
			err := cli.Kill(ctx, containerID)
			convey.So(err, convey.ShouldBeNil)
		})
	})
}

func TestNewContainerClient(t *testing.T) {
	convey.Convey("Testing NewContainerClient with Containerd", t, func() {
		convey.Convey("Success case", func() {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc(IsContainerd, func() bool {
					return true
				}),
				gomonkey.ApplyFunc(containerd.New,
					func(address string, opts ...containerd.ClientOpt) (*containerd.Client, error) {
						return &containerd.Client{}, nil
					}),
			})
			defer patches.ResetAll()
			containerClient, err := NewContainerClient()
			convey.So(err, convey.ShouldBeNil)
			convey.So(containerClient, convey.ShouldHaveSameTypeAs, &CtrClient{})
		})
		convey.Convey("Fail case: new containerd client failed", func() {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc(IsContainerd, func() bool {
					return true
				}),
				gomonkey.ApplyFunc(containerd.New,
					func(address string, opts ...containerd.ClientOpt) (*containerd.Client, error) {
						return nil, errors.New("xxx")
					}),
			})
			defer patches.ResetAll()
			containerClient, err := NewContainerClient()
			convey.So(containerClient, convey.ShouldBeNil)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "failed to create containerd client: xxx")
		})
	})

	convey.Convey("Testing NewContainerClient with Docker", t, func() {
		convey.Convey("Success case", func() {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc(IsContainerd, func() bool {
					return false
				}),
				gomonkey.ApplyFunc(client.NewClientWithOpts,
					func(ops ...func(*client.Client) error) (*client.Client, error) {
						return &client.Client{}, nil
					}),
				gomonkey.ApplyFunc((*client.Client).NegotiateAPIVersion,
					func(_ *client.Client, ctx context.Context) {
						return
					}),
			})
			defer patches.ResetAll()
			containerClient, err := NewContainerClient()
			convey.So(err, convey.ShouldBeNil)
			convey.So(containerClient, convey.ShouldHaveSameTypeAs, &DockerClient{})
		})
		convey.Convey("Fail case: new docker client failed", func() {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc(IsContainerd, func() bool {
					return false
				}),
				gomonkey.ApplyFunc(client.NewClientWithOpts,
					func(ops ...func(*client.Client) error) (*client.Client, error) {
						return nil, errors.New("xxx")
					}),
				gomonkey.ApplyFunc((*client.Client).NegotiateAPIVersion,
					func(_ *client.Client, ctx context.Context) {
						return
					}),
			})
			defer patches.ResetAll()
			containerClient, err := NewContainerClient()
			convey.So(containerClient, convey.ShouldBeNil)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "failed to create docker client: xxx")
		})
	})
}

type fakeFileInfo struct {
	isDir bool
}

func (fakeFileInfo) Name() string       { return "" }
func (fakeFileInfo) Size() int64        { return 0 }
func (fakeFileInfo) Mode() os.FileMode  { return 0750 }
func (fakeFileInfo) ModTime() time.Time { return time.Time{} }
func (f fakeFileInfo) IsDir() bool      { return f.isDir }
func (fakeFileInfo) Sys() interface{}   { return nil }

func TestIsContainerd(t *testing.T) {
	patches := gomonkey.ApplyFunc(os.Stat, func(name string) (os.FileInfo, error) {
		return nil, os.ErrNotExist
	})
	defer patches.Reset()

	convey.Convey("Testing IsContainerd when socket file does not exist", t, func() {
		isContainerd := IsContainerd()
		convey.So(isContainerd, convey.ShouldBeFalse)
	})

	patches = gomonkey.ApplyFunc(os.Stat, func(name string) (os.FileInfo, error) {
		return nil, os.ErrPermission
	})
	defer patches.Reset()

	convey.Convey("Testing IsContainerd when socket file does not have permission to stat", t, func() {
		isContainerd := IsContainerd()
		convey.So(isContainerd, convey.ShouldBeFalse)
	})

	patches = gomonkey.ApplyFunc(os.Stat, func(name string) (os.FileInfo, error) {
		return fakeFileInfo{isDir: true}, nil
	})
	defer patches.Reset()

	convey.Convey("Testing IsContainerd when socket file exists but is dir", t, func() {
		isContainerd := IsContainerd()
		convey.So(isContainerd, convey.ShouldBeFalse)
	})

	patches = gomonkey.ApplyFunc(os.Stat, func(name string) (os.FileInfo, error) {
		return fakeFileInfo{isDir: false}, nil
	})
	defer patches.Reset()
	convey.Convey("Testing IsContainerd when socket file exists and is a file", t, func() {
		isContainerd := IsContainerd()
		convey.So(isContainerd, convey.ShouldBeTrue)
	})
}

func Test_NewTCPDockerClient(t *testing.T) {
	patches := gomonkey.ApplyFunc(client.NewClientWithOpts,
		func(ops ...func(*client.Client) error) (*client.Client, error) {
			return &client.Client{}, nil
		})
	defer patches.Reset()
	patches = gomonkey.ApplyFunc((*client.Client).NegotiateAPIVersion,
		func(_ *client.Client, ctx context.Context) {
			return
		})
	defer patches.Reset()

	convey.Convey("test success", t, func() {
		cli, err := NewTCPDockerClient("127.0.0.1")
		convey.So(err, convey.ShouldBeNil)
		convey.So(cli, convey.ShouldNotBeNil)
	})

	patches = gomonkey.ApplyFunc(client.NewClientWithOpts,
		func(ops ...func(*client.Client) error) (*client.Client, error) {
			return nil, errors.New("error")
		})
	defer patches.Reset()

	convey.Convey("test failed", t, func() {
		cli, err := NewTCPDockerClient("127.0.0.1")
		convey.So(cli, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func Test_TCPDockerOp(t *testing.T) {
	patches := gomonkey.ApplyFunc((*client.Client).ImagePull,
		func(_ *client.Client, ctx context.Context, refStr string, options dockerTypes.ImagePullOptions) (
			io.ReadCloser, error) {
			readCloser := io.NopCloser(io.LimitReader(strings.NewReader("xx"), 0))
			defer readCloser.Close()
			return readCloser, nil
		})
	defer patches.Reset()

	convey.Convey("test PullPrivateImage success", t, func() {
		cli := &DockerClient{cli: &client.Client{}}
		err := cli.PullPrivateImage(context.Background(), "test", "auth")
		convey.So(err, convey.ShouldBeNil)
	})

	patches = gomonkey.ApplyFunc((*client.Client).ImagePull,
		func(_ *client.Client, ctx context.Context, refStr string, options dockerTypes.ImagePullOptions) (
			io.ReadCloser, error) {
			return nil, errors.New("error")
		})
	defer patches.Reset()

	convey.Convey("test PullPrivateImage failed", t, func() {
		cli := &DockerClient{cli: &client.Client{}}
		err := cli.PullPrivateImage(context.Background(), "test", "auth")
		convey.So(err, convey.ShouldNotBeNil)
	})

	patches = gomonkey.ApplyFunc((*client.Client).Events,
		func(_ *client.Client, ctx context.Context, options dockerTypes.EventsOptions) (
			<-chan events.Message, <-chan error) {
			return nil, nil
		})
	defer patches.Reset()

	convey.Convey("test Events failed", t, func() {
		cli := &DockerClient{cli: &client.Client{}}
		msgChan, errChan := cli.Event(context.Background())
		convey.So(msgChan, convey.ShouldBeNil)
		convey.So(errChan, convey.ShouldBeNil)
	})

	patches = gomonkey.ApplyFunc((*client.Client).ImageInspectWithRaw,
		func(_ *client.Client, ctx context.Context, imageID string) (dockerTypes.ImageInspect, []byte, error) {
			return dockerTypes.ImageInspect{}, nil, nil
		})
	defer patches.Reset()

	convey.Convey("test Events failed", t, func() {
		cli := &DockerClient{cli: &client.Client{}}
		_, _, err := cli.ImageInspectWithRaw(context.Background(), "test")
		convey.So(err, convey.ShouldBeNil)
	})

	patches = gomonkey.ApplyFunc((*client.Client).ImageRemove,
		func(_ *client.Client, ctx context.Context, imageID string, options dockerTypes.ImageRemoveOptions) (
			[]dockerTypes.ImageDeleteResponseItem, error) {
			return []dockerTypes.ImageDeleteResponseItem{}, nil
		})
	defer patches.Reset()

	convey.Convey("test Events failed", t, func() {
		cli := &DockerClient{cli: &client.Client{}}
		_, err := cli.ImageRemove(context.Background(), "test", dockerTypes.ImageRemoveOptions{})
		convey.So(err, convey.ShouldBeNil)
	})
}
