module functioncore

go 1.20

require (
	clients v1.0.0
	huaweicloud.com/containers/security/cbb_adapt v1.0.7
	github.com/huaweicloud/huaweicloud-sdk-go-obs v3.23.12+incompatible
	github.com/agiledragon/gomonkey/v2 v2.11.0
	github.com/asaskevich/govalidator v0.0.0-20230301143203-a9d515a09cc2
	// for test or internal use
	github.com/docker/docker v23.0.3+incompatible
	github.com/fsnotify/fsnotify v1.7.0
	github.com/gin-gonic/gin v1.10.0
	github.com/redis/go-redis/v9 v9.0.5
	github.com/golang/mock v1.4.4
	github.com/golang/protobuf v1.5.3
	github.com/huaweicloud/huaweicloud-sdk-go-v3 v0.1.84
	github.com/panjf2000/ants/v2 v2.10.0
	github.com/robfig/cron/v3 v3.0.1
	github.com/smartystreets/goconvey v1.6.4
	github.com/spf13/cobra v1.8.0
	github.com/stretchr/testify v1.7.1
	github.com/valyala/fasthttp v1.55.0
	go.etcd.io/etcd/api/v3 v3.5.11
	go.etcd.io/etcd/client/v3 v3.5.11
	go.uber.org/automaxprocs v1.5.3
	go.opentelemetry.io/otel v1.21.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.21.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.21.0
	go.opentelemetry.io/otel/sdk v1.21.0
	go.opentelemetry.io/otel/trace v1.21.0
	go.uber.org/automaxprocs v1.5.3
	go.uber.org/zap v1.27.0
	golang.org/x/net v0.21.0
	golang.org/x/text v0.9.0
	golang.org/x/time v0.0.0-20200630173020-3af7569d3a1e
	google.golang.org/grpc v1.57.2
	google.golang.org/protobuf v1.26.0
	gopkg.in/yaml.v3 v3.0.1
	k8s.io/api v0.27.13
	k8s.io/apimachinery v0.27.13
	k8s.io/client-go v0.27.13
	k8s.io/kubectl v0.31.2
	k8s.io/metrics v0.31.2
    github.com/olekukonko/tablewriter v0.0.5
	github.com/containerd/containerd v1.6.16
)

replace (
	clients => ../datasystem/output/sdk/go
	github.com/agiledragon/gomonkey/v2 => github.com/agiledragon/gomonkey/v2 v2.11.0
	github.com/asaskevich/govalidator => github.com/asaskevich/govalidator v0.0.0-20230301143203-a9d515a09cc2
	// for test or internal use
	github.com/docker/docker => github.com/docker/docker v23.0.3+incompatible
	github.com/gin-gonic/gin => github.com/gin-gonic/gin v1.10.0
	github.com/redis/go-redis/v9 => github.com/redis/go-redis/v9 v9.0.5
	github.com/golang/mock => github.com/golang/mock v1.3.1
	github.com/golang/protobuf => github.com/golang/protobuf v1.5.3
	github.com/google/uuid => github.com/google/uuid v1.4.0
	github.com/panjf2000/ants/v2 => github.com/panjf2000/ants/v2 v2.10.0
	github.com/smartystreets/goconvey => github.com/smartystreets/goconvey v1.6.4
	github.com/spf13/cobra => github.com/spf13/cobra v1.8.0
	github.com/stretchr/testify => github.com/stretchr/testify v1.7.1
	github.com/valyala/fasthttp => github.com/valyala/fasthttp v1.55.0
	go.etcd.io/etcd/api/v3 => go.etcd.io/etcd/api/v3 v3.5.11
	go.etcd.io/etcd/client/v3 => go.etcd.io/etcd/client/v3 v3.5.11
	go.uber.org/automaxprocs => go.uber.org/automaxprocs v1.5.3
	go.uber.org/zap => go.uber.org/zap v1.27.0
	// indirect dependency, but affects VPC plugin building, will cause error if not pinned
    golang.org/x/crypto => golang.org/x/crypto v0.0.0-20220214200702-86341886e292
	// affects VPC plugin building, will cause error if not pinned
	golang.org/x/net => golang.org/x/net v0.21.0
	golang.org/x/oauth2 => golang.org/x/oauth2 v0.0.0-20190402181905-9f3314589c9a
	// affects VPC plugin building, will cause error if not pinned
	golang.org/x/sys => golang.org/x/sys v0.0.0-20210124154548-22da62e12c0c
	golang.org/x/text => golang.org/x/text v0.9.0
	golang.org/x/time => golang.org/x/time v0.0.0-20191024005414-555d28b269f0
	gopkg.in/yaml.v2 => gopkg.in/yaml.v2 v2.4.0
	k8s.io/api => k8s.io/api v0.27.13
	k8s.io/apimachinery => k8s.io/apimachinery v0.27.13
	k8s.io/client-go => k8s.io/client-go v0.27.13
	k8s.io/kubectl => k8s.io/kubectl v0.27.13
	k8s.io/metrics => k8s.io/metrics v0.27.13
    github.com/olekukonko/tablewriter => github.com/olekukonko/tablewriter v0.0.5
    github.com/smartystreets/goconvey/convey => github.com/smartystreets/goconvey/convey v1.7.2
    k8s.io/code-generator => k8s.io/code-generator v0.22.2
    go.uber.org/atomic => go.uber.org/atomic v1.10.0
	github.com/andybalholm/brotli => github.com/andybalholm/brotli v1.0.5
	github.com/klauspost/compress => github.com/klauspost/compress v1.16.3
	github.com/Microsoft/hcsshim => github.com/Microsoft/hcsshim v0.8.22
	google.golang.org/grpc => google.golang.org/grpc v1.57.2
	google.golang.org/protobuf => google.golang.org/protobuf v1.25.0
	github.com/gorilla/websocket => github.com/gorilla/websocket v1.5.1
	github.com/docker/go-connections => github.com/docker/go-connections v0.5.0
)
