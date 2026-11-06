# local-cache-service

Use Nginx as the code cache tool to reduce the code download time.

Local-cache-service is recommended. The cold start time of the function is reduced to 20 ms.
## Installing the Chart

To install the chart with the release name `local-cache-service`:

```bash
helm install local-cache-service ./ -f ./values.yaml --debug
```

## Uninstalling the Chart

To uninstall/delete the local-cache-service deployment:

```bash
helm uninstall local-cache-service
```