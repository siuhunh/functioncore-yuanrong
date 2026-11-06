### remote-debug

#### 启动 mock rdispatcher

填写 IP

```python
server_ip="" #填写当前机器IP地址
```

启动
```shell
python mock_cae_rdispatcher.py
```

此命令会启动一个 websocket server，分别与 CLI 和 worker 实例建立连接。
用于接收 IDE 的 websocket 请求，并转发给 worker。
