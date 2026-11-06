# Copyright (c) 2024 Huawei Technologies Co., Ltd
#
# This software is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
# http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
import asyncio
import websockets
import json
import base64

connected = set()
workers = set()
users = set()

async def register(websocket):
    if not workers:  # 如果没有 worker，注册为 worker
        print("Registered as worker")
        workers.add(websocket)
    else:  # 否则注册为 user
        print("Registered as user")
        users.add(websocket)
    connected.add(websocket)

async def unregister(websocket):
    if websocket in workers:
        workers.remove(websocket)
    if websocket in users:
        users.remove(websocket)
    connected.remove(websocket)

async def echo(websocket, path):
    await register(websocket)
    try:
        async for message in websocket:
            print(f"Received message from client: {message}")
            print(f"path is: {path}")
            if websocket in users:  # 如果是 user 发来的消息
                if workers:  # 如果有 worker 连接
                    worker = next(iter(workers))  # 获取一个 worker
                    # Create a dictionary with traceID and message data
                    data = {"traceID": "12345", "data": base64.b64encode(message).decode('utf-8')}

                    # Serialize the dictionary to a JSON string
                    json_data = json.dumps(data)

                    print(json_data)
                    # Send the JSON data to the worker
                    await worker.send(json_data.encode('utf-8'))
                else:
                    await websocket.send("No worker available")
            elif websocket in workers:  # 如果是 worker 发来的消息
                if users:  # 如果有 user 连接
                    user = next(iter(users))  # 获取一个 user
                    if isinstance(message,str):
                        print("message is str")
                        data = json.loads(message)['data']
                        print(decoded_data)
                        await user.send(decoded_data)  # 将消息转发给 user
                    else:
                        data = json.loads(message.decode('utf-8'))['data']
                        decoded_data = base64.b64decode(data)
                        print(decoded_data)
                        await user.send(decoded_data)  # 将消息转发给 user
                else:
                    await websocket.send("No user available")
    finally:
        await unregister(websocket)

async def main():
    # server_ip must not empty
    server_ip = ""
    server = await websockets.serve(echo, server_ip, 9766)
    await server.wait_closed()

if __name__ == '__main__':
    asyncio.run(main())

