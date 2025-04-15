import asyncio
import json
import os
from http import HTTPStatus
from typing import Dict, Optional

import websockets
from dotenv import load_dotenv
from websockets import Headers
from websockets.asyncio.server import ServerConnection
from websockets.http11 import Response, Request

from volcano_websocket_client import VolcanoWebsocketClient

class TTSServer:
    def __init__(self, host: str = "0.0.0.0", port: int = 10013):
        """
        初始化TTS服务器
        :param host: 服务器主机地址
        :param port: 服务器端口
        """
        self.host = host
        self.port = port
        self.tts_clients: Dict[str, VolcanoWebsocketClient] = {}  # 存储每个会话的TTS客户端

        # 加载环境变量
        load_dotenv()
        self.app_id = os.getenv("APP_ID")
        self.token = os.getenv("TOKEN")

    async def handle(self, websocket: websockets):
        """
        处理客户端连接
        :param websocket: WebSocket连接
        """
        try:
            # 等待客户端发送请求
            async for message in websocket:

                # 解析客户端消息
                data = json.loads(message)
                device_id = data.get('device_id', 'caf')
                text = data.get('text', '')
                session_id = data.get('session_id')
                action = data.get('action', 'synthesize')  # 默认为synthesize
                audio_format = data.get('audio_format', 'mp3')
                sample_rate = data.get('sample_rate', 24000)
                speech_rate = data.get('speech_rate', 50)
                loudness_rate = data.get('loudness_rate', 0)
                speaker = data.get('voice_type', '')

                try:
                    print(f'action: {action} session_id: {session_id} text: {text} speaker: {speaker} audio_format: {audio_format}  sample_rate: {sample_rate} speech_rate: {speech_rate} loudness_rate: {loudness_rate}', flush=True)

                    if not session_id:
                        await websocket.send(json.dumps({
                            'session_id': None,
                            'event': -1,
                            'data': '缺少session_id参数'
                        }))
                        continue

                    # 处理开始会话请求
                    if action == 'start':
                        if session_id in self.tts_clients:
                            await websocket.send(json.dumps({
                                'session_id': session_id,
                                'event': -1,
                                'data': '会话已存在'
                            }))
                            continue

                        # 定义音频回调函数
                        async def audio_callback(event_id, audio_data: Optional[bytes] = None):
                            payload: str = audio_data.decode("latin1") if audio_data else ''
                            # 发送音频数据给客户端
                            await websocket.send(json.dumps({
                                'session_id': session_id,
                                'event': event_id,
                                'data': payload
                            }))

                        # 根据device_id建立websocket连接
                        try:
                            if self.tts_clients.get(session_id):
                                tts_client = self.tts_clients[device_id]
                                await tts_client.disconnect()
                                self.tts_clients.pop(session_id)
                        except KeyError as e:
                            print(f"start error: session_id: {session_id} tts_client: {self.tts_clients} e: {e.args}")

                        tts_client = VolcanoWebsocketClient(self.app_id, self.token)
                        self.tts_clients[session_id] = tts_client
                        await tts_client.connect(audio_callback)

                        # 开始流式处理
                        await tts_client.start_session(speaker, audio_format, sample_rate, speech_rate, loudness_rate)

                        continue

                    # 处理结束会话请求
                    if action == 'end':
                        if session_id not in self.tts_clients:
                            await websocket.send(json.dumps({
                                'session_id': session_id,
                                'event': -1,
                                'data': "会话不存在"
                            }))
                            continue

                        # 关闭TTS客户端连接
                        tts_client = self.tts_clients[session_id]
                        await tts_client.finish_session()
                        del self.tts_clients[session_id]
                        continue

                    # 处理合成请求
                    if action == 'synthesize':
                        if session_id not in self.tts_clients:
                            await websocket.send(json.dumps({
                                'session_id': session_id,
                                'event': -1,
                                'data': "会话未开始"
                            }))
                            continue

                        if not text:
                            await websocket.send(json.dumps({
                                'session_id': session_id,
                                'event': -1,
                                'data': "缺少文本"
                            }))
                            continue

                        # 获取TTS客户端并发送文本
                        tts_client = self.tts_clients[session_id]
                        await tts_client.synthesize(text, speaker)
                        continue

                    # 未知动作
                    await websocket.send(json.dumps({
                        'session_id': session_id,
                        'event': -1,
                        'data': f'未知动作: {action}'
                    }))

                except json.JSONDecodeError:
                    await websocket.send(json.dumps({
                        'session_id': session_id,
                        'event': -1,
                        'data': f'无效的JSON格式{data}'
                    }))
                except Exception as e:
                    await websocket.send(json.dumps({
                        'session_id': session_id,
                        'event': -1,
                        'data': f'{str(e)}'
                    }))

        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            # 清理客户端连接
            for session_id, tts_client in list(self.tts_clients.items()):
                try:
                    await tts_client.disconnect()
                # except:
                #     pass
                finally:
                    del self.tts_clients[session_id]

    async def process_request(self, connection: ServerConnection, request: Request) -> Optional[Response]:
        headers: Headers = request.headers
        auth_header = headers.get("Authorization")

        if auth_header != f"Bearer {self.token}":
            return Response(
                status_code=HTTPStatus.UNAUTHORIZED,
                headers=Headers({"Content-Type": "text/plain"}),
                body=b"Unauthorized: Invalid token\n",
                reason_phrase=""
            )

        return None  # Allow connection

    async def start(self):
        """
        启动TTS服务器
        """
        server = await websockets.serve(
            self.handle,
            self.host,
            self.port,
            process_request=self.process_request  # 鉴权逻辑放这里
        )
        print(f"TTS服务器已启动，监听地址: ws://{self.host}:{self.port}", flush=True)
        await server.wait_closed()


async def main():
    """
    主函数
    """
    server = TTSServer()
    await server.start()


if __name__ == "__main__":
    asyncio.run(main())
