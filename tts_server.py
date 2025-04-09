import asyncio
import json
import os
import uuid
from http import HTTPStatus
from typing import Dict, Optional

import websockets
from dotenv import load_dotenv
from websockets import Headers
from websockets.asyncio.server import ServerConnection
from websockets.http11 import Response, Request

from tts_ext import start_connection, parser_response, print_response, start_session, \
    send_text, finish_session, EVENT_TTSResponse, AUDIO_ONLY_RESPONSE, finish_connection, EVENT_ConnectionFinished, \
    EVENT_ConnectionFailed


class RemoteTtsClient:
    def __init__(self, app_id: str, token: str, speaker: str = 'zh_female_shuangkuaisisi_moon_bigtts'):
        """
        初始化TTS客户端
        :param app_id: 应用ID
        :param token: 访问令牌
        :param speaker: 说话人声音，默认使用中文女声
        """
        self.app_id = app_id
        self.token = token
        self.speaker = speaker
        self.ws: Optional[websockets] = None
        self.session_id = None
        self.url = 'wss://openspeech.bytedance.com/api/v3/tts/bidirection'
        self.ws_header = {
            "X-Api-App-Key": app_id,
            "X-Api-Access-Key": token,
            "X-Api-Resource-Id": 'volc.service_type.10029',
            "X-Api-Connect-Id": uuid.uuid4(),
        }
        self.audio_callback = None
        self.stream_task = None

    # 流式接受音频数据
    async def _fetch_audio(self):
        try:
            async for message in self.ws:
                print("收到消息:", message)
                try:
                    res = parser_response(message)
                    print_response(res, 'send_text res:')
                    if res.optional.event == EVENT_TTSResponse and res.header.message_type == AUDIO_ONLY_RESPONSE:
                        if self.audio_callback:
                            await self.audio_callback(res.payload)
                    elif res.optional.event in [EVENT_ConnectionFinished, EVENT_ConnectionFailed]:
                        break
                    else:
                        continue
                finally:
                    print("结束")
        finally:
            if self.stream_task:
                self.stream_task = None

    # 建立WebSocket连接
    async def connect(self, audio_callback):
        self.ws = await websockets.connect(self.url, additional_headers=self.ws_header, max_size=1000000000)
        await start_connection(self.ws)

        self.audio_callback = audio_callback
        self.stream_task = asyncio.create_task(self._fetch_audio())

    # 启动新的会话
    async def start_session(self, speaker, audio_format, sample_rate, speech_rate, loudness_rate):
        self.session_id = uuid.uuid4().__str__().replace('-', '')
        if speaker != '':
            self.speaker = speaker
        await start_session(self.ws, self.speaker, self.session_id, audio_format, sample_rate, speech_rate,
                            loudness_rate)

    # 流式合成文本
    async def synthesize(self, text: str, speaker):
        if not self.ws:
            raise RuntimeError("WebSocket未连接，请先调用connect()")
        if speaker != '':
            self.speaker = speaker
        await send_text(self.ws, self.speaker, text, self.session_id)

    # 结束本次会话
    async def finish_session(self):
        await finish_session(self.ws, self.session_id)

    async def disconnect(self):
        if self.stream_task:
            self.stream_task.cancel()
            try:
                await self.stream_task
            except asyncio.CancelledError:
                print("✅ stream_task 已取消")
            self.stream_task = None

        if self.ws:
            try:
                await finish_session(self.ws, self.session_id)
            except Exception as e:
                print(f"⚠️ finish_session 错误: {e}")

            try:
                await finish_connection(self.ws)
            except Exception as e:
                print(f"⚠️ finish_connection 错误: {e}")

            await self.ws.close()

        self.ws = None
        self.session_id = None
        self.audio_callback = None

    def is_closed(self):
        if self.ws:
            return self.ws.closed
        else:
            return True


class TTSServer:
    def __init__(self, host: str = "0.0.0.0", port: int = 10013):
        """
        初始化TTS服务器
        :param host: 服务器主机地址
        :param port: 服务器端口
        """
        self.host = host
        self.port = port
        self.tts_clients: Dict[str, RemoteTtsClient] = {}  # 存储每个会话的TTS客户端

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
                request_id = data.get('request_id')
                action = data.get('action', 'synthesize')  # 默认为synthesize
                audio_format = data.get('audio_format', 'mp3')
                sample_rate = data.get('sample_rate', 24000)
                speech_rate = data.get('speech_rate', 50)
                loudness_rate = data.get('loudness_rate', 0)
                speaker = data.get('voice_type', '')

                try:
                    print(
                        f'action: {action} request_id: {request_id} text: {text} speaker: {speaker} audio_format: {audio_format}  sample_rate: {sample_rate} speech_rate: {speech_rate} loudness_rate: {loudness_rate}',flush=True)

                    if not request_id:
                        await websocket.send(json.dumps({
                            'error': '缺少request_id参数',
                            'request_id': None
                        }))
                        continue

                    # 处理开始会话请求
                    if action == 'start':
                        if request_id in self.tts_clients:
                            await websocket.send(json.dumps({
                                'error': '会话已存在',
                                'request_id': request_id
                            }))
                            continue

                        # 定义音频回调函数
                        async def audio_callback(audio_data):
                            # 发送音频数据给客户端
                            await websocket.send(json.dumps({
                                'request_id': request_id,
                                'status': 'success',
                                'audio_data': audio_data.hex()
                            }))

                        # 根据device_id建立websocket连接
                        if self.tts_clients.get(device_id) is None:
                            tts_client = RemoteTtsClient(self.app_id, self.token)
                            self.tts_clients[request_id] = tts_client
                            await tts_client.connect(audio_callback)
                        else:
                            tts_client = self.tts_clients[device_id]
                            if tts_client.is_closed():
                                self.tts_clients.pop(device_id)
                                tts_client = RemoteTtsClient(self.app_id, self.token)
                                self.tts_clients[request_id] = tts_client
                                await tts_client.connect(audio_callback)

                        # 开始流式处理
                        await tts_client.start_session(speaker, audio_format, sample_rate, speech_rate, loudness_rate)

                        await websocket.send(json.dumps({
                            'request_id': request_id,
                            'status': 'success',
                            'message': '会话已开始'
                        }))
                        continue

                    # 处理结束会话请求
                    if action == 'end':
                        if request_id not in self.tts_clients:
                            await websocket.send(json.dumps({
                                'error': '会话不存在',
                                'request_id': request_id
                            }))
                            continue

                        # 关闭TTS客户端连接
                        tts_client = self.tts_clients[request_id]
                        await tts_client.finish_session()
                        del self.tts_clients[request_id]

                        await websocket.send(json.dumps({
                            'request_id': request_id,
                            'status': 'success',
                            'message': '会话已结束'
                        }))
                        continue

                    # 处理合成请求
                    if action == 'synthesize':
                        if request_id not in self.tts_clients:
                            await websocket.send(json.dumps({
                                'error': '会话未开始',
                                'request_id': request_id
                            }))
                            continue

                        if not text:
                            await websocket.send(json.dumps({
                                'error': '缺少text参数',
                                'request_id': request_id
                            }))
                            continue

                        # 获取TTS客户端并发送文本
                        tts_client = self.tts_clients[request_id]
                        await tts_client.synthesize(text, speaker)
                        continue

                    # 未知动作
                    await websocket.send(json.dumps({
                        'error': f'未知动作: {action}',
                        'request_id': request_id
                    }))

                except json.JSONDecodeError:
                    await websocket.send(json.dumps({
                        'error': '无效的JSON格式',
                        'request_id': None
                    }))
                except Exception as e:
                    await websocket.send(json.dumps({
                        'error': str(e),
                        'request_id': request_id
                    }))

        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            # 清理客户端连接
            for request_id, tts_client in list(self.tts_clients.items()):
                try:
                    await tts_client.disconnect()
                # except:
                #     pass
                finally:
                    del self.tts_clients[request_id]

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
