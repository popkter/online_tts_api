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
        # 修改存储结构，使用嵌套字典，外层用device_id索引，内层用session_id索引
        self.device_sessions: Dict[str, Dict[str, VolcanoWebsocketClient]] = {}
        # self.device_single_sessions: Dict[str, VolcanoWebsocketClient] = {}
        
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
                    print(f'action: {action} device_id: {device_id} session_id: {session_id} text: {text} speaker: {speaker}', flush=True)

                    if not session_id:
                        await websocket.send(json.dumps({
                            'session_id': None,
                            'event': -1,
                            'data': '缺少session_id参数'
                        }))
                        break

                    # 处理开始会话请求
                    if action == 'start':
                        # 如果设备已有会话，关闭所有旧会话
                        print(f'设备注册状态: {device_id in self.device_sessions}', flush=True)
                        if device_id in self.device_sessions:

                            print(f"检测到设备已有会话，正在关闭设备的所有会话: {device_id}", flush=True)
                            for old_session_id, old_client in self.device_sessions[device_id].items():
                                try:
                                    await old_client.disconnect()
                                except Exception as e:
                                    print(f"关闭旧会话时出错: device_id={device_id}, session_id={old_session_id}, error={e}", flush=True)
                            self.device_sessions.pop(device_id)

                        # 定义音频回调函数
                        async def audio_callback(event_id, audio_data: Optional[bytes] = None):
                            payload: str = audio_data.decode("latin1") if audio_data else ''
                            # 发送音频数据给客户端
                            await websocket.send(json.dumps({
                                'session_id': session_id,
                                'event': event_id,
                                'data': payload
                            }))

                        # 创建新的TTS客户端并连接
                        try:
                            tts_client = VolcanoWebsocketClient(self.app_id, self.token)

                            # 保存新的客户端
                            if device_id not in self.device_sessions:
                                self.device_sessions[device_id] = {}
                            self.device_sessions[device_id][session_id] = tts_client

                            await tts_client.connect(audio_callback)
                            
                            # 开始流式处理
                            await tts_client.start_session(speaker, audio_format, sample_rate, speech_rate, loudness_rate)

                            
                            await websocket.send(json.dumps({
                                'session_id': session_id,
                                'event': 0,
                                'data': '会话已开始'
                            }))
                        except Exception as e:
                            print(f"创建新会话时出错: {e}", flush=True)
                            await websocket.send(json.dumps({
                                'session_id': session_id,
                                'event': -1,
                                'data': f'创建会话失败: {str(e)}'
                            }))
                        continue

                    # 处理结束会话请求
                    if action == 'end':
                        if (device_id not in self.device_sessions or 
                            session_id not in self.device_sessions[device_id]):
                            await websocket.send(json.dumps({
                                'session_id': session_id,
                                'event': -1,
                                'data': "会话不存在"
                            }))
                            continue

                        # 关闭TTS客户端连接
                        tts_client = self.device_sessions[device_id][session_id]
                        await tts_client.finish_session()
                        # del self.device_sessions[device_id][session_id]
                        # if not self.device_sessions[device_id]:  # 如果设备没有更多会话，删除设备记录
                        #     del self.device_sessions[device_id]
                        continue

                    # 处理合成请求
                    if action == 'synthesize':
                        if (device_id not in self.device_sessions or 
                            session_id not in self.device_sessions[device_id]):
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
                        tts_client = self.device_sessions[device_id][session_id]
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
            for device_id in list(self.device_sessions.keys()):
                for session_id, tts_client in list(self.device_sessions[device_id].items()):
                    try:
                        await tts_client.disconnect()
                    finally:
                        del self.device_sessions[device_id][session_id]
                if not self.device_sessions[device_id]:
                    del self.device_sessions[device_id]

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
