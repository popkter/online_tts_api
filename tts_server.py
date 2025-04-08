import asyncio
import json
import os
import threading
from typing import Dict
import websockets
from dotenv import load_dotenv
from websocket_tts import TTSClient

class TTSServer:
    def __init__(self, host: str = "localhost", port: int = 8765):
        """
        初始化TTS服务器
        :param host: 服务器主机地址
        :param port: 服务器端口
        """
        self.host = host
        self.port = port
        self.tts_clients: Dict[str, TTSClient] = {}  # 存储每个会话的TTS客户端
        self.active_connections: Dict[str, websockets.WebSocketServerProtocol] = {}
        
        # 加载环境变量
        load_dotenv()
        self.app_id = os.getenv("APP_ID")
        self.token = os.getenv("TOKEN")


    async def close_client(self):
        for tts_client in self.tts_clients.values():
            await tts_client.close()


    def run_close_client(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self.close_client())
        finally:
            loop.close()

    async def handle_client(self, websocket: websockets.WebSocketServerProtocol):
        """
        处理客户端连接
        :param websocket: WebSocket连接
        """
        try:
            # 等待客户端发送请求
            async for message in websocket:
                try:
                    # 解析客户端消息
                    data = json.loads(message)
                    text = data.get('text', '')
                    request_id = data.get('request_id')
                    action = data.get('action', 'synthesize')  # 默认为synthesize

                    print("action:", action, " request_id:", request_id, " text: ", text)

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
                        
                        # 创建新的TTS客户端并连接
                        tts_client = TTSClient(self.app_id, self.token)
                        await tts_client.connect()
                        
                        # 定义音频回调函数
                        async def audio_callback(audio_data):
                            # 发送音频数据给客户端
                            await websocket.send(json.dumps({
                                'request_id': request_id,
                                'status': 'success',
                                'audio_data': audio_data.hex()
                            }))
                        
                        # 开始流式处理
                        await tts_client.start_streaming(audio_callback)

                        # 创建并启动线程，使用事件循环执行异步方法
               
                                
                        close_thread = threading.Thread(target=self.run_close_client)
                        close_thread.daemon = True  # 设置为守护线程，这样主线程结束时它会自动结束
                        close_thread.start()  # 启动线程
                        print(f"已启动关闭客户端线程: {close_thread.name}")
                        
                        self.tts_clients[request_id] = tts_client
                        
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
                        await tts_client.synthesize_stream(text)
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
                    await tts_client.close()
                except:
                    pass
                del self.tts_clients[request_id]

    async def start(self):
        """
        启动TTS服务器
        """
        server = await websockets.serve(
            self.handle_client,
            self.host,
            self.port
        )
        print(f"TTS服务器已启动，监听地址: ws://{self.host}:{self.port}")
        await server.wait_closed()

async def main():
    """
    主函数
    """
    server = TTSServer()
    await server.start()

if __name__ == "__main__":
    asyncio.run(main())