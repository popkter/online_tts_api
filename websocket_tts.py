import asyncio
import os
import uuid

import websockets
from dotenv import load_dotenv

from stream_tts import start_connection, parser_response, print_response, EVENT_ConnectionStarted, start_session, \
    EVENT_SessionStarted, send_text, finish_session, EVENT_TTSResponse, EVENT_TTSSentenceStart, EVENT_TTSSentenceEnd, \
    AUDIO_ONLY_RESPONSE, finish_connection


class TTSClient:
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
        self.ws = None
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
        self.is_streaming = False

    async def connect(self):
        """
        建立WebSocket连接并启动会话
        """
        self.ws = await websockets.connect(self.url, additional_headers=self.ws_header, max_size=1000000000)
        await start_connection(self.ws)
        res = parser_response(await self.ws.recv())
        if res.optional.event != EVENT_ConnectionStarted:
            raise RuntimeError("连接失败")

        self.session_id = uuid.uuid4().__str__().replace('-', '')
        await start_session(self.ws, self.speaker, self.session_id)
        res = parser_response(await self.ws.recv())
        if res.optional.event != EVENT_SessionStarted:
            raise RuntimeError('会话启动失败!')

    async def start_streaming(self, audio_callback):
        """
        开始流式处理
        :param audio_callback: 音频数据回调函数
        """
        if self.is_streaming:
            return
            
        self.audio_callback = audio_callback
        self.is_streaming = True
        self.stream_task = asyncio.create_task(self._stream_audio())

    async def _stream_audio(self):
        """
        流式处理音频数据
        """
        try:
            while self.is_streaming:
                try:
                    res = parser_response(await self.ws.recv())
                    print_response(res, 'send_text res:')
                    if res.optional.event == EVENT_TTSSentenceStart:
                        continue
                    elif res.optional.event == EVENT_TTSResponse and res.header.message_type == AUDIO_ONLY_RESPONSE:
                        if self.audio_callback:
                            await self.audio_callback(res.payload)
                    elif res.optional.event == EVENT_TTSSentenceEnd:
                        # await self.close()
                        continue
                    else:
                        break
                except websockets.exceptions.ConnectionClosed:
                    break
                except Exception as e:
                    print(f"流式处理出错: {str(e)}")
                    break
        finally:
            self.is_streaming = False
            if self.stream_task:
                self.stream_task = None

    async def synthesize_stream(self, text: str):
        """
        流式合成文本
        :param text: 要转换的文本
        """
        if not self.ws:
            raise RuntimeError("WebSocket未连接，请先调用connect()")
        
        await send_text(self.ws, self.speaker, text, self.session_id)

    async def finish_session(self):
        """
        关闭WebSocket连接
        """
        await finish_session(self.ws, self.session_id)

    async def close(self):
        """
        关闭WebSocket连接
        """
        self.is_streaming = False
        if self.stream_task:
            try:
                await self.stream_task
            except:
                pass
            self.stream_task = None
            
        if self.ws:
            await finish_connection(self.ws)
            try:
                res = parser_response(await self.ws.recv())
                print_response(res, 'finish_connection res:')
            except:
                pass
            await self.ws.close()
            self.ws = None
            self.session_id = None
            self.audio_callback = None

async def main():
    """
    主函数，演示TTS客户端的使用方法
    """
    load_dotenv()
    app_id = os.getenv("APP_ID")
    token = os.getenv("TOKEN")

    # 创建TTS客户端
    client = TTSClient(app_id, token)
    
    try:
        # 连接到WebSocket
        await client.connect()
        
        # 定义音频回调函数
        async def audio_callback(audio_data):
            print(f"收到音频数据: {len(audio_data)} 字节")
            # 这里可以处理音频数据，例如保存到文件或发送给其他客户端
        
        # 开始流式处理
        await client.start_streaming(audio_callback)
        
        # 示例：合成第一段文本
        text1 = "你好，这是一个文本转语音的测试。"
        await client.synthesize_stream(text1)
        
        # 示例：合成第二段文本
        text2 = "这是第二段测试文本。"
        await client.synthesize_stream(text2)
        
        # 等待一段时间以确保所有音频数据都被处理
        await asyncio.sleep(5)
        
    finally:
        # 完成后关闭连接
        await client.close()

if __name__ == "__main__":
    asyncio.run(main())