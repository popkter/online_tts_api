import asyncio
import uuid
from typing import Optional

import websockets

from tts_ext import start_connection, parser_response, start_session, \
    send_text, finish_session, finish_connection, EVENT_ConnectionFinished, \
    EVENT_ConnectionFailed, print_log


class VolcanoWebsocketClient:
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
                res = parser_response(message)
                print_log(f"event-->  {res.optional.event} sessionId-->  {res.optional.sessionId} payload_type--> {res.header.message_type}" )
                if self.audio_callback:
                    await self.audio_callback(res.optional.event, res.payload)
                if res.optional.event in [EVENT_ConnectionFinished, EVENT_ConnectionFailed]:
                    break
                else:
                    continue
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
