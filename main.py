import asyncio
import uuid
import websockets
import json
import gzip
from fastapi import FastAPI, Response
import uvicorn
from dotenv import load_dotenv
import os
import uvloop

load_dotenv()
appid = os.getenv("APP_ID")
token = os.getenv("TOKEN")
cluster = "volcano_tts"

host = "openspeech.bytedance.com"
api_url = f"wss://{host}/api/v1/tts/ws_binary"

default_header = bytearray(b'\x11\x10\x11\x00')


async def stream_tts(request_json):
    payload_bytes = str.encode(json.dumps(request_json))
    payload_bytes = gzip.compress(payload_bytes)
    full_client_request = bytearray(default_header)
    full_client_request.extend((len(payload_bytes)).to_bytes(4, 'big'))
    full_client_request.extend(payload_bytes)
    header = {"Authorization": f"Bearer; {token}"}

    max_retries = 10
    for attempt in range(max_retries):
        print(f"尝试 {attempt + 1}/{max_retries}")
        audio_data = bytearray()

        try:
            async with websockets.connect(api_url, additional_headers=header, ping_interval=None) as ws:
                response_headers = ws.response.headers
                print("Request Body", request_json)
                print("Response Headers:", response_headers)  # 打印响应头

                await ws.send(full_client_request)

                while True:
                    try:
                        res = await ws.recv()
                        done = parse_response(res, audio_data)

                        if done == 1:  # 成功完成
                            return bytes(audio_data)
                            # retry
                        elif done == -1:
                            break
                    except websockets.exceptions.ConnectionClosed as e:
                        print(f"connection close: {e}")
                        break

        except Exception as e:
            print(f"connection error: {e}")

        # 只在最后一次尝试并有数据时返回部分数据
        if attempt == max_retries - 1 and len(audio_data) > 0:
            print("response part data")
            return bytes(audio_data)


def parse_response(res, audio_data: bytearray):
    print("--------------------------- response ---------------------------")
    header_size = res[0] & 0x0f
    message_type = res[1] >> 4
    message_type_specific_flags = res[1] & 0x0f
    message_compression = res[2] & 0x0f
    header_extensions = res[4:header_size * 4]
    payload = res[header_size * 4:]
    if header_size != 1:
        print(f"           Header extensions: {header_extensions}")
    if message_type == 0xb:  # audio-only server response
        if message_type_specific_flags == 0:
            return 0
        else:
            sequence_number = int.from_bytes(payload[:4], "big", signed=True)
            payload_size = int.from_bytes(payload[4:8], "big", signed=False)
            payload = payload[8:]
            audio_data.extend(payload)  # 追加音频数据
            if sequence_number < 0:
                return 1
            return 0
    elif message_type == 0xf:
        code = int.from_bytes(payload[:4], "big", signed=False)
        msg_size = int.from_bytes(payload[4:8], "big", signed=False)
        error_msg = payload[8:]
        if message_compression == 1:
            error_msg = gzip.decompress(error_msg)
        error_msg = str(error_msg, "utf-8")
        print(f"          Error message code: {code}")
        print(f"          Error message size: {msg_size} bytes")
        print(f"               Error message: {error_msg}")
        if code == 3031 or code == 3032 or code == 3040:
            return -1
        return 1
    elif message_type == 0xc:
        msg_size = int.from_bytes(payload[:4], "big", signed=False)
        payload = payload[4:]
        if message_compression == 1:
            payload = gzip.decompress(payload)
        print(f"            Frontend message: {payload}")
    else:
        print("undefined message type!")
        return 1


app = FastAPI()


@app.get("/synthesize")
async def synthesize(
        text: str,
        voice_type: str = "ICL_zh_female_huoponvhai_tob",
        emotion: str = "xxx",
        req_id: str = None,  # 默认 None，内部生成
        rate: int = 24000,
        speed: float = 1.1,
        volume: float = 1.0,
        pitch: float = 1.0,
        encoding: str = "mp3",
):
    if req_id is None:
        req_id = str(uuid.uuid4())  # 动态生成唯一 ID

    request_json = {
        "app": {
            "appid": appid,
            "token": "access_token",
            "cluster": cluster
        },
        "user": {
            "uid": "388808087185088"
        },
        "audio": {
            "voice_type": voice_type,
            "encoding": encoding,
            "rate": rate,
            "speed_ratio": speed,
            "volume_ratio": volume,
            "pitch_ratio": pitch,
            "emotion": emotion,
        },
        "request": {
            "reqid": req_id,
            "text": text,
            "text_type": "plain",
            "operation": "submit"
        }
    }

    audio_data = await stream_tts(request_json)

    if encoding == "mp3":
        return Response(content=audio_data, media_type="audio/mp3")
    elif encoding == "pcm":
        headers = {
            "Content-Type": "audio/l16",
            "X-Sample-Rate": str(rate),
            "X-Channels": "1",
            "X-Bits-Per-Sample": "16"
        }
        return Response(
            content=audio_data,
            media_type="audio/l16",
            headers=headers
        )


if __name__ == '__main__':
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    uvicorn.run("main:app", host="0.0.0.0", port=10012, reload=False, workers=4, limit_concurrency=100)  # ✅ 使用字符串
    # uvicorn.run(app, host="0.0.0.0", port=10012, workers=4)
