import asyncio
import json
import os
import uuid

import aiofiles
import websocket
import websockets
from dotenv import load_dotenv
from websockets.asyncio.client import ClientConnection

# https://www.volcengine.com/docs/6561/1329505#%E7%A4%BA%E4%BE%8Bsamples


PROTOCOL_VERSION = 0b0001
DEFAULT_HEADER_SIZE = 0b0001

# Message Type:
FULL_CLIENT_REQUEST = 0b0001
AUDIO_ONLY_RESPONSE = 0b1011
FULL_SERVER_RESPONSE = 0b1001
ERROR_INFORMATION = 0b1111

# Message Type Specific Flags
MsgTypeFlagNoSeq = 0b0000  # Non-terminal packet with no sequence
MsgTypeFlagPositiveSeq = 0b1  # Non-terminal packet with sequence > 0
MsgTypeFlagLastNoSeq = 0b10  # last packet with no sequence
MsgTypeFlagNegativeSeq = 0b11  # Payload contains event number (int32)
MsgTypeFlagWithEvent = 0b100
# Message Serialization
NO_SERIALIZATION = 0b0000
JSON = 0b0001
# Message Compression
COMPRESSION_NO = 0b0000
COMPRESSION_GZIP = 0b0001

EVENT_NONE = 0
EVENT_Start_Connection = 1

EVENT_FinishConnection = 2

EVENT_ConnectionStarted = 50  # 成功建连

EVENT_ConnectionFailed = 51  # 建连失败（可能是无法通过权限认证）

EVENT_ConnectionFinished = 52  # 连接结束

# 上行Session事件
EVENT_StartSession = 100

EVENT_FinishSession = 102
# 下行Session事件
EVENT_SessionStarted = 150
EVENT_SessionFinished = 152

EVENT_SessionFailed = 153

# 上行通用事件
EVENT_TaskRequest = 200

# 下行TTS事件
EVENT_TTSSentenceStart = 350

EVENT_TTSSentenceEnd = 351

EVENT_TTSResponse = 352


class Header:
    def __init__(self,
                 protocol_version=PROTOCOL_VERSION,
                 header_size=DEFAULT_HEADER_SIZE,
                 message_type: int = 0,
                 message_type_specific_flags: int = 0,
                 serial_method: int = NO_SERIALIZATION,
                 compression_type: int = COMPRESSION_NO,
                 reserved_data=0):
        self.header_size = header_size
        self.protocol_version = protocol_version
        self.message_type = message_type
        self.message_type_specific_flags = message_type_specific_flags
        self.serial_method = serial_method
        self.compression_type = compression_type
        self.reserved_data = reserved_data

    def as_bytes(self) -> bytes:
        return bytes([
            (self.protocol_version << 4) | self.header_size,
            (self.message_type << 4) | self.message_type_specific_flags,
            (self.serial_method << 4) | self.compression_type,
            self.reserved_data
        ])


class Optional:
    def __init__(self, event: int = EVENT_NONE, sessionId: str = None, sequence: int = None):
        self.event = event
        self.sessionId = sessionId
        self.errorCode: int = 0
        self.connectionId: str | None = None
        self.response_meta_json: str | None = None
        self.sequence = sequence

    # 转成 byte 序列
    def as_bytes(self) -> bytes:
        option_bytes = bytearray()
        if self.event != EVENT_NONE:
            option_bytes.extend(self.event.to_bytes(4, "big", signed=True))
        if self.sessionId is not None:
            session_id_bytes = str.encode(self.sessionId)
            size = len(session_id_bytes).to_bytes(4, "big", signed=True)
            option_bytes.extend(size)
            option_bytes.extend(session_id_bytes)
        if self.sequence is not None:
            option_bytes.extend(self.sequence.to_bytes(4, "big", signed=True))
        return option_bytes


class Response:
    def __init__(self, header: Header, optional: Optional):
        self.optional = optional
        self.header = header
        self.payload: bytes | None = None

    def __str__(self):
        return super().__str__()


# 发送事件
async def send_event(ws: websocket, header: bytes, optional: bytes | None = None,
                     payload: bytes = None):
    full_client_request = bytearray(header)
    if optional is not None:
        full_client_request.extend(optional)
    if payload is not None:
        payload_size = len(payload).to_bytes(4, 'big', signed=True)
        full_client_request.extend(payload_size)
        full_client_request.extend(payload)
    await ws.send(full_client_request)


# 读取 res 数组某段 字符串内容
def read_res_content(res: bytes, offset: int):
    content_size = int.from_bytes(res[offset: offset + 4], byteorder="big", signed=True)
    offset += 4
    content = str(res[offset: offset + content_size])
    offset += content_size
    return content, offset


# 读取 payload
def read_res_payload(res: bytes, offset: int):
    payload_size = int.from_bytes(res[offset: offset + 4], byteorder="big", signed=True)
    offset += 4
    payload = res[offset: offset + payload_size]
    offset += payload_size
    return payload, offset


# 解析响应结果
def parser_response(res) -> Response:
    if isinstance(res, str):
        raise RuntimeError(res)
    response = Response(Header(), Optional())
    # 解析结果
    # header
    header = response.header
    num = 0b00001111
    header.protocol_version = res[0] >> 4 & num
    header.header_size = res[0] & 0x0f
    header.message_type = (res[1] >> 4) & num
    header.message_type_specific_flags = res[1] & 0x0f
    header.serialization_method = res[2] >> num
    header.message_compression = res[2] & 0x0f
    header.reserved = res[3]
    #
    offset = 4
    optional = response.optional
    if header.message_type == FULL_SERVER_RESPONSE or AUDIO_ONLY_RESPONSE:
        # read event
        if header.message_type_specific_flags == MsgTypeFlagWithEvent:
            optional.event = int.from_bytes(res[offset:offset + 4], byteorder="big", signed=True)
            offset += 4
            if optional.event == EVENT_NONE:
                return response
            # read connectionId
            elif optional.event == EVENT_ConnectionStarted:
                optional.connectionId, offset = read_res_content(res, offset)
            elif optional.event == EVENT_ConnectionFailed:
                optional.response_meta_json, offset = read_res_content(res, offset)
            elif (optional.event == EVENT_SessionStarted
                  or optional.event == EVENT_SessionFailed
                  or optional.event == EVENT_SessionFinished):
                optional.sessionId, offset = read_res_content(res, offset)
                optional.response_meta_json, offset = read_res_content(res, offset)
            else:
                optional.sessionId, offset = read_res_content(res, offset)
                response.payload, offset = read_res_payload(res, offset)

    elif header.message_type == ERROR_INFORMATION:
        optional.errorCode = int.from_bytes(res[offset:offset + 4], byteorder="big", signed=True)
        offset += 4
        response.payload, offset = read_res_payload(res, offset)
    return response


async def run_demo(appId: str, token: str, speaker: str, text: str, output_path: str):
    ws_header = {
        "X-Api-App-Key": appId,
        "X-Api-Access-Key": token,
        "X-Api-Resource-Id": 'volc.service_type.10029',
        "X-Api-Connect-Id": uuid.uuid4(),
    }
    url = 'wss://openspeech.bytedance.com/api/v3/tts/bidirection'
    # websocket.create_connection(url,ws_header) as ws

    async with websockets.connect(url, additional_headers=ws_header, max_size=1000000000) as ws:
        await start_connection(ws)
        res = parser_response(await ws.recv())
        print_response(res, 'start_connection res:')
        if res.optional.event != EVENT_ConnectionStarted:
            raise RuntimeError("start connection failed")

        session_id = uuid.uuid4().__str__().replace('-', '')
        await start_session(ws, speaker, session_id)
        res = parser_response(await ws.recv())
        print_response(res, 'start_session res:')
        if res.optional.event != EVENT_SessionStarted:
            raise RuntimeError('start session failed!')

        # 发送文本
        await send_text(ws, speaker, text, session_id)
        await finish_session(ws, session_id)
        async with aiofiles.open(output_path, mode="wb") as output_file:
            while True:
                res = parser_response(await ws.recv())
                print_response(res, 'send_text res:')
                if res.optional.event == EVENT_TTSResponse and res.header.message_type == AUDIO_ONLY_RESPONSE:
                    await output_file.write(res.payload)
                elif res.optional.event in [EVENT_TTSSentenceStart, EVENT_TTSSentenceEnd]:
                    continue
                else:
                    break
        await finish_connection(ws)
        res = parser_response(await ws.recv())
        print_response(res, 'finish_connection res:')
        print('===> 退出程序')


def print_response(res, tag: str):
    print(f'===>{tag} header:{res.header.__dict__}')
    print(f'===>{tag} optional:{res.optional.__dict__}')


def get_payload_bytes(uid='1234', event=EVENT_NONE, text='', speaker='', audio_format='mp3',
                      audio_sample_rate=24000):
    return str.encode(json.dumps(
        {
            "user": {"uid": uid},
            "event": event,
            "namespace": "BidirectionalTTS",
            "req_params": {
                "text": text,
                "speaker": speaker,
                "audio_params": {
                    "format": audio_format,
                    "sample_rate": audio_sample_rate
                }
            }
        }
    ))


async def start_connection(websocket):
    header = Header(message_type=FULL_CLIENT_REQUEST, message_type_specific_flags=MsgTypeFlagWithEvent).as_bytes()
    optional = Optional(event=EVENT_Start_Connection).as_bytes()
    payload = str.encode("{}")
    return await send_event(websocket, header, optional, payload)


async def start_session(websocket, speaker, session_id):
    header = Header(message_type=FULL_CLIENT_REQUEST,
                    message_type_specific_flags=MsgTypeFlagWithEvent,
                    serial_method=JSON
                    ).as_bytes()
    optional = Optional(event=EVENT_StartSession, sessionId=session_id).as_bytes()
    payload = get_payload_bytes(event=EVENT_StartSession, speaker=speaker)
    return await send_event(websocket, header, optional, payload)


async def send_text(ws: ClientConnection, speaker: str, text: str, session_id):
    header = Header(message_type=FULL_CLIENT_REQUEST,
                    message_type_specific_flags=MsgTypeFlagWithEvent,
                    serial_method=JSON).as_bytes()
    optional = Optional(event=EVENT_TaskRequest, sessionId=session_id).as_bytes()
    payload = get_payload_bytes(event=EVENT_TaskRequest, text=text, speaker=speaker)
    return await send_event(ws, header, optional, payload)


async def finish_session(ws, session_id):
    header = Header(message_type=FULL_CLIENT_REQUEST,
                    message_type_specific_flags=MsgTypeFlagWithEvent,
                    serial_method=JSON
                    ).as_bytes()
    optional = Optional(event=EVENT_FinishSession, sessionId=session_id).as_bytes()
    payload = str.encode('{}')
    return await send_event(ws, header, optional, payload)


async def finish_connection(ws):
    header = Header(message_type=FULL_CLIENT_REQUEST,
                    message_type_specific_flags=MsgTypeFlagWithEvent,
                    serial_method=JSON
                    ).as_bytes()
    optional = Optional(event=EVENT_FinishConnection).as_bytes()
    payload = str.encode('{}')
    return await send_event(ws, header, optional, payload)


if __name__ == "__main__":
    load_dotenv()
    appId = os.getenv("APP_ID")
    token = os.getenv("TOKEN")

    speaker = 'zh_female_shuangkuaisisi_moon_bigtts'
    text = '明朝开国皇帝朱元璋也称这本书为,万物之根'
    output_path = 'output.mp3'
    asyncio.run(run_demo(appId, token, speaker, text, output_path))
