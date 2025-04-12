import asyncio
import json
import os
import uuid

import websockets

from tts_ext import EVENT_TTSResponse, EVENT_TTSSentenceStart, EVENT_TTSSentenceEnd, EVENT_SessionStarted, \
    EVENT_SessionFinished, EVENT_StartSession, EVENT_FinishSession, EVENT_ConnectionStarted, EVENT_Start_Connection, \
    EVENT_ConnectionFailed, EVENT_FinishConnection, EVENT_ConnectionFinished

# WebSocket 服务器地址
WS_SERVER_URI = "ws://localhost:10013"  # 替换成你的 WebSocket 服务端地址

text_segments = ['从前有个可', '爱的小姑娘，', '谁见了都喜欢']


async def send_messages(ws):
    session_id = str(uuid.uuid4())

    # 启动新一轮语音合成
    start_request = {
        "device_id": "caf",
        "request_id": session_id,
        "action": "start",
        # 音频格式,默认mp3
        "audio_format": "pcm",
        # 采样率，默认24000
        "sample_rate": 24000,
        # 语速，默认50
        "speech_rate": 50,
        # 音量，默认0
        "loudness_rate": 0,
        # 发音人，默认 zh_female_shuangkuaisisi_moon_bigtts
        "voice_type": "zh_male_yangguangqingnian_emo_v2_mars_bigtts"
    }

    await ws.send(json.dumps(start_request))

    # 开始合成音频
    for text_segment in text_segments:
        await asyncio.sleep(0.5)
        await ws.send(json.dumps({
            "text": text_segment,
            "request_id": session_id,
            "action": "synthesize",
        }))
        print(f'text_segment -> {text_segment}')

    # 结束一轮合成
    end_request = {
        "request_id": session_id,
        "action": "end",
    }

    await ws.send(json.dumps(end_request))


async def receive_messages(ws: websockets):
    """持续监听服务端消息"""

    output_file = 'output_request_id.pcm'

    try:
        # 创建输出文件

        async for message in ws:
            print(f"收到消息: {message[:100]}...")  # 只打印消息的前100个字符

            response_data = json.loads(message)
            # 检查是否是音频数据
            request_id = response_data["request_id"]
            event = response_data["event"]
            data = response_data["data"]

            if event == EVENT_SessionStarted:
                output_file = f'output_{request_id}.pcm'
                print(f"音频将保存到: {output_file}")
                # 清空或新建文件
                with open(output_file, 'wb'):
                    pass  # 创建空文件
                print('会话已经开始')

            elif event == EVENT_TTSResponse:
                # 追加写入音频数据
                audio_data = data.encode("latin1")
                with open(output_file, 'ab') as f:
                    f.write(audio_data)

            elif event == EVENT_TTSSentenceStart:
                print('返回句内容开始')

            elif event == EVENT_TTSSentenceEnd:
                print('返回句内容结束')

            elif event == EVENT_SessionFinished:
                print('会话已经结束')
                output_file = None  # 会话结束，重置 output_file

            elif event == EVENT_ConnectionStarted:
                print('链接建立成功')
            elif event == EVENT_ConnectionFailed:
                print('链接建立失败')

            elif event == EVENT_ConnectionFinished:
                print('连接已经断开')
            else:
                print(f'未处理的事件: {event}')

    except websockets.exceptions.ConnectionClosed:
        print("连接已关闭，停止接收。")
    except Exception as e:
        print(f"接收消息时发生错误: {str(e)}")
    finally:
        print(f"音频已保存到: {output_file}")


from dotenv import load_dotenv

# 加载环境变量
load_dotenv()


async def main():
    from dotenv import load_dotenv
    # 加载环境变量
    load_dotenv()

    token = os.getenv("TOKEN")

    async with websockets.connect(WS_SERVER_URI, additional_headers={"Authorization": f"Bearer {token}"}) as ws:
        print("已连接到服务器。")

        receive_task = asyncio.create_task(receive_messages(ws))
        await send_messages(ws)
        await receive_task  # 等待接收结束

if __name__ == "__main__":
    asyncio.run(main())
