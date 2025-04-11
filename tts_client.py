import asyncio
import json
import os
import uuid

import websockets

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
    output_file = f"output_combined_{uuid.uuid4()}.pcm"
    try:
        # 创建输出文件
        print(f"音频将保存到: {output_file}")

        async for message in ws:
            print(f"收到消息: {message[:100]}...")  # 只打印消息的前100个字符

            response_data = json.loads(message)
            # 检查是否是音频数据
            if 'audio_data' in response_data and response_data.get('status') == 'success':
                # 将字符串转换为二进制数据
                audio_data = response_data['audio_data'].encode("latin1")

                # 将音频数据追加到文件
                with open(output_file, 'ab') as f:
                    f.write(audio_data)
                print(f"已保存音频数据片段 ({len(audio_data)} 字节)")
            elif 'error' in response_data:
                print(f"服务器返回错误: {response_data['error']}")
            elif 'message' in response_data:
                print(f"服务器返回信息: {response_data['message']}")
            else:
                print(f"收到非音频响应: {response_data.get('action', 'unknown')}")
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
