import asyncio
import json
import uuid

import websockets

# WebSocket 服务器地址
WS_SERVER_URI = "ws://localhost:8765"  # 替换成你的 WebSocket 服务端地址

text_segments = ['从前有个可', '爱的小姑娘，', '谁见了都喜欢，但最喜欢', '她的是她的奶奶，简直是她要什么就给她什么。',
                 '一次，奶奶送给', '小姑娘一顶用丝绒做的小红帽，戴在她的头上', '正好合适。从此，',
                 '小姑娘再也不愿意戴任',
                 '何别的帽子，于是大家便叫她', '小红帽，一天', ' ，妈妈对小红帽说："来，', '小红帽，这里有一块蛋糕和一瓶',
                 '葡萄酒，快给奶奶', '送去，奶奶生病了，身子很虚弱，吃了这', '些就会好一些的。趁着现在天还没有热，',
                 '赶紧动身吧。在路上要好好走，不要跑，', '也不要离开大路，否则你会摔跤的', '，那样奶奶就什么也吃不上了',
                 '。到奶奶家的时候，别忘', '了说早上好', '也不', '要一进屋就东瞧西瞅。"']


async def send_messages(ws):
    """异步发送消息给服务端"""
    session_id = str(uuid.uuid4())

    start_request = {'text': "",
                     'request_id': session_id,
                     'action': 'start'}

    end_request = {'text': "",
                   'request_id': session_id,
                   'action': 'end'}

    await ws.send(json.dumps(start_request))

    print("已发送开始会话请求")

    for text_segment in text_segments:
        await ws.send(json.dumps({
            'text': text_segment,
            'request_id': session_id,
            'action': 'synthesize',
        }))
        print(f"发送: {text_segment}")

    await ws.send(json.dumps(end_request))

    # while True:
    #     msg = input("请输入要发送的消息（输入 exit 退出）：")
    #     if msg.lower() == "exit":
    #         print("退出发送任务。")
    #         await ws.send(json.dumps(end_request))
    #         break
    #     await ws.send(json.dumps({'text': msg,
    #                             'request_id': session_id,
    #                             'action': 'synthesize'}))
    #     print(f"发送: {msg}")


async def receive_messages(ws):
    """持续监听服务端消息"""
    try:
        # 创建输出文件
        output_file = f"output_combined_{uuid.uuid4()}.mp3"
        print(f"音频将保存到: {output_file}")
        
        while True:
            message = await ws.recv()
            print(f"收到消息: {message[:100]}...")  # 只打印消息的前100个字符
            
            # 解析消息
            try:
                response_data = json.loads(message)
                
                # 检查是否是音频数据
                if 'audio_data' in response_data and response_data.get('status') == 'success':
                    # 将十六进制字符串转换为二进制数据
                    audio_data = bytes.fromhex(response_data['audio_data'])
                    
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
            except json.JSONDecodeError:
                print("收到非JSON格式消息")
            except Exception as e:
                print(f"处理消息时出错: {str(e)}")
    except websockets.exceptions.ConnectionClosed:
        print("连接已关闭，停止接收。")
    except Exception as e:
        print(f"接收消息时发生错误: {str(e)}")
    finally:
        print(f"音频已保存到: {output_file}")


async def main():
    async with websockets.connect(WS_SERVER_URI) as websocket:
        print("已连接到服务器。")

        # 使用gather同时执行所有任务
        await asyncio.gather(
            send_messages(websocket),
            receive_messages(websocket)
        )


if __name__ == "__main__":
    asyncio.run(main())
