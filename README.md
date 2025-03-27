# Volcano TTS
> customize api by volcano speech for sample development

**基于[火山语音合成服务](https://www.volcengine.com/docs/6561/1257584)封装的在线语音合成服务**

## 部署
1. 克隆仓库到本地，运行main.py即可本地运行
2. 基于DockFile可构建自定义的docker镜像，按需选择暴露端口

## 使用
> 使用前请确认已经在[火山控制台](https://www.volcengine.com/docs/6561/196768)申请到相关appid，cluster，token。
1. 根据 `.env.example` 文件建立 `.env`，修改 `APP_ID` 和 `TOKEN`为可用的值。

2. 运行main.py，即可在本地建立fastapi服务，在本地浏览器访问以下端口即可体验TTS合成服务：
    > http://localhost:10012/synthesize?text="你好，这是火山TTS在线音频测试。"

3. 请求参数
   ```python
   text: str                #待合成的文本，必须
   voice_type: str = "ICL_zh_female_huoponvhai_tob" # 合成音色，部分音色可能不可用，如个人账户使用大模型音色
   emotion: str = "xxx"     # 音色情感
   req_id: str = None       # 默认 None，内部生成 
   rate: int = 24000        # 合成音频的码率
   speed: float = 1.1       # 速率
   volume: float = 1.0      # 音量
   pitch: float = 1.0       # 音高
   encoding: str = "mp3"    #编码，可选 "mp3/pcm"，指定为pcm将返回音频数据
   ```