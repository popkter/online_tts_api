# 使用官方的 Python 3.9 slim 版本作为基础镜像
FROM python:3.9-slim

# 设置工作目录
WORKDIR /app

# 复制依赖项文件
COPY . .

# 安装依赖包
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libffi-dev \
    libssl-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# 安装 Python 依赖项
RUN pip install -i https://pypi.tuna.tsinghua.edu.cn/simple --no-cache-dir -r requirements.txt

# 暴露端口
EXPOSE 10013

CMD ["python", "tts_server.py"]

# 运行命令
#CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "10012"]


## 使用 Python 3.9 作为基础镜像
#FROM python:3.9-slim
#
## 设置工作目录
#WORKDIR /app
#
## 设置环境变量
#ENV PYTHONUNBUFFERED=1
#ENV TZ=Asia/Shanghai
#ENV EXPOSED_PORT=10013
#
## 安装系统依赖
#RUN apt-get update && apt-get install -y \
#    build-essential \
#    && rm -rf /var/lib/apt/lists/*
#
## 复制项目文件
#COPY requirements.txt .
#COPY websocket_tts_server_proxy.py .
#COPY websocket_tts_client.py .
#COPY tts_ext.py .
#
#COPY .env .
#
## 安装 Python 依赖
#RUN pip install --no-cache-dir -r requirements.txt
#
## 暴露端口
#EXPOSE 10013
#
## 启动服务
#CMD ["python", "tts_server.py"]
