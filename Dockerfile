# 使用官方的 Python 3.9 slim 版本作为基础镜像
FROM python:3.9-slim

# 设置工作目录
WORKDIR /app

# 复制依赖项文件
COPY requirements.txt .

# 安装 Python 依赖项
RUN pip install -i https://pypi.mirrors.ustc.edu.cn/simple requests --no-cache-dir -r requirements.txt
#RUN pip install --no-cache-dir -r requirements.txt

# 复制应用程序代码
COPY . .

# 暴露端口
EXPOSE 10012

# 运行命令
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "10012"]
