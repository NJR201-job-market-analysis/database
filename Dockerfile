# 使用官方的、輕量級的 Python 映像檔
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends netcat-openbsd && \
    rm -rf /var/lib/apt/lists/*

# 設定語系，避免編碼問題
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

# 安裝 pipenv
RUN pip install pipenv==2022.4.8

COPY . .

RUN pipenv sync --system

RUN chmod +x init_db.sh

RUN ENV=DOCKER python genenv.py

# CMD ["python", "init_db.py"]
# 將 init_db.sh 設為容器的進入點
ENTRYPOINT ["./init_db.sh"]