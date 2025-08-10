# 1. 使用官方的、輕量級的 Python 映像檔
FROM python:3.11-slim

# 2. 設定工作目錄為 /database
WORKDIR /database

# 3. 設定語系，避免編碼問題
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

# 4. 安裝 pipenv
RUN pip install pipenv==2022.4.8

# 5. 複製依賴定義檔到 /database
# 為了利用 Docker 的快取機制，先複製這兩個檔案
COPY Pipfile Pipfile.lock ./

# 6. 在 /database 中安裝所有依賴
# 使用 --system 會將套件安裝到系統的 Python 環境，在容器中更直接
RUN pipenv sync --system

# 7. 複製您本地 `database` 資料夾中的所有內容到容器的 /database 目錄
COPY . .

# 8. 執行腳本來產生 .env 檔案
# `pipenv run` 會在正確的環境中執行
RUN python genenv.py

# 9. 設定容器的最終執行命令
# 使用 `pipenv run` 來啟動您的初始化腳本。
# 腳本執行完畢後，容器會成功退出，Portainer 就會顯示 'Complete'。
CMD ["python", "init_db.py"]