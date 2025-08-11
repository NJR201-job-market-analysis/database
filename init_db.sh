#!/bin/sh
# 讓腳本在遇到任何錯誤時立即退出
set -e

# 等待 MySQL 準備就緒
# 使用 nc (netcat) 命令來測試 mysql 服務的 3306 連接埠是否開啟
echo "Waiting for MySQL to be ready..."
while ! nc -z mysql 3306; do
  echo "MySQL is unavailable - sleeping"
  sleep 1
done

echo "MySQL is up - executing database initialization..."

# 當連線成功後，初始化資料庫
# 注意：這裡我們可以直接用 python，因為 Dockerfile 中用了 --system
python -m init_db

echo "Database initialization complete."