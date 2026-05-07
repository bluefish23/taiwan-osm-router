@echo off
chcp 65001 >nul
echo === Taiwan OSM Router + Cloudflare Tunnel ===
echo.

set CF="C:\Program Files (x86)\cloudflared\cloudflared.exe"

python --version >nul 2>&1 || (echo [ERROR] 需要安裝 Python 3.11+ && pause && exit /b 1)
if not exist %CF% (echo [ERROR] 需要安裝 cloudflared: winget install Cloudflare.cloudflared && pause && exit /b 1)

if not exist .env (
    echo [ERROR] 請先複製 env.example 為 .env 並填入 TDX / CWA API keys
    pause
    exit /b 1
)

echo 安裝套件...
pip install -r requirements.txt -q

echo.
echo 啟動 Cloudflare Tunnel (背景)...
start /b "" %CF% tunnel --url http://localhost:8000

echo.
echo 啟動 API 伺服器 (http://localhost:8000)...
echo Tunnel URL 會顯示在上方 (https://xxx.trycloudflare.com)
echo 按 Ctrl+C 停止
echo.
uvicorn osm_api:app --host 0.0.0.0 --port 8000
