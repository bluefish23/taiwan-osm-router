@echo off
chcp 65001 >nul
echo === 建置 Vercel 前端 ===

if "%~1"=="" (
    echo 用法: build_frontend.bat ^<你的tunnel URL^>
    echo 範例: build_frontend.bat https://osm-router.trycloudflare.com
    exit /b 1
)

set API_URL=%~1

if not exist frontend mkdir frontend

echo 複製 admin.html...
copy /y admin.html frontend\admin.html >nul

echo 注入 API_BASE 到 index.html...
(echo ^<script^>window.__API_BASE="%API_URL%";^</script^>) > frontend\index.html
type index.html >> frontend\index.html

echo.
echo 完成! frontend/ 目錄已備妥:
echo   frontend\index.html  (API_BASE = %API_URL%)
echo   frontend\admin.html
echo   frontend\vercel.json
echo.
echo 部署: cd frontend ^&^& npx vercel --prod
