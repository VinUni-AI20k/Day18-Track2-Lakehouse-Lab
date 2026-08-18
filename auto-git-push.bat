@echo off
cd /d "C:\Users\R7000P\Day18-Track2-NguyenMinhPhuong-2A202601947"

git add -A

git diff --cached --quiet
if %errorlevel% equ 0 exit /b 0

git commit -m "auto commit %date% %time%"
git push

exit /b %errorlevel%