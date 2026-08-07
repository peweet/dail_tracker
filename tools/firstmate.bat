@echo off
setlocal

if /I "%~1"=="doctor" goto doctor

wsl.exe -d Ubuntu --cd /home/pglyn/firstmate -- /home/pglyn/.local/bin/pi
exit /b %errorlevel%

:doctor
echo WSL user:
wsl.exe -d Ubuntu -- id -un
echo Pi:
wsl.exe -d Ubuntu -- /home/pglyn/.local/bin/pi --version
echo Firstmate revision:
wsl.exe -d Ubuntu -- git -C /home/pglyn/firstmate rev-parse --short HEAD
echo tmux:
wsl.exe -d Ubuntu -- tmux -V
echo GitHub CLI:
wsl.exe -d Ubuntu -- gh auth status
echo.
echo Complete any ACTION above, then run firstmate.
exit /b 0
