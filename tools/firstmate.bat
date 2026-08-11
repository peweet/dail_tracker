@echo off
setlocal

if /I "%~1"=="doctor" goto doctor
if /I "%~1"=="repair" goto repair

wsl.exe -d Ubuntu --cd /home/pglyn/firstmate -- env PATH=/home/pglyn/.local/bin:/usr/local/bin:/usr/bin:/bin /home/pglyn/.local/bin/pi %*
exit /b %errorlevel%

:doctor
set "FAILED=0"
echo WSL user:
wsl.exe -d Ubuntu -- id -un
if errorlevel 1 set "FAILED=1"
echo Pi:
wsl.exe -d Ubuntu -- /home/pglyn/.local/bin/pi --version
if errorlevel 1 set "FAILED=1"
echo Firstmate revision:
wsl.exe -d Ubuntu -- git -C /home/pglyn/firstmate rev-parse --short HEAD
if errorlevel 1 set "FAILED=1"
echo tmux:
wsl.exe -d Ubuntu -- tmux -V
if errorlevel 1 set "FAILED=1"
echo GitHub CLI:
wsl.exe -d Ubuntu -- gh auth status
if errorlevel 1 set "FAILED=1"
echo Codex bridge:
wsl.exe -d Ubuntu -- /home/pglyn/.local/bin/codex --version
if errorlevel 1 set "FAILED=1"
echo Claude bridge:
wsl.exe -d Ubuntu -- /home/pglyn/.local/bin/claude --version
if errorlevel 1 set "FAILED=1"
echo Pi OpenAI-Codex authentication:
wsl.exe -d Ubuntu -- /home/pglyn/.local/bin/pi auth check --provider openai-codex --no-refresh --json
if errorlevel 1 set "FAILED=1"
echo Firstmate local bootstrap:
set "BOOTSTRAP_OUT=%TEMP%\firstmate-bootstrap-%RANDOM%.txt"
wsl.exe -d Ubuntu -- env PATH=/home/pglyn/.local/bin:/usr/local/bin:/usr/bin:/bin FM_BOOTSTRAP_DETECT_ONLY=1 FM_BOOTSTRAP_NETWORK=skip /home/pglyn/firstmate/bin/fm-bootstrap.sh > "%BOOTSTRAP_OUT%" 2>&1
set "BOOTSTRAP_STATUS=%ERRORLEVEL%"
type "%BOOTSTRAP_OUT%"
if not "%BOOTSTRAP_STATUS%"=="0" set "FAILED=1"
findstr /B /C:"MISSING:" /C:"MISSING_MANUAL:" /C:"NEEDS_GH_AUTH" /C:"BACKEND_INVALID:" /C:"CREW_DISPATCH:" /C:"STARTUP_MEMORY_BUDGET:" /C:"TANGLE:" "%BOOTSTRAP_OUT%" >nul
if not errorlevel 1 set "FAILED=1"
del /q "%BOOTSTRAP_OUT%"
echo.
if "%FAILED%"=="0" (
  echo Firstmate launch and bootstrap prerequisites are ready.
) else (
  echo Firstmate is not ready. Resolve the failed bridge or bootstrap action above, then re-run doctor.
)
exit /b %FAILED%

:repair
wsl.exe -d Ubuntu -- /bin/bash /mnt/c/Users/pglyn/PycharmProjects/dail_extractor/tools/setup_firstmate_agents.sh
exit /b %errorlevel%
