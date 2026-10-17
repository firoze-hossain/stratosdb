@echo off
setlocal
rem ==============================================================================
rem StratosDB Windows Service Management Helper
rem Usage:
rem   stratosdb-service.bat install [port] [data_dir]
rem   stratosdb-service.bat start
rem   stratosdb-service.bat stop
rem   stratosdb-service.bat restart
rem   stratosdb-service.bat status
rem   stratosdb-service.bat remove
rem ==============================================================================

set "DIR=%~dp0"
set "EXE=%DIR%stratosdb.exe"
set "CONF=%DIR%stratosdb.conf"
set "SVC_NAME=StratosDB"

if "%1"=="install" goto :install
if "%1"=="start" goto :start
if "%1"=="stop" goto :stop
if "%1"=="restart" goto :restart
if "%1"=="status" goto :status
if "%1"=="remove" goto :remove

echo StratosDB Windows Service Manager
echo ---------------------------------
echo Usage: stratosdb-service.bat [install ^| start ^| stop ^| restart ^| status ^| remove]
goto :end

:install
echo Registering StratosDB Windows Service...
sc.exe create %SVC_NAME% binPath= "\"%EXE%\" --config \"%CONF%\"" start= auto DisplayName= "StratosDB Database Server"
sc.exe description %SVC_NAME% "StratosDB Relational Database Engine"
echo Service registered. Run 'stratosdb-service.bat start' to start.
goto :end

:start
echo Starting StratosDB service...
sc.exe start %SVC_NAME%
goto :end

:stop
echo Stopping StratosDB service...
sc.exe stop %SVC_NAME%
goto :end

:restart
echo Restarting StratosDB service...
sc.exe stop %SVC_NAME%
timeout /t 2 /nobreak >nul
sc.exe start %SVC_NAME%
goto :end

:status
sc.exe query %SVC_NAME%
goto :end

:remove
echo Unregistering StratosDB service...
sc.exe stop %SVC_NAME% >nul 2>&1
sc.exe delete %SVC_NAME%
echo Service unregistered.
goto :end

:end
