@echo off
setlocal
rem ==============================================================================
rem StratosDB Command-Line SQL Client (stdsql)
rem Speaks PostgreSQL wire protocol v3 natively
rem ==============================================================================

set "DIR=%~dp0"
if exist "%DIR%runtime\bin\java.exe" (
    set "JAVA_EXE=%DIR%runtime\bin\java.exe"
) else (
    set "JAVA_EXE=java"
)

set "JAR_PATH=%DIR%lib\app\stratosdb-cli.jar"
if not exist "%JAR_PATH%" (
    set "JAR_PATH=%DIR%stratosdb-cli.jar"
)

"%JAVA_EXE%" -jar "%JAR_PATH%" %*
