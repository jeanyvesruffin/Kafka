@echo off
REM Demarrage du broker Kafka en KRaft, mono-noeud, sans droits admin.
REM A utiliser a partir de la phase 1 du TODO-KAFKA.

if "%KAFKA_HOME%"=="" set KAFKA_HOME=C:\kafka_2.13-4.3.1

if not exist "%KAFKA_HOME%" (
echo KAFKA_HOME introuvable : %KAFKA_HOME%
echo Decompresse kafka_2.13-4.3.1.tgz puis definis KAFKA_HOME.
exit /b 1
)

echo Demarrage du broker depuis %KAFKA_HOME% (Ctrl+C pour arreter)
call "%KAFKA_HOME%\bin\windows\kafka-server-start.bat" "%KAFKA_HOME%\config\server.properties"
