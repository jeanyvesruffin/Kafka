@echo off
setlocal
REM Demarrage du broker Kafka en KRaft, mono-noeud, sans droits admin.
REM A utiliser a partir de la phase 1 du TODO-KAFKA.
REM Usage : start-kafka.cmd [chemin du JDK]  (sans argument, le script le demande)

if "%KAFKA_HOME%"=="" set "KAFKA_HOME=C:\kafka_2.13-4.3.1"
if "%KAFKA_LOG_DIRS%"=="" set "KAFKA_LOG_DIRS=C:\kafka-logs"

if not exist "%KAFKA_HOME%" (
echo KAFKA_HOME introuvable : %KAFKA_HOME%
echo Decompresse kafka_2.13-4.3.1.tgz puis definis KAFKA_HOME.
exit /b 1
)

REM --- JDK a utiliser ---------------------------------------------------------
REM Aucun droit admin requis : grace a setlocal, JAVA_HOME n'est defini que le
REM temps de ce script, la variable d'environnement Windows n'est pas modifiee.
set "DEFAULT_JDK=%USERPROFILE%\jdk-25.0.4.1+1"
if not "%JAVA_HOME%"=="" set "DEFAULT_JDK=%JAVA_HOME%"
set "JDK_PATH=%~1"

:ask_jdk
if not "%JDK_PATH%"=="" goto check_jdk
echo.
echo Chemin du JDK a utiliser pour Kafka (Entree = valeur par defaut) :
set "JDK_PATH="
set /p "JDK_PATH=[%DEFAULT_JDK%] : "
if defined JDK_PATH set JDK_PATH=%JDK_PATH:"=%
if "%JDK_PATH%"=="" set "JDK_PATH=%DEFAULT_JDK%"

:check_jdk
if exist "%JDK_PATH%\bin\java.exe" goto jdk_ok
echo Introuvable : %JDK_PATH%\bin\java.exe
set "JDK_PATH="
goto ask_jdk

:jdk_ok
set "JAVA_HOME=%JDK_PATH%"
echo JDK utilise : %JAVA_HOME%

REM Les JDK 24+ affichent des WARNING sun.misc.Unsafe (Kafka s'en sert pour
REM liberer les fichiers mappes en memoire). L'option suivante les fait taire ;
REM elle n'existe pas avant le JDK 23, donc on verifie qu'elle est reconnue.
"%JAVA_HOME%\bin\java" --sun-misc-unsafe-memory-access=allow -version >nul 2>&1
if not errorlevel 1 set "KAFKA_OPTS=--sun-misc-unsafe-memory-access=allow %KAFKA_OPTS%"

REM Windows refuse de supprimer un fichier marque en lecture seule. Kafka renomme
REM ses vieux snapshots Raft en *.deleted et echoue a les purger au demarrage
REM (AccessDeniedException fatale). On leve l'attribut et on les supprime avant.
if exist "%KAFKA_LOG_DIRS%" for /r "%KAFKA_LOG_DIRS%" %%f in (*.deleted) do (attrib -R "%%f" & del /f /q "%%f")

echo Demarrage du broker depuis %KAFKA_HOME% (Ctrl+C pour arreter)
call "%KAFKA_HOME%\bin\windows\kafka-server-start.bat" "%KAFKA_HOME%\config\server.properties"
