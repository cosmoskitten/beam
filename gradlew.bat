@echo off

set CMD_LINE_ARGS=%*
set ORG_CMD_LINE_ARGS=%*

echo Executing
echo.
echo   gradlew %CMD_LINE_ARGS%
echo.

for /F "tokens=1,2*" %%i in (project-mappings) do call :process %%i %%j

if not "%ORG_CMD_LINE_ARGS%" == "%CMD_LINE_ARGS%" (
  type deprecation-warning.txt

  echo Changed command to
  echo.
  echo   gradlew %CMD_LINE_ARGS%
  echo.
)

gradlew_orig.bat %CMD_LINE_ARGS%
EXIT /B 0


:process
set VAR1=%1
set VAR2=%2
call set CMD_LINE_ARGS=%%CMD_LINE_ARGS:%VAR1%=%VAR2%%%
EXIT /B 0

