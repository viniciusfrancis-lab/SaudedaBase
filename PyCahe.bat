@echo off
del /s /q *.pyc >nul 2>&1
del /s /q *.pyo >nul 2>&1

for /d /r . %%d in (__pycache__) do (
    if exist "%%d" rd /s /q "%%d"
)

echo Cache Python removido.
pause