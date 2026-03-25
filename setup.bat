@echo off
echo === Setup Multi-Agent MCP ===
echo.

echo [1/3] Instalando dependencias Python...
pip install playwright mcp fastmcp
if %ERRORLEVEL% NEQ 0 (
    echo ERRO: Falha ao instalar dependencias
    pause
    exit /b 1
)

echo.
echo [2/3] Instalando Chromium (Playwright)...
playwright install chromium
if %ERRORLEVEL% NEQ 0 (
    echo ERRO: Falha ao instalar Chromium
    pause
    exit /b 1
)

echo.
echo [3/3] Testando servidor MCP...
python -c "from perplexity import MODEL_MAP; print('OK - modelos:', list(MODEL_MAP.keys()))"

echo.
echo === Setup concluido! ===
echo.
echo PROXIMO PASSO:
echo 1. Reinicie o Claude Code para carregar o MCP multi-agent
echo 2. Na primeira consulta, um browser vai abrir - faca login no Perplexity
echo 3. Feche o browser apos login - o perfil fica salvo em ./profile
echo.
pause
