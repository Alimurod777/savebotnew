param(
    [switch]$IncludeVenv,
    [switch]$IncludeLogs
)

$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $repoRoot

$targets = @(
    '.pytest_cache',
    '.mypy_cache',
    '.ruff_cache'
)

foreach ($target in $targets) {
    if (Test-Path -LiteralPath $target) {
        Remove-Item -LiteralPath $target -Recurse -Force
    }
}

Get-ChildItem -Recurse -Directory -Force |
    Where-Object {
        $_.Name -eq '__pycache__' -and ($IncludeVenv -or $_.FullName -notlike "*\venv\*")
    } |
    Remove-Item -Recurse -Force

Get-ChildItem -Recurse -File -Force -Include *.pyc, *.pyo |
    Where-Object { $IncludeVenv -or $_.FullName -notlike "*\venv\*" } |
    Remove-Item -Force

if ($IncludeLogs) {
    Get-ChildItem -Recurse -File -Force -Include *.log |
        Where-Object { $IncludeVenv -or $_.FullName -notlike "*\venv\*" } |
        Remove-Item -Force
}

Write-Host "Cache tozalandi."
if ($IncludeVenv) {
    Write-Host "venv ichidagi pyc/pyo fayllar ham tozalandi."
} else {
    Write-Host "venv ichidagi fayllar qoldirildi. Ularni ham tozalash uchun: .\clear_cache.ps1 -IncludeVenv"
}
if ($IncludeLogs) {
    Write-Host "Log fayllar ham tozalandi."
} else {
    Write-Host "Log fayllarni ham tozalash uchun: .\clear_cache.ps1 -IncludeLogs"
}
