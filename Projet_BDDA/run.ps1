# PowerShell script to run the minisgbd using go run (bypasses Windows Defender issues)
Param(
    [string]$Config = "config.txt"
)

Push-Location $PSScriptRoot
go run ./src -config $Config
Pop-Location
