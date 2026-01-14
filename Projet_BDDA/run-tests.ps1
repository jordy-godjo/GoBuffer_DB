# Run Go tests sequentially to avoid parallel-package I/O contention on CI/dev machines
Write-Host "Running 'go test ./... -p 1' from project root"
Set-Location -Path $PSScriptRoot
go test ./... -p 1
