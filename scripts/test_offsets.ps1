# Integration Test for Offset Management (Seek & Commit)
# This script verifies that:
# 1. Poll advances the fetch offset.
# 2. Seek can reset the offset to re-read messages.

$ErrorActionPreference = "Stop"
$Topic = "offset-test-topic"
$LogFile = "offset_consumer.log"

# 1. Cleanup and Build
Write-Host "--- Step 1: Cleanup and Build ---" -ForegroundColor Cyan
make stop
if (Test-Path "./data") { Remove-Item "./data" -Recurse -Force }
if (Test-Path $LogFile) { Remove-Item $LogFile }
make build

# 2. Start Broker
Write-Host "--- Step 2: Starting Broker ---" -ForegroundColor Cyan
$BrokerProc = Start-Process -FilePath "./bin/broker.exe" -NoNewWindow -PassThru
Start-Sleep -Seconds 2

# 3. Create topic with 1 partition
Write-Host "--- Step 3: Creating topic ---" -ForegroundColor Cyan
$Body = @{ name = $Topic; partitions = 1 } | ConvertTo-Json
Invoke-RestMethod -Uri "http://localhost:8080/api/topics" -Method Post -Body $Body -ContentType "application/json"

# 4. Produce a message
Write-Host "--- Step 4: Producing message ---" -ForegroundColor Cyan
./bin/producer.exe -topic $Topic -key "msg-key" -message "Message-1"

# 5. Start Consumer and Poll
Write-Host "--- Step 5: Initial Poll ---" -ForegroundColor Cyan
$ConsumerProc = Start-Process -FilePath "./bin/consumer.exe" -ArgumentList "-topic $Topic -group group-A" -NoNewWindow -RedirectStandardOutput $LogFile -PassThru
Start-Sleep -Seconds 5
$ConsumerProc | Stop-Process -Force

Write-Host "Consumer log after first poll:"
Get-Content $LogFile

if (-not (Select-String -Path $LogFile -Pattern "Received: Message-1")) {
    Write-Host "FAILURE: Message-1 not received in first poll." -ForegroundColor Red
    exit 1
}

# 6. Second Poll (should be empty because offset advanced)
Write-Host "--- Step 6: Second Poll (should be empty) ---" -ForegroundColor Cyan
Clear-Content $LogFile
$ConsumerProc = Start-Process -FilePath "./bin/consumer.exe" -ArgumentList "-topic $Topic -group group-A" -NoNewWindow -RedirectStandardOutput $LogFile -PassThru
Start-Sleep -Seconds 3
$ConsumerProc | Stop-Process -Force

Write-Host "Consumer log after second poll:"
$content = Get-Content $LogFile
if ($content -contains "Received: Message-1") {
    Write-Host "FAILURE: Message-1 received again without Seek. Poll should have advanced offset." -ForegroundColor Red
    exit 1
}

# 7. Seek to 0 and Poll again (should receive Message-1 again)
Write-Host "--- Step 7: Seek to 0 and Poll ---" -ForegroundColor Cyan
Clear-Content $LogFile
# Use the new -seek flag
$ConsumerProc = Start-Process -FilePath "./bin/consumer.exe" -ArgumentList "-topic $Topic -group group-A -seek 0" -NoNewWindow -RedirectStandardOutput $LogFile -PassThru
Start-Sleep -Seconds 5
$ConsumerProc | Stop-Process -Force

Write-Host "Consumer log after Seek and Poll:"
Get-Content $LogFile

if (-not (Select-String -Path $LogFile -Pattern "Received: Message-1")) {
    Write-Host "FAILURE: Message-1 not received after Seek 0." -ForegroundColor Red
    exit 1
}

Write-Host "SUCCESS: Initial Poll, Offset Advance, and Seek verified!" -ForegroundColor Green
make stop
exit 0
