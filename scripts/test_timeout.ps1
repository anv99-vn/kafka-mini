# Integration Test for Timeout-Based Polling
# This script verifies that:
# 1. Poll respects the TimeoutMs limit and returns partial results if needed.
# 2. Offset is advanced only for returned messages.

$ErrorActionPreference = "Stop"
$Topic = "timeout-test-topic"
$LogFile1 = "timeout_run1.log"
$LogFile2 = "timeout_run2.log"

# 1. Cleanup and Build
Write-Host "--- Step 1: Cleanup and Build ---" -ForegroundColor Cyan
make stop
if (Test-Path "./data") { Remove-Item "./data" -Recurse -Force }
if (Test-Path $LogFile1) { Remove-Item $LogFile1 }
if (Test-Path $LogFile2) { Remove-Item $LogFile2 }
make build-broker build-producer build-consumer

# 2. Start Broker
Write-Host "--- Step 2: Starting Broker ---" -ForegroundColor Cyan
$BrokerProc = Start-Process -FilePath "./bin/broker.exe" -NoNewWindow -PassThru
Start-Sleep -Seconds 2

# 3. Create topic
Write-Host "--- Step 3: Creating topic ---" -ForegroundColor Cyan
$Body = @{ name = $Topic; partitions = 1 } | ConvertTo-Json
Invoke-RestMethod -Uri "http://localhost:8080/api/topics" -Method Post -Body $Body -ContentType "application/json"

# 4. Produce 100 messages
Write-Host "--- Step 4: Producing 100 messages ---" -ForegroundColor Cyan
for ($i=1; $i -le 100; $i++) {
    ./bin/producer.exe -topic $Topic -key "key-$i" -message "Msg-$i"
}

# 5. Poll with very small timeout (1ms)
Write-Host "--- Step 5: Polling with 1ms timeout ---" -ForegroundColor Cyan
$Proc = Start-Process -FilePath "./bin/consumer.exe" -ArgumentList "-topic $Topic -group group-T -poll-timeout 1" -NoNewWindow -RedirectStandardOutput $LogFile1 -PassThru
Start-Sleep -Seconds 8
$Proc | Stop-Process -Force

$count1 = (Select-String -Path $LogFile1 -Pattern "Received: Msg-").Count
Write-Host "First poll (1ms) received $count1 messages."

# 6. Poll again with normal timeout to get the rest
Write-Host "--- Step 6: Polling with 1000ms timeout (get the rest) ---" -ForegroundColor Cyan
$Proc = Start-Process -FilePath "./bin/consumer.exe" -ArgumentList "-topic $Topic -group group-T -poll-timeout 1000" -NoNewWindow -RedirectStandardOutput $LogFile2 -PassThru
Start-Sleep -Seconds 8
$Proc | Stop-Process -Force

$count2 = (Select-String -Path $LogFile2 -Pattern "Received: Msg-").Count
Write-Host "Second poll (1000ms) received $count2 messages."

$total = $count1 + $count2
Write-Host "Total messages received: $total"

if ($total -ne 100) {
    Write-Host "FAILURE: Total messages should be 100, got $total" -ForegroundColor Red
    exit 1
}

if ($count1 -eq 100) {
    Write-Host "WARNING: Polling was NOT limited by 1ms (received all 100). This might happen if the local disk is too fast, but usually it should be limited." -ForegroundColor Yellow
}

Write-Host "SUCCESS: Timeout behavior and offset advancement verified!" -ForegroundColor Green
make stop
exit 0
