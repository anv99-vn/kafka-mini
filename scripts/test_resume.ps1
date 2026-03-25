# Integration Test for Consumer Offset Resume
# This script verifies that:
# 1. A consumer group receives 5 messages then stops.
# 2. Restarting the consumer with the same group ID receives 0 new messages.
# 3. A different consumer group receives all 5 messages.

$ErrorActionPreference = "Stop"
$Topic = "resume-test"
$LogFileA1 = "resume_groupA_1.log"
$LogFileA2 = "resume_groupA_2.log"
$LogFileB = "resume_groupB.log"

# 1. Cleanup and Build
Write-Host "--- Step 1: Cleanup and Build ---" -ForegroundColor Cyan
make stop
if (Test-Path "./data") { Remove-Item "./data" -Recurse -Force }
foreach ($f in $LogFileA1, $LogFileA2, $LogFileB) { if (Test-Path $f) { Remove-Item $f } }
make build

# 2. Start Broker
Write-Host "--- Step 2: Starting Broker ---" -ForegroundColor Cyan
$BrokerProc = Start-Process -FilePath "./bin/broker.exe" -NoNewWindow -PassThru
Start-Sleep -Seconds 2

# 3. Create topic
Write-Host "--- Step 3: Creating topic ---" -ForegroundColor Cyan
$Body = @{ name = $Topic; partitions = 1 } | ConvertTo-Json
Invoke-RestMethod -Uri "http://localhost:8080/api/topics" -Method Post -Body $Body -ContentType "application/json"

# 4. Produce 5 messages
Write-Host "--- Step 4: Producing 5 messages ---" -ForegroundColor Cyan
for ($i=1; $i -le 5; $i++) {
    ./bin/producer.exe -topic $Topic -key "key-$i" -message "Message-$i"
}

# 5. Consumer Group A - First Run (Receive 5)
Write-Host "--- Step 5: Consumer Group A - First Run ---" -ForegroundColor Cyan
$Proc = Start-Process -FilePath "./bin/consumer.exe" -ArgumentList "-topic $Topic -group group-A" -NoNewWindow -RedirectStandardOutput $LogFileA1 -PassThru
Start-Sleep -Seconds 7
$Proc | Stop-Process -Force

$countA1 = (Select-String -Path $LogFileA1 -Pattern "Received: Message-").Count
Write-Host "Group A (Run 1) received $countA1 messages."
if ($countA1 -ne 5) {
    Write-Host "FAILURE: Group A should have received 5 messages, got $countA1" -ForegroundColor Red
    exit 1
}

# 6. Consumer Group A - Second Run (Receive 0)
Write-Host "--- Step 6: Consumer Group A - Second Run (Restart) ---" -ForegroundColor Cyan
$Proc = Start-Process -FilePath "./bin/consumer.exe" -ArgumentList "-topic $Topic -group group-A" -NoNewWindow -RedirectStandardOutput $LogFileA2 -PassThru
Start-Sleep -Seconds 5
$Proc | Stop-Process -Force

$countA2 = (Select-String -Path $LogFileA2 -Pattern "Received: Message-").Count
Write-Host "Group A (Run 2) received $countA2 messages."
if ($countA2 -ne 0) {
    Write-Host "FAILURE: Group A (Restart) should have received 0 messages, got $countA2" -ForegroundColor Red
    exit 1
}

# 7. Consumer Group B - First Run (Receive 5)
Write-Host "--- Step 7: Consumer Group B - First Run ---" -ForegroundColor Cyan
$Proc = Start-Process -FilePath "./bin/consumer.exe" -ArgumentList "-topic $Topic -group group-B" -NoNewWindow -RedirectStandardOutput $LogFileB -PassThru
Start-Sleep -Seconds 7
$Proc | Stop-Process -Force

$countB = (Select-String -Path $LogFileB -Pattern "Received: Message-").Count
Write-Host "Group B received $countB messages."
if ($countB -ne 5) {
    Write-Host "FAILURE: Group B should have received 5 messages, got $countB" -ForegroundColor Red
    exit 1
}

Write-Host "SUCCESS: Consumer offset resume and isolation verified!" -ForegroundColor Green
make stop
exit 0
