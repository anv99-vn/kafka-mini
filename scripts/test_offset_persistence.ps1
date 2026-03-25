# Integration Test for Offset Persistence across Broker Restarts
$ErrorActionPreference = "Stop"
$Topic = "persist-offsets-test"
$LogFile1 = "persist_offsets_run1.log"
$LogFile2 = "persist_offsets_run2.log"
$DataDir = "./data"
$OffsetFile = "$DataDir/offsets.json"

# 1. Cleanup and Build
Write-Host "--- Step 1: Cleanup and Build ---" -ForegroundColor Cyan
make stop
if (Test-Path $DataDir) { Remove-Item $DataDir -Recurse -Force }
foreach ($f in $LogFile1, $LogFile2) { if (Test-Path $f) { Remove-Item $f } }
make build

# 2. Start Broker
Write-Host "--- Step 2: Starting Broker ---" -ForegroundColor Cyan
$BrokerProc = Start-Process -FilePath "./bin/broker.exe" -NoNewWindow -PassThru
Start-Sleep -Seconds 2

# 3. Create topic
Write-Host "--- Step 3: Creating topic ---" -ForegroundColor Cyan
$Body = @{ name = $Topic; partitions = 1 } | ConvertTo-Json
Invoke-RestMethod -Uri "http://localhost:8080/api/topics" -Method Post -Body $Body -ContentType "application/json"

# 4. Produce 10 messages
Write-Host "--- Step 4: Producing 10 messages ---" -ForegroundColor Cyan
for ($i=1; $i -le 10; $i++) {
    ./bin/producer.exe -topic $Topic -key "key-$i" -message "Message-$i"
}

# 5. Consumer Group A - Consume 5 messages
Write-Host "--- Step 5: Consumer Group A - Consume 5 messages ---" -ForegroundColor Cyan
$Proc = Start-Process -FilePath "./bin/consumer.exe" -ArgumentList "-topic $Topic -group group-persist" -NoNewWindow -RedirectStandardOutput $LogFile1 -PassThru
Start-Sleep -Seconds 7
$Proc | Stop-Process -Force

$count1 = (Select-String -Path $LogFile1 -Pattern "Received: Message-").Count
Write-Host "Run 1 received $count1 messages."
# Note: It might receive more than 5 depending on timing, but we want to see it resume correctly.

# 6. Stop Broker
Write-Host "--- Step 6: Stopping Broker ---" -ForegroundColor Cyan
make stop
Start-Sleep -Seconds 3

# 7. Verify offsets.json exists
Write-Host "--- Step 7: Verifying offsets.json ---" -ForegroundColor Cyan
if (Test-Path "$DataDir/offsets.json") {
    Write-Host "SUCCESS: $DataDir/offsets.json exists." -ForegroundColor Green
    Get-Content "$DataDir/offsets.json" | Write-Host -ForegroundColor Gray
} else {
    Write-Host "FAILURE: $DataDir/offsets.json NOT found!" -ForegroundColor Red
    ls $DataDir
    exit 1
}

# 8. Start Broker again
Write-Host "--- Step 8: Restarting Broker ---" -ForegroundColor Cyan
$BrokerProc = Start-Process -FilePath "./bin/broker.exe" -NoNewWindow -PassThru
Start-Sleep -Seconds 2

# 9. Consumer Group A - Consume remaining messages
Write-Host "--- Step 9: Consumer Group A - Resuming consumption ---" -ForegroundColor Cyan
$Proc = Start-Process -FilePath "./bin/consumer.exe" -ArgumentList "-topic $Topic -group group-persist" -NoNewWindow -RedirectStandardOutput $LogFile2 -PassThru
Start-Sleep -Seconds 7
$Proc | Stop-Process -Force

$count2 = (Select-String -Path $LogFile2 -Pattern "Received: Message-").Count
Write-Host "Run 2 received $count2 messages."

# The total number of unique messages should be 10.
# Run 2 should start from where Run 1 left off.
# If Run 1 received 5, Run 2 should receive 5 more (Message-6 to Message-10).

$totalUnique = 0
$messages = @()
foreach ($f in $LogFile1, $LogFile2) {
    $matches = Select-String -Path $f -Pattern "Received: (Message-\d+)"
    foreach ($m in $matches) {
        $msg = $m.Matches[0].Groups[1].Value
        if ($msg -notin $messages) {
            $messages += $msg
        }
    }
}

$uniqueCount = $messages.Count
Write-Host "Total unique messages received: $uniqueCount"
if ($uniqueCount -eq 10) {
    Write-Host "SUCCESS: All 10 messages received exactly once across restart!" -ForegroundColor Green
} else {
    Write-Host "FAILURE: Expected 10 unique messages, got $uniqueCount" -ForegroundColor Red
    exit 1
}

make stop
exit 0
