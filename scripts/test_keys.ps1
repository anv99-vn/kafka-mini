# scripts/test_keys.ps1
# This script verifies that producers can send messages with different keys 
# and the consumer correctly receives all of them.

$ErrorActionPreference = "Stop"
$LogFile = "multi_key_consumer.log"
$Topic = "multi-key-topic"

# 1. Build all binaries
Write-Host "--- Step 1: Building binaries ---" -ForegroundColor Cyan
make build-broker build-producer build-consumer

# 2. Cleanup old logs and data
if (Test-Path $LogFile) { Remove-Item $LogFile }
if (Test-Path "./data") { Remove-Item "./data" -Recurse -Force }

# 3. Start Broker in background
Write-Host "--- Step 2: Starting Broker ---" -ForegroundColor Cyan
$BrokerProc = Start-Process -FilePath "./bin/broker.exe" -NoNewWindow -PassThru
Start-Sleep -Seconds 2

# 4. Start Consumer in background and redirect output
Write-Host "--- Step 3: Starting Consumer ---" -ForegroundColor Cyan
$ConsumerProc = Start-Process -FilePath "./bin/consumer.exe" -ArgumentList "-topic $Topic" -NoNewWindow -RedirectStandardOutput $LogFile -PassThru
Start-Sleep -Seconds 2

# 4.5 Create Topic with 3 partitions via Admin API
Write-Host "--- Step 3.5: Creating topic with 3 partitions ---" -ForegroundColor Cyan
$Body = @{
    name = $Topic
    partitions = 3
    replication_factor = 1
} | ConvertTo-Json
Invoke-RestMethod -Uri "http://localhost:8080/api/topics" -Method Post -Body $Body -ContentType "application/json"
Start-Sleep -Seconds 1

# 5. Run Producer to send messages with different keys
Write-Host "--- Step 4: Sending messages with different keys ---" -ForegroundColor Cyan
./bin/producer.exe -topic $Topic -key "user-1" -message "msg-A"
./bin/producer.exe -topic $Topic -key "user-2" -message "msg-B"
./bin/producer.exe -topic $Topic -key "user-3" -message "msg-C"

# 6. Wait for messages to be processed
Write-Host "--- Step 5: Waiting for messages in Consumer log ---" -ForegroundColor Cyan
Start-Sleep -Seconds 5

# 7. Cleanup
Write-Host "`n--- Step 6: Cleaning up ---" -ForegroundColor Cyan
try {
    Stop-Process -Id $ConsumerProc.Id -Force -ErrorAction SilentlyContinue
    Stop-Process -Id $BrokerProc.Id -Force -ErrorAction SilentlyContinue
} catch {}

# Wait for file to be released
Start-Sleep -Seconds 1

# 8. Check result
if (Test-Path $LogFile) {
    $Content = Get-Content $LogFile -Raw
    
    $Keys = @("user-1", "user-2", "user-3")
    $AllFound = $true
    
    foreach ($Key in $Keys) {
        if ($Content -like "*(key: $Key, offset: *") {
            Write-Host "SUCCESS: Found message with key $Key" -ForegroundColor Green
        } else {
            Write-Host "FAILURE: Message with key $Key not found!" -ForegroundColor Red
            $AllFound = $false
        }
    }

    if ($AllFound) {
        Write-Host "`nOVERALL SUCCESS: All multi-key messages verified!" -ForegroundColor Green
        exit 0
    } else {
        exit 1
    }
} else {
    Write-Host "`nFAILURE: Consumer log file not created." -ForegroundColor Red
    exit 1
}
