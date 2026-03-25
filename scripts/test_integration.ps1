# Kafka-mini Integration Test Script
# This script starts the broker, a consumer, and a producer to verify the end-to-end flow.

$ErrorActionPreference = "Stop"
$LogFile = "consumer.log"

# 1. Build all binaries
Write-Host "--- Step 1: Building binaries ---" -ForegroundColor Cyan
make build-broker build-producer build-consumer

# 2. Cleanup old logs
if (Test-Path $LogFile) { Remove-Item $LogFile }

# 3. Start Broker in background
Write-Host "--- Step 2: Starting Broker ---" -ForegroundColor Cyan
$BrokerProc = Start-Process -FilePath "./bin/broker.exe" -NoNewWindow -PassThru
Start-Sleep -Seconds 2

# 4. Start Consumer in background and redirect output
Write-Host "--- Step 3: Starting Consumer ---" -ForegroundColor Cyan
$ConsumerProc = Start-Process -FilePath "./bin/consumer.exe" -ArgumentList "-topic test-topic" -NoNewWindow -RedirectStandardOutput $LogFile -PassThru
Start-Sleep -Seconds 2

# 5. Run Producer to send a message
Write-Host "--- Step 4: Sending message via Producer ---" -ForegroundColor Cyan
./bin/producer.exe -topic test-topic -message "hello kafka-mini"

# 6. Wait for message to be processed
Write-Host "--- Step 5: Waiting for message in Consumer log ---" -ForegroundColor Cyan
Start-Sleep -Seconds 5

# 8. Cleanup
Write-Host "`n--- Step 6: Cleaning up ---" -ForegroundColor Cyan
try {
    Stop-Process -Id $ConsumerProc.Id -Force -ErrorAction SilentlyContinue
    Stop-Process -Id $BrokerProc.Id -Force -ErrorAction SilentlyContinue
} catch {}

# Wait for file to be released
Start-Sleep -Seconds 1

# 7. Check result
if (Test-Path $LogFile) {
    $Content = Get-Content $LogFile -Raw
    
    if ($Content -like "*Received: hello kafka-mini*") {
        Write-Host "`nSUCCESS: Message flow verified!" -ForegroundColor Green
        $Success = $true
    } else {
        Write-Host "`nFAILURE: Message not found in consumer log." -ForegroundColor Red
        $Success = $false
    }
} else {
    Write-Host "`nFAILURE: Consumer log file not created." -ForegroundColor Red
    $Success = $false
}

if ($Success) { exit 0 } else { exit 1 }
