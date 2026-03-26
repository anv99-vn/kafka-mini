$ErrorActionPreference = "Stop"

# 1. Cleanup and Build
Write-Host "--- Step 1: Cleanup and Build ---" -ForegroundColor Cyan
make stop
if (Test-Path "./data-test80") { Remove-Item "./data-test80" -Recurse -Force }
if (Test-Path "broker-test80.log") { Remove-Item "broker-test80.log" }
if (Test-Path "consumer-A.log") { Remove-Item "consumer-A.log" }
if (Test-Path "consumer-B.log") { Remove-Item "consumer-B.log" }
if (Test-Path "consumer-C.log") { Remove-Item "consumer-C.log" }

make build

# 2. Start Broker
Write-Host "--- Step 2: Starting Broker ---" -ForegroundColor Cyan
Start-Job -Name broker-test80 -ScriptBlock { 
    cd $Using:PWD
    ./bin/broker.exe -id 0 -addr :9092 -admin :8080 -data ./data-test80 -peers "localhost:9092" > broker-test80.log 2>&1 
}

Write-Host "Waiting 3 seconds for broker..."
Start-Sleep -Seconds 3

# 3. Create Topic with 4 partitions
Write-Host "--- Step 3: Creating Topic with 4 partitions ---" -ForegroundColor Cyan
$Topic = "test80-topic"
$Body = @{ name = $Topic; partitions = 4 } | ConvertTo-Json
Invoke-RestMethod -Uri "http://localhost:8080/api/topics" -Method Post -Body $Body -ContentType "application/json"

# 4. Start 3 Consumers concurrently in the same group
Write-Host "--- Step 4: Starting 3 Consumers ---" -ForegroundColor Cyan
$GroupId = "test80-group"
Start-Job -Name consumer-A -ScriptBlock {
    cd $Using:PWD
    ./bin/consumer.exe -addr localhost:9092 -group $Using:GroupId -topic $Using:Topic -poll-timeout 1000 > consumer-A.log 2>&1
}
Start-Job -Name consumer-B -ScriptBlock {
    cd $Using:PWD
    ./bin/consumer.exe -addr localhost:9092 -group $Using:GroupId -topic $Using:Topic -poll-timeout 1000 > consumer-B.log 2>&1
}
Start-Job -Name consumer-C -ScriptBlock {
    cd $Using:PWD
    ./bin/consumer.exe -addr localhost:9092 -group $Using:GroupId -topic $Using:Topic -poll-timeout 1000 > consumer-C.log 2>&1
}

Write-Host "Waiting 10 seconds for initial rebalance..."
Start-Sleep -Seconds 10

# 5. Produce 80 messages with different keys
Write-Host "--- Step 5: Producing 80 messages ---" -ForegroundColor Cyan
for ($i = 0; $i -lt 80; $i++) {
    $key = "key-$i"
    ./bin/producer.exe -addr localhost:9092 -topic $Topic -key $key -message "msg-$i" > $null
}

Write-Host "Waiting 15 seconds for consumers to process all messages..."
Start-Sleep -Seconds 15

# 6. Verify Results
Write-Host "--- Step 6: Verifying Results ---" -ForegroundColor Cyan
$logA = if (Test-Path "consumer-A.log") { Get-Content "consumer-A.log" } else { "" }
$logB = if (Test-Path "consumer-B.log") { Get-Content "consumer-B.log" } else { "" }
$logC = if (Test-Path "consumer-C.log") { Get-Content "consumer-C.log" } else { "" }

$countA = ($logA | Select-String "Received:").Count
$countB = ($logB | Select-String "Received:").Count
$countC = ($logC | Select-String "Received:").Count
$total = $countA + $countB + $countC

Write-Host "Consumer A received: $countA messages"
Write-Host "Consumer B received: $countB messages"
Write-Host "Consumer C received: $countC messages"
Write-Host "Total messages received: $total" -ForegroundColor Yellow

# Show unique message values to ensure no duplicates
$allMsgs = ($logA + $logB + $logC | Select-String -Pattern "Received: msg-(\d+)" | ForEach-Object { $_.Matches[0].Groups[1].Value })
$uniqueCount = ($allMsgs | Select-Object -Unique).Count
Write-Host "Unique messages received: $uniqueCount"

if ($total -eq 80 -and $uniqueCount -eq 80) {
    Write-Host "SUCCESS: Exactly 80 unique messages were processed!" -ForegroundColor Green
} else {
    Write-Host "FAILURE: Expected 80 unique messages, but got $total total and $uniqueCount unique" -ForegroundColor Red
}

# 7. Cleanup
make stop
Write-Host "`nCONCURRENT CONSUMER TEST FINISHED!" -ForegroundColor Green
exit 0
