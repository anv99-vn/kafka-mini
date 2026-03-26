$ErrorActionPreference = "Stop"

# 1. Cleanup and Build
Write-Host "--- Step 1: Cleanup and Build ---" -ForegroundColor Cyan
make stop
if (Test-Path "./data-dist") { Remove-Item "./data-dist" -Recurse -Force }
if (Test-Path "broker-dist.log") { Remove-Item "broker-dist.log" }
if (Test-Path "consumer-A.log") { Remove-Item "consumer-A.log" }
if (Test-Path "consumer-B.log") { Remove-Item "consumer-B.log" }

make build

# 2. Start Broker
Write-Host "--- Step 2: Starting Broker ---" -ForegroundColor Cyan
Start-Job -Name broker-dist -ScriptBlock { 
    cd $Using:PWD
    ./bin/broker.exe -id 0 -addr :9092 -admin :8080 -data ./data-dist -peers "localhost:9092" > broker-dist.log 2>&1 
}

Write-Host "Waiting 3 seconds for broker..."
Start-Sleep -Seconds 3

# 3. Create Topic with 4 partitions
Write-Host "--- Step 3: Creating Topic with 4 partitions ---" -ForegroundColor Cyan
$Topic = "dist-topic"
$Body = @{ name = $Topic; partitions = 4 } | ConvertTo-Json
Invoke-RestMethod -Uri "http://localhost:8080/api/topics" -Method Post -Body $Body -ContentType "application/json"

# 4. Produce 80 messages (Batch 1)
Write-Host "--- Step 4: Producing 80 messages (Batch 1) ---" -ForegroundColor Cyan
for ($i = 0; $i -lt 80; $i++) {
    $key = "key-$i"
    ./bin/producer.exe -addr localhost:9092 -topic $Topic -key $key -message "msg-$i" > $null
}

# 5. Start Consumer A
Write-Host "--- Step 5: Starting Consumer A and collecting messages ---" -ForegroundColor Cyan
$GroupId = "dist-group"
Start-Job -Name consumer-A -ScriptBlock {
    cd $Using:PWD
    ./bin/consumer.exe -addr localhost:9092 -group $Using:GroupId -topic $Using:Topic > consumer-A.log 2>&1
}

Write-Host "Waiting 10 seconds for Consumer A to process Batch 1..."
Start-Sleep -Seconds 10

# 6. Show Stats Stage 1
function Show-Stats ($stageName, $logFile) {
    Write-Host "`n--- $stageName Statistics ---" -ForegroundColor Yellow
    $logLines = Get-Content $logFile | Where-Object { $_ -like "*POLL_DELIVERY*" }
    $stats = @{}
    foreach ($line in $logLines) {
        if ($line -match "partition=(\d+) count=(\d+) member=([\d\.:]+)") {
            $p = $Matches[1]
            $c = [int]$Matches[2]
            $m = $Matches[3]
            $key = "$m|Partition $p"
            $stats[$key] += $c
        }
    }
    
    $stats.GetEnumerator() | Sort-Object Name | ForEach-Object {
        Write-Host "$($_.Name): $($_.Value) messages"
    }
}

Show-Stats "Stage 1 (Single Consumer)" "broker-dist.log"

# 7. Start Consumer B
Write-Host "`n--- Step 7: Starting Consumer B (Rebalance) ---" -ForegroundColor Cyan
Start-Job -Name consumer-B -ScriptBlock {
    cd $Using:PWD
    ./bin/consumer.exe -addr localhost:9092 -group $Using:GroupId -topic $Using:Topic > consumer-B.log 2>&1
}

Write-Host "Waiting 5 seconds for rebalance..."
Start-Sleep -Seconds 5

# 8. Produce 80 more messages (Batch 2)
Write-Host "--- Step 8: Producing 80 more messages (Batch 2) ---" -ForegroundColor Cyan
for ($i = 80; $i -lt 160; $i++) {
    $key = "key-$i"
    ./bin/producer.exe -addr localhost:9092 -topic $Topic -key $key -message "msg-$i" > $null
}

Write-Host "Waiting 10 seconds for Consumers A & B to process Batch 2..."
Start-Sleep -Seconds 10

# 9. Show Stats Stage 2 (Cumulative)
Show-Stats "Stage 2 (Two Consumers - Cumulative)" "broker-dist.log"

# 10. Cleanup
make stop
Write-Host "`nDISTRIBUTION TEST FINISHED!" -ForegroundColor Green
exit 0
