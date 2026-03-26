$ErrorActionPreference = "Stop"

# 1. Cleanup and Build
Write-Host "--- Step 1: Cleanup and Build ---" -ForegroundColor Cyan
make stop
if (Test-Path "./data-rebalance") { Remove-Item "./data-rebalance" -Recurse -Force }
if (Test-Path "broker-rebalance.log") { Remove-Item "broker-rebalance.log" }

make build

# 2. Start Broker
Write-Host "--- Step 2: Starting Broker ---" -ForegroundColor Cyan
# Start broker with -id 0, no peers (single node)
Start-Job -Name broker-rebalance -ScriptBlock { 
    cd $Using:PWD
    ./bin/broker.exe -id 0 -addr :9092 -admin :8080 -data ./data-rebalance -peers "localhost:9092" > broker-rebalance.log 2>&1 
}

Write-Host "Waiting 3 seconds for broker..."
Start-Sleep -Seconds 3

# 3. Create Topic with 4 partitions
Write-Host "--- Step 3: Creating Topic with 4 partitions ---" -ForegroundColor Cyan
$Topic = "rebalance-topic"
$Body = @{ name = $Topic; partitions = 4 } | ConvertTo-Json
try {
    Invoke-RestMethod -Uri "http://localhost:8080/api/topics" -Method Post -Body $Body -ContentType "application/json"
    Write-Host "Topic '$Topic' created with 4 partitions." -ForegroundColor Green
} catch {
    Write-Host "Failed to create topic: $($_.Exception.Message)" -ForegroundColor Red
    make stop
    exit 1
}

# 4. Start Consumer A
Write-Host "--- Step 4: Starting Consumer A ---" -ForegroundColor Cyan
$GroupId = "rebalance-group"
Start-Job -Name consumer-A -ScriptBlock {
    cd $Using:PWD
    ./bin/consumer.exe -addr localhost:9092 -group $Using:GroupId -topic $Using:Topic > consumer-A.log 2>&1
}

Write-Host "Waiting 5 seconds for Consumer A to join..."
Start-Sleep -Seconds 5

Write-Host "Checking Broker Logs for Rebalance..."
$logs = Get-Content "broker-rebalance.log" | Where-Object { $_ -like "*Rebalanced group $GroupId*" }
if ($logs) {
    Write-Host "LATEST REBALANCE: $($logs[-1])" -ForegroundColor Magenta
} else {
    Write-Host "No rebalance log found yet." -ForegroundColor Yellow
}

# 5. Start Consumer B
Write-Host "--- Step 5: Starting Consumer B ---" -ForegroundColor Cyan
Start-Job -Name consumer-B -ScriptBlock {
    cd $Using:PWD
    ./bin/consumer.exe -addr localhost:9092 -group $Using:GroupId -topic $Using:Topic > consumer-B.log 2>&1
}

Write-Host "Waiting 5 seconds for Consumer B to join and trigger rebalance..."
Start-Sleep -Seconds 5

$logs = Get-Content "broker-rebalance.log" | Where-Object { $_ -like "*Rebalanced group $GroupId*" }
Write-Host "LATEST REBALANCE: $($logs[-1])" -ForegroundColor Magenta
if ($logs[-1] -like "*2 members*") {
    Write-Host "SUCCESS: Rebalanced to 2 members!" -ForegroundColor Green
} else {
    Write-Host "FAILURE: Did not see 2 members in rebalance log." -ForegroundColor Red
}

# 6. Stop Consumer A and wait for timeout
Write-Host "--- Step 6: Stopping Consumer A (Simulate failure) ---" -ForegroundColor Cyan
Stop-Job -Name consumer-A
Write-Host "Waiting 12 seconds for Consumer A session to timeout (10s threshold)..."
Start-Sleep -Seconds 12

# Trigger a rebalance by having Consumer B poll again (it polls every 1s in its loop)
Start-Sleep -Seconds 2

$logs = Get-Content "broker-rebalance.log" | Where-Object { $_ -like "*Rebalanced group $GroupId*" }
Write-Host "LATEST REBALANCE: $($logs[-1])" -ForegroundColor Magenta
if ($logs[-1] -like "*1 members*") {
    Write-Host "SUCCESS: Consumer A timed out, partitions re-assigned to Consumer B!" -ForegroundColor Green
} else {
    Write-Host "FAILURE: Did not see rebalance to 1 member." -ForegroundColor Red
}

# 7. Cleanup
Write-Host "--- Step 7: Cleanup ---" -ForegroundColor Cyan
make stop
Write-Host "REBALANCE TEST SCRIPT FINISHED!" -ForegroundColor Green
exit 0
