$ErrorActionPreference = "Stop"

# 1. Cleanup and Build
Write-Host "--- Step 1: Cleanup and Build ---" -ForegroundColor Cyan
make stop
if (Test-Path "./data0") { Remove-Item "./data0" -Recurse -Force }
if (Test-Path "./data1") { Remove-Item "./data1" -Recurse -Force }
if (Test-Path "./data2") { Remove-Item "./data2" -Recurse -Force }
$BrokerLogs = "broker0.log", "broker1.log", "broker2.log"
foreach ($f in $BrokerLogs) { if (Test-Path $f) { Remove-Item $f } }

make build

# 2. Start 3 Brokers
Write-Host "--- Step 2: Starting 3 Brokers ---" -ForegroundColor Cyan
$Peers = "localhost:9091,localhost:9092,localhost:9093"
Start-Job -Name broker0 -ScriptBlock { cd $Using:PWD; ./bin/broker.exe -id 0 -addr :9091 -admin :8081 -data ./data0 -peers $Using:Peers > broker0.log 2>&1 }
Start-Job -Name broker1 -ScriptBlock { cd $Using:PWD; ./bin/broker.exe -id 1 -addr :9092 -admin :8082 -data ./data1 -peers $Using:Peers > broker1.log 2>&1 }
Start-Job -Name broker2 -ScriptBlock { cd $Using:PWD; ./bin/broker.exe -id 2 -addr :9093 -admin :8083 -data ./data2 -peers $Using:Peers > broker2.log 2>&1 }

Write-Host "Waiting 5 seconds for leader election..."
Start-Sleep -Seconds 5

# 3. Create topic on all nodes until one succeeds
Write-Host "--- Step 3: Creating Topic ---" -ForegroundColor Cyan
$Topic = "raft-topic"
$Body = @{ name = $Topic; partitions = 3 } | ConvertTo-Json

$success = $false
foreach ($port in @(8081, 8082, 8083)) {
    try {
        $res = Invoke-RestMethod -Uri "http://localhost:$port/api/topics" -Method Post -Body $Body -ContentType "application/json"
        if ($res.success -eq $true) {
            Write-Host "Created topic via leader at port $port" -ForegroundColor Green
            $success = $true
            break
        } else {
            Write-Host "Node at port $port ($($res.error_message))" -ForegroundColor Yellow
        }
    } catch {
        Write-Host "Request to port $port failed" -ForegroundColor Yellow
    }
}

if (-not $success) {
    Write-Host "FAILURE: Could not create topic. No leader found." -ForegroundColor Red
    make stop
    exit 1
}

Write-Host "Waiting 2 seconds for replication..."
Start-Sleep -Seconds 2

# 4. Verify topic on ALL nodes
Write-Host "--- Step 4: Verifying Replication ---" -ForegroundColor Cyan
$allReplicated = $true
foreach ($port in @(8081, 8082, 8083)) {
    $res = Invoke-RestMethod -Uri "http://localhost:$port/api/topics" -Method Get
    if ($null -ne $res.names -and $Topic -in $res.names) {
        Write-Host "SUCCESS: Topic found on node at port $port" -ForegroundColor Green
    } else {
        Write-Host "FAILURE: Topic NOT found on node at port $port. Replication failed." -ForegroundColor Red
        $allReplicated = $false
    }
}

if (-not $allReplicated) {
    make stop
    exit 1
}

Write-Host "ALL TESTS PASSED!" -ForegroundColor Green
make stop
exit 0
