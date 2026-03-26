$ErrorActionPreference = "Stop"
$LogFile = "failover_consumer.log"
$Topic = "producer-failover-test"

# 1. Cleanup and Build
Write-Host "--- Step 1: Cleanup and Build ---" -ForegroundColor Cyan
make stop
if (Test-Path "./data0") { Remove-Item "./data0" -Recurse -Force }
if (Test-Path "./data1") { Remove-Item "./data1" -Recurse -Force }
if (Test-Path "./data2") { Remove-Item "./data2" -Recurse -Force }
if (Test-Path $LogFile) { Remove-Item $LogFile }
$BrokerLogs = "broker0.log", "broker1.log", "broker2.log"
foreach ($f in $BrokerLogs) { if (Test-Path $f) { Remove-Item $f } }

make build

# 2. Start 3 Brokers
Write-Host "--- Step 2: Starting 3 Brokers ---" -ForegroundColor Cyan
$Peers = "localhost:9091,localhost:9092,localhost:9093"
Start-Job -Name broker0 -ScriptBlock { cd $Using:PWD; ./bin/broker.exe -id 0 -addr :9091 -admin :8081 -data ./data0 -peers $Using:Peers > broker0.log 2>&1 }
Start-Job -Name broker1 -ScriptBlock { cd $Using:PWD; ./bin/broker.exe -id 1 -addr :9092 -admin :8082 -data ./data1 -peers $Using:Peers > broker1.log 2>&1 }
Start-Job -Name broker2 -ScriptBlock { cd $Using:PWD; ./bin/broker.exe -id 2 -addr :9093 -admin :8083 -data ./data2 -peers $Using:Peers > broker2.log 2>&1 }

Write-Host "Waiting 5 seconds for initial leader election..."
Start-Sleep -Seconds 5

# 3. Create topic on all nodes until one succeeds is the leader
Write-Host "--- Step 3: Finding Leader and Creating Topic ---" -ForegroundColor Cyan
$Body = @{ name = $Topic; partitions = 1 } | ConvertTo-Json

$LeaderIdx = -1
$Ports = @(8081, 8082, 8083)

for ($i = 0; $i -lt 3; $i++) {
    $port = $Ports[$i]
    try {
        $res = Invoke-RestMethod -Uri "http://localhost:$port/api/topics" -Method Post -Body $Body -ContentType "application/json"
        if ($res.success -eq $true) {
            Write-Host "Created topic via leader broker$i (port $port)" -ForegroundColor Green
            $LeaderIdx = $i
            break
        }
    } catch {
    }
}

if ($LeaderIdx -eq -1) {
    Write-Host "FAILURE: Could not create topic or find leader." -ForegroundColor Red
    make stop
    exit 1
}

Start-Sleep -Seconds 2

# 4. Kill the Leader
Write-Host "--- Step 4: Killing Leader (broker$LeaderIdx) ---" -ForegroundColor Cyan
Stop-Job -Name "broker$LeaderIdx"
Remove-Job -Name "broker$LeaderIdx"
Write-Host "Waiting 5 seconds for new leader election..."
Start-Sleep -Seconds 5

# 5. Run Producer with all addresses to trigger failover and reconnect
Write-Host "--- Step 5: Producing message after leader death ---" -ForegroundColor Cyan
./bin/producer.exe -addr "localhost:9091,localhost:9092,localhost:9093" -topic $Topic -key "failover-key" -message "msg-failover"

# 6. Find an alive broker for consumer
$AlivePort = 9091
if ($LeaderIdx -eq 0) { $AlivePort = 9092 }

# Consumer reads the message
Write-Host "--- Step 6: Starting Consumer to verify message ---" -ForegroundColor Cyan
$ConsumerProc = Start-Process -FilePath "./bin/consumer.exe" -ArgumentList "-addr localhost:$AlivePort", "-topic $Topic", "-group failover-group" -NoNewWindow -RedirectStandardOutput $LogFile -PassThru
Start-Sleep -Seconds 5

# 7. Check result
Write-Host "`n--- Step 7: Verifying Consumer Log ---" -ForegroundColor Cyan
try {
    Stop-Process -Id $ConsumerProc.Id -Force -ErrorAction SilentlyContinue
} catch {}

make stop
Start-Sleep -Seconds 1

if (Test-Path $LogFile) {
    $Content = Get-Content $LogFile -Raw
    if ($Content -like "*(key: failover-key, offset: *") {
        Write-Host "SUCCESS: Consumer read the failover message!" -ForegroundColor Green
        Write-Host "ALL TESTS PASSED!" -ForegroundColor Green
        exit 0
    } else {
        Write-Host "FAILURE: Message with key failover-key not found in consumer log." -ForegroundColor Red
        exit 1
    }
} else {
    Write-Host "`nFAILURE: Consumer log file not created." -ForegroundColor Red
    exit 1
}
