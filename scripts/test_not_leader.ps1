$ErrorActionPreference = "Stop"
$LogFile = "not_leader_consumer.log"
$Topic = "not-leader-topic"

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

# 3. Create topic on all nodes until one succeeds (Find Leader)
Write-Host "--- Step 3: Finding Leader and Creating Topic ---" -ForegroundColor Cyan
$Body = @{ name = $Topic; partitions = 1 } | ConvertTo-Json

$LeaderIdx = -1
$Ports = @(8081, 8082, 8083)
$RpcPorts = @(9091, 9092, 9093)

for ($i = 0; $i -lt 3; $i++) {
    $port = $Ports[$i]
    try {
        $res = Invoke-RestMethod -Uri "http://localhost:$port/api/topics" -Method Post -Body $Body -ContentType "application/json"
        if ($res.success -eq $true) {
            Write-Host "Leader found! broker$i (Admin: $port, RPC: $($RpcPorts[$i]))" -ForegroundColor Green
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

# 4. Find a Follower Node
$FollowerIdx = ($LeaderIdx + 1) % 3
$FollowerRpcPort = $RpcPorts[$FollowerIdx]
$LeaderRpcPort = $RpcPorts[$LeaderIdx]

Write-Host "--- Step 4: Connecting Producer to Follower first ---" -ForegroundColor Cyan
Write-Host "Producer will try to send to Follower (Port: $FollowerRpcPort) first."
Write-Host "It should receive a 'not leader' error and automatically switch to Leader (Port: $LeaderRpcPort)."

# Notice we put the Follower port FIRST in the comma-separated list
$ProducerAddrs = "localhost:$FollowerRpcPort,localhost:$LeaderRpcPort"

./bin/producer.exe -addr $ProducerAddrs -topic $Topic -key "not-leader-key" -message "msg-bouncing-around"

# 5. Verify the message is saved using Consumer
Write-Host "--- Step 5: Starting Consumer to verify message ---" -ForegroundColor Cyan
$ConsumerProc = Start-Process -FilePath "./bin/consumer.exe" -ArgumentList "-addr localhost:$LeaderRpcPort", "-topic $Topic", "-group not-leader-group" -NoNewWindow -RedirectStandardOutput $LogFile -PassThru
Start-Sleep -Seconds 5

# 7. Check result
Write-Host "`n--- Step 6: Verifying Consumer Log ---" -ForegroundColor Cyan
try {
    Stop-Process -Id $ConsumerProc.Id -Force -ErrorAction SilentlyContinue
} catch {}

make stop
Start-Sleep -Seconds 1

if (Test-Path $LogFile) {
    $Content = Get-Content $LogFile -Raw
    if ($Content -like "*(key: not-leader-key, offset: *") {
        Write-Host "SUCCESS: Consumer read the message! Producer successfully handled 'not leader'." -ForegroundColor Green
        Write-Host "ALL TESTS PASSED!" -ForegroundColor Green
        exit 0
    } else {
        Write-Host "FAILURE: Message with key not-leader-key not found in consumer log." -ForegroundColor Red
        exit 1
    }
} else {
    Write-Host "`nFAILURE: Consumer log file not created." -ForegroundColor Red
    exit 1
}
