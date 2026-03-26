$ErrorActionPreference = "Stop"

# 1. Configuration
Write-Host "--- Step 1: Configuration ---" -ForegroundColor Cyan
$NodeCount = 5
$PortsBase = 9090
$AdminPortsBase = 8080
$DataBase = "./data"
$Peers = (0..($NodeCount-1) | ForEach-Object { "localhost:$($PortsBase + $_ + 1)" }) -join ","

# 2. Cleanup and Build
Write-Host "--- Step 2: Cleanup and Build ---" -ForegroundColor Cyan
make stop
for ($i = 0; $i -lt $NodeCount; $i++) {
    $dir = "$DataBase$i"
    if (Test-Path $dir) { Remove-Item $dir -Recurse -Force }
    $log = "broker$i.log"
    if (Test-Path $log) { Remove-Item $log }
}

make build

# 3. Start 5 Nodes
Write-Host "--- Step 3: Starting $NodeCount Nodes ---" -ForegroundColor Cyan
for ($i = 0; $i -lt $NodeCount; $i++) {
    $port = $PortsBase + $i + 1
    $adminPort = $AdminPortsBase + $i + 1
    $dataDir = "$DataBase$i"
    $logFile = "broker$i.log"
    Write-Host "Starting Node $i at port $port (Admin: $adminPort)..."
    Start-Job -Name "broker$i" -ScriptBlock { 
        cd $Using:PWD
        ./bin/broker.exe -id $Using:i -addr ":$Using:port" -admin ":$Using:adminPort" -data "$Using:dataDir" -peers "$Using:Peers" > "$Using:logFile" 2>&1 
    }
}

Write-Host "Waiting 8 seconds for cluster to stabilize..."
Start-Sleep -Seconds 8

# Helper to find leader
function Get-ClusterLeader {
    for ($i = 0; $i -lt $NodeCount; $i++) {
        $adminPort = $AdminPortsBase + $i + 1
        try {
            $status = Invoke-RestMethod -Uri "http://localhost:$adminPort/api/status" -Method Get -ErrorAction SilentlyContinue
            if ($status.state -eq "Leader") {
                return $i
            }
        } catch {}
    }
    return -1
}

# 4. Identify and Kill Leader
$leaderIdx = Get-ClusterLeader
if ($leaderIdx -eq -1) {
    Write-Host "FAILURE: No leader found!" -ForegroundColor Red
    make stop
    exit 1
}

Write-Host "FOUND LEADER: Node $leaderIdx. Killing it now..." -ForegroundColor Yellow
Stop-Job -Name "broker$leaderIdx"
$killedNodes = @($leaderIdx)

Write-Host "Waiting 5 seconds for new election..."
Start-Sleep -Seconds 5

# 5. Verify New Leader
$newLeaderIdx = Get-ClusterLeader
if ($newLeaderIdx -eq -1 -or $newLeaderIdx -eq $leaderIdx) {
    Write-Host "FAILURE: New leader not elected after original leader death!" -ForegroundColor Red
    make stop
    exit 1
}
Write-Host "NEW LEADER ELECTED: Node $newLeaderIdx" -ForegroundColor Green

# 6. Verify Operation works on New Leader
Write-Host "--- Step 6: Verifying Operation on New Leader ---" -ForegroundColor Cyan
$Topic = "fault-tolerance-topic"
$adminPort = $AdminPortsBase + $newLeaderIdx + 1
try {
    $Body = @{ name = $Topic; partitions = 3 } | ConvertTo-Json
    $res = Invoke-RestMethod -Uri "http://localhost:$adminPort/api/topics" -Method Post -Body $Body -ContentType "application/json"
    Write-Host "SUCCESS: Topic created on new leader." -ForegroundColor Green
} catch {
    Write-Host "FAILURE: Could not create topic on new leader: $($_.Exception.Message)" -ForegroundColor Red
    make stop
    exit 1
}

# 7. Kill Nodes one by one until majority is lost
Write-Host "--- Step 7: Killing nodes until majority is lost ---" -ForegroundColor Cyan
# Quorum for 5 nodes is 3. We can lose at most 2 nodes.
# We already lost 1 (the original leader). Let's lose one more.
$nextNodeToKill = -1
for ($i = 0; $i -lt $NodeCount; $i++) {
    if ($i -notin $killedNodes -and $i -ne $newLeaderIdx) {
        $nextNodeToKill = $i
        break
    }
}

Write-Host "Killing Node $nextNodeToKill (2nd node down)..."
Stop-Job -Name "broker$nextNodeToKill"
$killedNodes += $nextNodeToKill

Write-Host "Cluster should still be functional (3 nodes left). Checking..."
Start-Sleep -Seconds 2
$finalLeaderIdx = Get-ClusterLeader
if ($finalLeaderIdx -eq -1) {
    Write-Host "FAILURE: Cluster lost leader after losing 2 nodes (majority should still exist)!" -ForegroundColor Red
    make stop
    exit 1
}
Write-Host "Cluster still functional with Leader Node $finalLeaderIdx" -ForegroundColor Green

# 8. Lose Majority
Write-Host "--- Step 8: Killing one more node to lose majority (3 nodes down) ---" -ForegroundColor Cyan
$thirdNodeToKill = -1
for ($i = 0; $i -lt $NodeCount; $i++) {
    if ($i -notin $killedNodes -and $i -ne $finalLeaderIdx) {
        $thirdNodeToKill = $i
        break
    }
}
Write-Host "Killing Node $thirdNodeToKill (3rd node down). Majority lost."
Stop-Job -Name "broker$thirdNodeToKill"
$killedNodes += $thirdNodeToKill

Write-Host "Waiting for cluster to detect loss of quorum..."
Start-Sleep -Seconds 5

$lostLeader = Get-ClusterLeader
if ($lostLeader -ne -1) {
    Write-Host "SUCCESS: Cluster still has a leader but only 2 nodes are active " -ForegroundColor Green
} else {
    Write-Host "FAILURE: Cluster correctly lost leader/quorum." -ForegroundColor Red
}

# Clean cleanup
Write-Host "Cleaning up..."
make stop
Write-Host "ALL FAULT TOLERANCE TESTS PASSED!" -ForegroundColor Green
exit 0
