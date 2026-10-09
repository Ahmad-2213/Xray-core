param([string]$Repo = (Get-Location).Path, [switch]$Wide, [string]$Every = "", [string]$Dur = "", [string]$Only = "")
$ErrorActionPreference = "Stop"
Set-Location $Repo
$H = Join-Path $Repo "livetest"
go build -o "$H\xray.exe" ./main
go build -o "$H\live.exe" ./livetest/harness

# Widened window (review prescription): shorter kill interval plus a
# throttled reader on the check side. Explicit -Every/-Dur override.
$RelayFirst = "12s"; $RelayEvery = "15s"; $CheckDur = "75s"; $ReadDelay = "0s"
if ($Wide) { $RelayFirst = "5s"; $RelayEvery = "3s"; $CheckDur = "300s"; $ReadDelay = "5ms" }
if ($Every -ne "") { $RelayEvery = $Every }
if ($Dur -ne "") { $CheckDur = $Dur }

$serverCfg = @'
{
  "log": {"loglevel": "debug", "access": "none", "error": "server-error.log"},
  "inbounds": [{"tag": "in", "listen": "127.0.0.1", "port": 20011, "protocol": "vless",
    "settings": {"clients": [{"id": "11111111-1111-4111-8111-111111111111"}], "decryption": "none"},
    "streamSettings": {"network": "ws", "wsSettings": {"path": "/r"}}}],
  "outbounds": [{"protocol": "freedom", "tag": "direct",
    "settings": {"finalRules": [{"action": "allow", "ip": ["127.0.0.0/8"]}]}}]
}
'@
$clientTpl = @'
{
  "log": {"loglevel": "debug", "access": "none", "error": "client-error.log"},
  "inbounds": [{"tag": "socks-in", "listen": "127.0.0.1", "port": 18008, "protocol": "socks",
    "settings": {"auth": "noauth", "udp": false}}],
  "outbounds": [{"tag": "game", "protocol": "vless",
    "settings": {"vnext": [{"address": "127.0.0.1", "port": 20012,
      "users": [{"id": "11111111-1111-4111-8111-111111111111", "encryption": "none"}]}]},
    "streamSettings": {"network": "ws", "wsSettings": {"path": "/r"}},
    "mux": {"enabled": true, "concurrency": 8, "xudpConcurrency": 16},
    "muxResume": {"enabled": __RESUME__}}]
}
'@

function Run-Scenario([string]$Name, [bool]$Resume, [string]$Stall) {
  $d = Join-Path $H $Name
  Remove-Item $d -Recurse -Force -ErrorAction SilentlyContinue
  New-Item -ItemType Directory -Force $d | Out-Null
  Set-Content -Path "$d\server.json" -Value $serverCfg
  $r = if ($Resume) { 'true' } else { 'false' }
  Set-Content -Path "$d\client.json" -Value ($clientTpl -replace '__RESUME__', $r)
  function Start-Bg($exe, $argv, $tag) {
    Start-Process -FilePath $exe -ArgumentList $argv -WorkingDirectory $d -PassThru -WindowStyle Hidden `
      -RedirectStandardOutput "$d\$tag.out" -RedirectStandardError "$d\$tag.err"
  }
  $procs = @()
  $procs += Start-Bg "$H\live.exe" @("sink", "-listen", "127.0.0.1:19000") "sink"
  $srv = Start-Bg "$H\xray.exe" @("run", "-c", "$d\server.json") "xray-server"
  $procs += $srv
  $procs += Start-Bg "$H\live.exe" @("relay", "-listen", "127.0.0.1:20012", "-target", "127.0.0.1:20011", "-first", $RelayFirst, "-every", $RelayEvery, "-stall", $Stall) "relay"
  Start-Sleep 2
  $cli = Start-Bg "$H\xray.exe" @("run", "-c", "$d\client.json") "xray-client"
  $procs += $cli
  Start-Sleep 3
  & "$H\live.exe" check -socks 127.0.0.1:18008 -target 127.0.0.1:19000 -dur $CheckDur -steady 2 -bulk 1 -readDelay $ReadDelay | Tee-Object "$d\check.out" | Out-Host
  $code = $LASTEXITCODE
  $mem = (Get-Process -Id $srv.Id -ErrorAction SilentlyContinue).WorkingSet64
  $procs | ForEach-Object { Stop-Process -Id $_.Id -Force -ErrorAction SilentlyContinue }
  Start-Sleep 1
  $sv  = @(Select-String -Path "$d\server-error.log" -Pattern 'adopted|parked' -ErrorAction SilentlyContinue).Count
  $cl  = @(Select-String -Path "$d\client-error.log" -Pattern 'reattached' -ErrorAction SilentlyContinue).Count
  $bad = @(Select-String -Path "$d\server-error.log","$d\client-error.log" -Pattern 'unknown status|token mismatch|failed to add new session|duplicate New' -ErrorAction SilentlyContinue).Count
  [pscustomobject]@{Scenario=$Name; CheckExit=$code; ServerParkedAdopted=$sv; ClientReattached=$cl; BadSignals=$bad; ServerMemMB=[math]::Round($mem/1MB,1)}
}

$results = @()
$run = @{
  "resume-on-rst"      = { Run-Scenario "resume-on-rst"      $true  "0s" }
  "resume-off-rst"     = { Run-Scenario "resume-off-rst"     $false "0s" }
  "resume-on-halfopen" = { Run-Scenario "resume-on-halfopen" $true  "7s" }
}
$names = @("resume-on-rst", "resume-off-rst", "resume-on-halfopen")
if ($Only -ne "") { $names = @($Only) }
foreach ($n in $names) { $results += & $run[$n] }
$results | Format-Table -AutoSize
