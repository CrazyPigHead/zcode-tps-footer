# 用户直启数据服务守护链——SSH-only 场景的可靠启动方式。
#
# 为什么需要它：ZCode 桌面端把每个 stdio MCP server（本仓库注册的 supervisor 就是）挂进
# 一个 KILL_ON_JOB_CLOSE 的 Windows Job Object，MCP 连接关闭（会话结束/切换后 30 秒宽限、
# mcp-status 泳道闲置 5 分钟等）时 TerminateJobObject + taskkill /T 连坐全部后代——
# supervisor detached 拉起的监视器与数据服务同死且不留一行日志；而 Remote SSH 会话的 MCP
# 在远端执行，本地没有连接、没有看门狗，链死了无人复活（详见 README「SSH-only 使用」一节）。
# 由用户本人启动的进程在 ZCode 的进程树与 job 之外，回收机制够不着，链可活到关机。
#
# 用法（独立终端；在 ZCode 会话内执行会被检测并拒绝——那样拉起的链照样陪葬）：
#   powershell -ExecutionPolicy Bypass -File scripts\start-service.ps1
# 可重复执行：服务已在岗时什么都不动；开机后 / 开始 SSH-only 工作前 / 统计行消失后跑一次即可。
param(
  [int]$TimeoutSec = 20,
  [switch]$Force
)

$RepoRoot = Split-Path -Parent $PSScriptRoot

# --- 1. 拒绝在 ZCode 进程树内启动（在这里拉起的链会被连坐，白跑）：沿父进程链上溯，
# 撞见 ZCode 主进程（Electron）或命令行含 zcode 的 node（agent 运行时）即拒绝。
# 从本进程的父辈查起：本脚本路径自身含 "zcode-tps-footer"，查自己必然误伤。
if (-not $Force) {
  $cur = Get-CimInstance Win32_Process -Filter "ProcessId = $PID"
  $anc = Get-CimInstance Win32_Process -Filter "ProcessId = $($cur.ParentProcessId)" -ErrorAction SilentlyContinue
  for ($i = 0; $i -lt 12 -and $anc; $i++) {
    if ($anc.Name -ieq "ZCode.exe" -or ($anc.Name -like "node*" -and $anc.CommandLine -match "zcode")) {
      Write-Host "❌ 当前终端在 ZCode 的进程树内（祖先 $($anc.Name) pid=$($anc.ProcessId)）。" -ForegroundColor Red
      Write-Host "   在这里拉起的守护链仍会被 ZCode 的进程树回收连坐。请在独立的 PowerShell / Windows Terminal 中运行本脚本。" -ForegroundColor Yellow
      exit 1
    }
    $anc = Get-CimInstance Win32_Process -Filter "ProcessId = $($anc.ParentProcessId)" -ErrorAction SilentlyContinue
  }
}

# --- 2. 已在岗：不重启不扰动，直接退出
function Test-Port3117 {
  try { $c = New-Object Net.Sockets.TcpClient; $c.Connect("127.0.0.1", 3117); $c.Close(); return $true } catch { return $false }
}
if (Test-Port3117) {
  Write-Host "✅ 数据服务已在运行（127.0.0.1:3117 可达），无需操作。" -ForegroundColor Green
  exit 0
}

# --- 3. 清本仓库残留（先监视器后服务，顺序不能反：监视器在岗会把刚杀的服务秒级拉回）
$monitorPath = Join-Path $RepoRoot "server\monitor.mjs"
$serverPath = Join-Path $RepoRoot "server\server.mjs"
$procs = Get-CimInstance Win32_Process -Filter "Name LIKE 'node%'" -ErrorAction SilentlyContinue |
  Where-Object { $_.CommandLine -and ($_.CommandLine.IndexOf($monitorPath, [StringComparison]::OrdinalIgnoreCase) -ge 0 -or $_.CommandLine.IndexOf($serverPath, [StringComparison]::OrdinalIgnoreCase) -ge 0) }
$monitors = @($procs | Where-Object { $_.CommandLine.IndexOf($monitorPath, [StringComparison]::OrdinalIgnoreCase) -ge 0 })
$services = @($procs | Where-Object { $_.CommandLine.IndexOf($monitorPath, [StringComparison]::OrdinalIgnoreCase) -lt 0 })
foreach ($p in ($monitors + $services)) {
  Write-Host "   清理残留 pid=$($p.ProcessId)（$($p.Name)）"
  Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
}
if ($procs) { Start-Sleep -Seconds 1 }

# --- 4. detached 拉起监视器（与 install.ps1 同款）：监视器拉起并拥有数据服务
$node = (Get-Command node -ErrorAction SilentlyContinue).Source
if (-not $node) {
  Write-Host "❌ 找不到 node（数据服务需要 Node.js ≥ 23.4），请确认在 PATH 上可见。" -ForegroundColor Red
  exit 1
}
Start-Process -FilePath $node -ArgumentList ('"' + $monitorPath + '"') -WindowStyle Hidden -WorkingDirectory $RepoRoot

# --- 5. 等就绪（TcpClient 探活与 install.ps1 同款，天然不走系统代理）
for ($i = 0; $i -lt $TimeoutSec; $i++) {
  Start-Sleep -Seconds 1
  if (Test-Port3117) {
    Write-Host "✅ 数据服务已就绪（127.0.0.1:3117）。" -ForegroundColor Green
    Write-Host "   这条链由用户终端拉起、在 ZCode 进程树之外——ZCode 回收 MCP 连接时不会再连坐它，" -ForegroundColor Green
    Write-Host "   SSH-only 工作时段统计行持续可用，直到关机或手动停止（tools\regress\killnode.ps1）。" -ForegroundColor Green
    Write-Host "   日志：$RepoRoot\logs\monitor.log 与 server.log" -ForegroundColor Gray
    exit 0
  }
}
Write-Host "⚠️ ${TimeoutSec}s 内 3117 未就绪。排查：$RepoRoot\logs\monitor.log 与 server.log（Node ≥ 23.4、端口占用）。" -ForegroundColor Yellow
exit 1
