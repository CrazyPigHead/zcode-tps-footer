# 按本仓库 monitor.mjs / server.mjs 精确路径匹配（目录改名后仍有效），不硬编码仓库名。
# 监视器必须先杀：它会把刚杀掉的服务在数秒内拉回来，先杀服务 = 清理无效
$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$targets = @(
  (Join-Path $repoRoot "server\monitor.mjs"),
  (Join-Path $repoRoot "server\server.mjs")
)
$procs = Get-CimInstance Win32_Process -Filter "Name LIKE 'node%'" -ErrorAction SilentlyContinue |
  Where-Object { $_.CommandLine -and ($_.CommandLine.IndexOf($targets[0], [StringComparison]::OrdinalIgnoreCase) -ge 0 -or $_.CommandLine.IndexOf($targets[1], [StringComparison]::OrdinalIgnoreCase) -ge 0) }
if (-not $procs) { Write-Output "no matching node process"; exit }
# 先监视器后服务，避免杀服务触发监视器重拉。不用 Sort-Object 的脚本块属性语法
# （PS5.1 下静默失效导致整个管道为空、一个进程都杀不掉），显式算好顺序再遍历。
$monitors = @($procs | Where-Object { $_.CommandLine.IndexOf($targets[0], [StringComparison]::OrdinalIgnoreCase) -ge 0 })
$services = @($procs | Where-Object { $_.CommandLine.IndexOf($targets[0], [StringComparison]::OrdinalIgnoreCase) -lt 0 })
foreach ($p in ($monitors + $services)) {
  Write-Output "killing node pid=$($p.ProcessId)"
  Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
}
