# 按本仓库 server.mjs 精确路径匹配（目录改名后仍有效），不硬编码仓库名
$serverPath = Join-Path (Split-Path -Parent (Split-Path -Parent $PSScriptRoot)) "server\server.mjs"
$procs = Get-CimInstance Win32_Process -Filter "Name LIKE 'node%'" -ErrorAction SilentlyContinue |
  Where-Object { $_.CommandLine -and $_.CommandLine.IndexOf($serverPath, [StringComparison]::OrdinalIgnoreCase) -ge 0 }
foreach ($p in $procs) {
  Write-Output "killing node pid=$($p.ProcessId)"
  Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
}
if (-not $procs) { Write-Output "no matching node process" }
