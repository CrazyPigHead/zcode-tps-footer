# ZCode-TPS-Footer 一键安装
#
# 仓库即安装：MCP 注册与 asar 注入都直接指向本仓库目录，**克隆下来的目录须持久保留**
#（移动/删除仓库 = 卸载，重装只需在新位置重跑本脚本）。
#
# 用法（PowerShell，任意目录）：
#   powershell -ExecutionPolicy Bypass -File scripts\install.ps1
#   可选参数：
#     -ZCodeDir "D:\ProgramFiles\ZCode"   显式指定 ZCode 安装目录（自动探测失败时）
#     -KillZCode                          自动结束正在运行的 ZCode（替换 app.asar 前必须关闭）
#
# 做三件事：
#   1. 注册用户级 MCP server（~/.zcode/cli/config.json → mcp.servers，注册项只指向
#      仓库内 supervisor.mjs）：ZCode 每个会话建立 MCP 连接时自动拉起数据服务，
#      崩溃自动重拉
#   2. 拉起数据服务（仅本次安装验证用）
#   3. 解包 app.asar → 渲染层 index.html 注入一行 <script>（直接指向仓库内的
#      inject/inject.js，无部署副本，标签带 data-tps-footer 标记）→ 重打包。
#      注入检测与目录名无关：仓库移动/改名后重跑本脚本会自动把注入 URL 重写为当前位置。
#      官方原包按版本备份为 resources\app.asar.tps-bak.v<版本>：重新注入时自动删除旧版本
#      备份、对当前包新建备份；卸载时用与当前包版本一致的备份恢复
# 远端主机无需任何配置：自动解析自 ZCode 桌面端设置（最近 SSH 会话）。
# ZCode 整包更新覆盖 app.asar 后，重跑本脚本即可恢复（备份同步刷新为新版）。

param(
  [string]$ZCodeDir = "",
  [switch]$KillZCode
)

$ErrorActionPreference = "Stop"
$Here = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $Here
$InjectSrc = Join-Path $RepoRoot "inject\inject.js"
# 本工具注入标签的探测/清除模式：注入 URL 恒以 inject/inject.js 结尾，与仓库目录名/
# 位置无关；官方 index.html 不会有此形态
$InjTagRegex = '<script[^>]*\bsrc="file://[^"]*inject/inject\.js"[^>]*>'

function Write-Step($msg) { Write-Host "== $msg" -ForegroundColor Cyan }

# ---------------------------------------------------------------- 0. 定位 ZCode 与 node
function Find-ZCodeDir {
  param([string]$Hint)
  if ($Hint) { return $Hint }
  # a) 正在运行的进程路径（最可靠）
  $p = Get-Process -Name "ZCode" -ErrorAction SilentlyContinue | Where-Object { $_.Path } | Select-Object -First 1
  if ($p) { return Split-Path -Parent $p.Path }
  # b) 常见安装位置
  foreach ($c in @(
      "$env:LOCALAPPDATA\Programs\ZCode",
      "D:\ProgramFiles\ZCode",
      "$env:ProgramFiles\ZCode",
      "${env:ProgramFiles(x86)}\ZCode"
    )) {
    if (Test-Path (Join-Path $c "resources\app.asar")) { return $c }
  }
  return $null
}

$ZCodeDir = Find-ZCodeDir $ZCodeDir
if (-not $ZCodeDir -or -not (Test-Path (Join-Path $ZCodeDir "resources\app.asar"))) {
  Write-Host "❌ 找不到 ZCode 安装目录（resources\app.asar）。请用 -ZCodeDir 显式指定，例如：" -ForegroundColor Red
  Write-Host '   powershell -ExecutionPolicy Bypass -File scripts\install.ps1 -ZCodeDir "D:\ProgramFiles\ZCode"' -ForegroundColor Yellow
  exit 1
}
$Res = Join-Path $ZCodeDir "resources"
$Asar = Join-Path $Res "app.asar"
$node = (Get-Command node -ErrorAction SilentlyContinue).Source
if (-not $node) {
  Write-Host "❌ 找不到 node（MCP 监督者与数据服务都需要 Node.js ≥ 23.4）。请安装后在 PATH 里可见再重跑。" -ForegroundColor Red
  exit 1
}
Write-Host "ZCode 安装目录：$ZCodeDir"
Write-Host "本仓库（安装根，须持久保留）：$RepoRoot"

# ---------------------------------------------------------------- 1. 注册用户级 MCP server
Write-Step "注册用户级 MCP server（mcp.servers.zcode-tps-footer）"
& $node (Join-Path $Here "mcp-register.mjs") add $RepoRoot
# PS 5.1 下原生命令非零退出码不触发 EAP=Stop，必须显式查 LASTEXITCODE，
# 否则注册因配置异常中止后安装照样走完、结尾仍报成功
if ($LASTEXITCODE -ne 0) {
  Write-Host "❌ MCP 注册失败，已中止安装（用户配置未被改动）。请按上方提示处理后重跑。" -ForegroundColor Red
  exit 1
}

# ---------------------------------------------------------------- 2.（重）启动数据服务
Write-Step "（重）启动数据服务"
# 先停常驻监视器（monitor.mjs）：它 healthz 失联会秒级重拉服务，不停它下面的
# 清理就是猫鼠游戏。主路径按 3118 仲裁端口属主找（位置无关：仓库移动/改名后，
# 运行中旧监视器的命令行还是老路径，按路径匹配会漏杀，旧监视器就会一直占着
# 仲裁端口，新监视器全部起不来——与下方 3117 的端口属主清理同一个教训）。
$monListeners = Get-NetTCPConnection -LocalPort 3118 -State Listen -ErrorAction SilentlyContinue
foreach ($procId in @($monListeners | Select-Object -ExpandProperty OwningProcess -Unique)) {
  if (-not $procId) { continue }
  $p = Get-CimInstance Win32_Process -Filter "ProcessId=$procId" -ErrorAction SilentlyContinue
  if ($p -and $p.Name -like "node*" -and $p.CommandLine -like "*monitor.mjs*") {
    Stop-Process -Id $procId -Force -ErrorAction SilentlyContinue
    Write-Host "   已停止旧监视器进程 pid=$procId（3118 端口属主）"
  } else {
    Write-Host "⚠️ 3118 端口被非本工具进程占用（pid=$procId）；新监视器将以无仲裁降级模式运行。" -ForegroundColor Yellow
  }
}
# 兜底：改过 TPS_MONITOR_PORT 的测试实例不占 3118，按本仓库精确路径补杀
$monitorPath = Join-Path $RepoRoot "server\monitor.mjs"
Get-CimInstance Win32_Process -Filter "Name LIKE 'node%'" -ErrorAction SilentlyContinue |
  Where-Object { $_.CommandLine -and $_.CommandLine.IndexOf($monitorPath, [StringComparison]::OrdinalIgnoreCase) -ge 0 } |
  ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue; Write-Host "   已停止旧监视器进程 pid=$($_.ProcessId)（本仓库 monitor.mjs）" }
# 清理旧服务按 3117 端口属主找：命令行通配 "*tps-footer*" 在仓库改名/移动后失灵
#（旧路径不再含该字样，旧服务长期占港）。只动「node 且命令行含 server.mjs」的
# 属主；端口被无关进程占用时不误杀、仅告警。
$listeners = Get-NetTCPConnection -LocalPort 3117 -State Listen -ErrorAction SilentlyContinue
foreach ($procId in @($listeners | Select-Object -ExpandProperty OwningProcess -Unique)) {
  if (-not $procId) { continue }
  $p = Get-CimInstance Win32_Process -Filter "ProcessId=$procId" -ErrorAction SilentlyContinue
  $ownerName = if ($p) { $p.Name } else { "未知进程" }
  if ($p -and $p.Name -like "node*" -and $p.CommandLine -like "*server.mjs*") {
    Stop-Process -Id $procId -Force -ErrorAction SilentlyContinue
    Write-Host "   已停止旧服务进程 pid=$procId（3117 端口属主）"
  } else {
    Write-Host "⚠️ 3117 端口被非本工具进程占用（pid=$procId，$ownerName），未动它；若服务起不来请释放该端口。" -ForegroundColor Yellow
  }
}
# 兜底：清掉命令行指向本仓库 server.mjs 的残留 node 进程（端口上看不到的场景，
# 如改过 TPS_FOOTER_PORT 的测试遗留）。按精确路径匹配，目录改名也不影响识别旧仓库的服务。
$serverPath = Join-Path $RepoRoot "server\server.mjs"
Get-CimInstance Win32_Process -Filter "Name LIKE 'node%'" -ErrorAction SilentlyContinue |
  Where-Object { $_.CommandLine -and $_.CommandLine.IndexOf($serverPath, [StringComparison]::OrdinalIgnoreCase) -ge 0 } |
  ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue; Write-Host "   已停止旧服务进程 pid=$($_.ProcessId)（本仓库 server.mjs）" }
Start-Sleep -Milliseconds 500
# 启动常驻监视器而非服务本身：监视器拉起并拥有数据服务（崩溃捕获退出码/stderr 落
# logs/monitor.log，healthz 失联秒级重拉），此后不再依赖 MCP 连接存活做守护
Start-Process -FilePath $node -ArgumentList ('"' + (Join-Path $RepoRoot "server\monitor.mjs") + '"') -WindowStyle Hidden -WorkingDirectory $RepoRoot
$healthy = $false
foreach ($i in 1..15) {
  Start-Sleep -Milliseconds 800
  try { $c = New-Object Net.Sockets.TcpClient; $c.Connect("127.0.0.1", 3117); $c.Close(); $healthy = $true; break } catch {}
}
if ($healthy) {
  Write-Host "✅ 数据服务就绪（127.0.0.1:3117；常驻监视器守护，崩溃秒级重拉、退出码与 stderr 记录在 logs\monitor.log）" -ForegroundColor Green
} else {
  Write-Host "⚠️ 服务未立即响应；重启 ZCode 后首个会话会经 MCP 自动拉起。日志：$RepoRoot\logs\server.log 与 monitor.log" -ForegroundColor Yellow
}

# ---------------------------------------------------------------- 3. asar 注入
Write-Step "检查/注入 app.asar"
$running = Get-Process -Name "ZCode" -ErrorAction SilentlyContinue
if ($running) {
  if ($KillZCode) {
    Write-Host "   -KillZCode：结束 ZCode 进程 ..."
    Stop-Process -Name "ZCode" -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 2
  } else {
    Write-Host "❌ ZCode 正在运行，替换 app.asar 前必须完全退出（含托盘）。" -ForegroundColor Red
    Write-Host "   关闭 ZCode 后重跑本脚本，或直接带 -KillZCode 运行。" -ForegroundColor Yellow
    exit 1
  }
}

# 写权限：Program Files 类目录需要管理员；不足时自动提权重跑（UAC 弹窗一次）
try {
  $probe = Join-Path $Res ".tps-write-probe"
  [IO.File]::WriteAllText($probe, "x")
  Remove-Item $probe -Force
} catch {
  Write-Host "   resources 目录无写权限，以管理员身份重新运行（UAC 确认一次）..."
  $relaunch = "-NoProfile -ExecutionPolicy Bypass -File `"$PSCommandPath`""
  if ($ZCodeDir) { $relaunch += " -ZCodeDir `"$ZCodeDir`"" }
  $relaunch += " -KillZCode"
  Start-Process powershell -Verb RunAs -ArgumentList $relaunch -Wait
  exit
}

$Repack = Join-Path $env:TEMP "zcode-tps-repack"
New-Item -ItemType Directory -Force -Path $Repack | Out-Null

# PS 5.1 下 $ErrorActionPreference=Stop 时，原生命令的 stderr 被重定向会触发
# NativeCommandError；因此调用 npx 时临时放宽 EAP、不重定向 stderr，用退出码判断成败
function Invoke-NpxRaw($argList) {
  $prev = $ErrorActionPreference
  $ErrorActionPreference = "Continue"
  try { & npx -y @electron/asar @argList } finally { $ErrorActionPreference = $prev }
  return $LASTEXITCODE
}
function Invoke-Asar($argList, $what) {
  if ((Invoke-NpxRaw $argList) -ne 0) {
    throw "$what 失败（exit=$LASTEXITCODE）。请确认 npx 可用（首次需联网拉取 @electron/asar）。"
  }
}

# 备份健全性（仅告警，不中止）：缺失 / 与当前包版本不匹配
function Test-BackupHealth($resDir, $curVer) {
  $baks = @(Get-ChildItem (Join-Path $resDir "app.asar.tps-bak*") -ErrorAction SilentlyContinue)
  if (-not $baks) {
    Write-Host "⚠️ 没有原包备份（app.asar.tps-bak*）：当前注入包无可回滚对象，卸载时将无法恢复。" -ForegroundColor Yellow
    return
  }
  if ($curVer -and -not ($baks | Where-Object Name -eq "app.asar.tps-bak.v$curVer")) {
    Write-Host "⚠️ 备份（$($baks.Name -join ', ')）与当前包版本（$curVer）不一致；ZCode 升级后重跑本脚本会自动重建匹配的备份。" -ForegroundColor Yellow
  }
}

Push-Location $Repack
try {
  Remove-Item ".\index.html", ".\package.json" -Force -ErrorAction SilentlyContinue
  # Windows 打包的 asar 内部路径分隔符是反斜杠，两种都试（找不到条目属预期分支，看产物即可）
  $null = Invoke-NpxRaw @("extract-file", $Asar, "out\renderer\index.html")
  if (-not (Test-Path ".\index.html")) {
    $null = Invoke-NpxRaw @("extract-file", $Asar, "out/renderer/index.html")
  }
  # 当前包版本（备份命名与卸载时按版本恢复都用它）；取不到则退化为无版本标记备份
  $curVer = ""
  $null = Invoke-NpxRaw @("extract-file", $Asar, "package.json")
  if (Test-Path ".\package.json") {
    try { $curVer = [string](Get-Content ".\package.json" -Raw | ConvertFrom-Json).version } catch {}
  }
  if (-not (Test-Path ".\index.html")) { throw "无法从 app.asar 提取 out\renderer\index.html（新版本布局可能变了）" }

  # 检测既有注入：标签 src 是否恰指向当前仓库。仓库被移动/改名（src 指向旧位置）、
  # 重复标签都走重写分支恢复为唯一正确标签，因此移动仓库后重跑本脚本即可修复注入。
  $html = [IO.File]::ReadAllText((Join-Path $Repack "index.html"))
  $fileUrl = "file:///" + (($InjectSrc -replace '\\', '/') -replace ' ', '%20')
  $wantTag = "    <script defer data-tps-footer=`"1`" src=`"$fileUrl`"></script>"
  $tags = [regex]::Matches($html, $InjTagRegex)
  $curSrc = ""
  if ($tags.Count -ge 1) { $curSrc = [regex]::Match($tags[0].Value, 'src="([^"]*)"').Groups[1].Value }
  $isCurrent = ($tags.Count -eq 1 -and $curSrc -eq $fileUrl -and $tags[0].Value -match 'data-tps-footer')

  if ($isCurrent) {
    Write-Host "✅ 当前 asar 注入已指向本仓库，无需重打。"
    Test-BackupHealth $Res $curVer
  } else {
    if ($tags.Count -eq 0) {
      Write-Host "   ① 备份原包（版本 $curVer）..."
      # 走到这里 = 当前 asar 无注入标签，备份的必然是官方包。先建新备份再删旧备份，
      # 复制失败会抛错中止（旧备份保留）；卸载脚本按文件名里的版本挑匹配备份恢复。
      $bakName = if ($curVer) { "app.asar.tps-bak.v$curVer" } else { "app.asar.tps-bak" }
      Copy-Item $Asar (Join-Path $Res $bakName) -Force
      Get-ChildItem (Join-Path $Res "app.asar.tps-bak*") -ErrorAction SilentlyContinue |
        Where-Object Name -ne $bakName |
        ForEach-Object { Remove-Item $_.FullName -Force; Write-Host "   已删除旧版本备份 $($_.Name)" }
    } else {
      # 已注入但指向别处或重复标签：重写为当前仓库路径。当前包不是官方原包，
      # 绝不能备份覆盖；原包备份保持不动。
      Write-Host "   ① 检测到既有注入（src=$curSrc），重写为当前仓库路径 ..."
      Test-BackupHealth $Res $curVer
    }

    Write-Host "   ② 解包 ...（首次约 1-3 分钟）"
    Remove-Item ".\unpacked" -Recurse -Force -ErrorAction SilentlyContinue
    Invoke-Asar @("extract", $Asar, "unpacked") "解包 app.asar"

    Write-Host "   ③ 注入 script 标签 ..."
    $idx = Join-Path $Repack "unpacked\out\renderer\index.html"
    if (-not (Test-Path $idx)) { throw "解包结果里没有 out\renderer\index.html（新版本布局可能变了）" }
    $html = [IO.File]::ReadAllText($idx)
    # 先清除本工具的全部既有标签（旧路径/重复标签，天然去重），再插入唯一新标签；
    # data-tps-footer 标记供 uninstall.ps1 字节扫描识别（与仓库目录名无关）
    $html = [regex]::Replace($html, $InjTagRegex, "")
    $at = $html.IndexOf("</head>")
    if ($at -lt 0) { $at = $html.IndexOf("</body>") }
    if ($at -lt 0) { throw "index.html 里找不到 </head> 或 </body> 注入点" }
    $html = $html.Insert($at, $wantTag + "`n")
    [IO.File]::WriteAllText($idx, $html, [Text.UTF8Encoding]::new($false))

    Write-Host "   ④ 重打包 ..."
    # 原包 electron-builder 外置了 .node/.exe/.dll（node-pty prebuilds、ssh2 pagent.exe 等，
    # 它们会被 spawn/加载，嵌入 asar 会坏），重打包必须保持同类外置
    Invoke-Asar @("pack", "unpacked", "app.asar.patched", "--unpack", "{**/*.node,**/*.exe,**/*.dll,**/spawn-helper}") "重打包 app.asar"

    Write-Host "   ⑤ 替换 ..."
    Copy-Item ".\app.asar.patched" $Asar -Force
    Remove-Item (Join-Path $Res "app.asar.unpacked") -Recurse -Force -ErrorAction SilentlyContinue
    Copy-Item ".\app.asar.patched.unpacked" (Join-Path $Res "app.asar.unpacked") -Recurse -Force
    Write-Host "✅ 注入完成。"
  }
} finally {
  Pop-Location
}

Write-Host ""
Write-Host "🎉 完成！完全退出 ZCode 再重新打开：每个会话建立 MCP 连接时自动确保数据服务在线，每条回答下方即出现统计行（本地与 Remote SSH 会话都有）。" -ForegroundColor Green
Write-Host "   远端数据经 SSH 拉取，要求 ssh 免密可用：ssh <user>@<host> echo ok"
Write-Host "   回滚：powershell -ExecutionPolicy Bypass -File scripts\uninstall.ps1"
Write-Host "   ZCode 更新后统计行消失属正常（asar 被覆盖），重跑本脚本即可（备份同步刷新为新版）。"
