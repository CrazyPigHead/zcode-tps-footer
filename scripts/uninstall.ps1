# ZCode-TPS-Footer 一键回滚：
#   移除用户级 MCP 注册 + 按版本恢复原始 app.asar + 停止数据服务
# 用法（PowerShell，任意目录）：
#   powershell -ExecutionPolicy Bypass -File scripts\uninstall.ps1
#   可选 -ZCodeDir 指定安装目录；asar 恢复需要 ZCode 已退出（或带 -KillZCode）
param(
  [string]$ZCodeDir = "",
  [switch]$KillZCode
)

$ErrorActionPreference = "Continue"
$Here = Split-Path -Parent $MyInvocation.MyCommand.Path
$node = (Get-Command node -ErrorAction SilentlyContinue).Source

# 1) 停止数据服务（按命令行匹配本项目的数据面进程；MCP 监督者随会话退出，无需处理）
Write-Host "== 停止数据服务 ..."
Get-CimInstance Win32_Process -Filter "Name LIKE 'node%'" -ErrorAction SilentlyContinue |
  Where-Object { $_.CommandLine -like "*server.mjs*" -and $_.CommandLine -like "*tps-footer*" } |
  ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue; Write-Host "   已停止服务进程 pid=$($_.ProcessId)" }

# 2) 移除用户级 MCP 注册（mcp-register.mjs 只动 zcode-tps-footer 条目，其余设置原样保留）
Write-Host "== 移除用户级 MCP 注册 ..."
if ($node) {
  & $node (Join-Path $Here "mcp-register.mjs") remove
  # 注册失败（如 config.json 损坏被保护性拒绝）不中止 asar 恢复，但必须让用户知情：
  # 否则 MCP 条目残留，下次会话仍会拉起服务，卸载形同虚设
  if ($LASTEXITCODE -ne 0) {
    Write-Host "⚠️ MCP 注册移除失败。请手动编辑 %USERPROFILE%\.zcode\cli\config.json，删除 mcp.servers.zcode-tps-footer 条目。" -ForegroundColor Yellow
  }
} else {
  Write-Host "⚠️ 找不到 node，请手动编辑 %USERPROFILE%\.zcode\cli\config.json，删除 mcp.servers.zcode-tps-footer。"
}

# 3) 恢复原始 app.asar（按版本匹配 app.asar.tps-bak.v<版本> 备份）
function Find-ZCodeDir {
  param([string]$Hint)
  if ($Hint) { return $Hint }
  $p = Get-Process -Name "ZCode" -ErrorAction SilentlyContinue | Where-Object { $_.Path } | Select-Object -First 1
  if ($p) { return Split-Path -Parent $p.Path }
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
if (-not $ZCodeDir) {
  Write-Host "❌ 找不到 ZCode 安装目录（resources\app.asar）。MCP 注册已移除；请用 -ZCodeDir 显式指定后重跑以恢复 asar。" -ForegroundColor Red
  exit 1
}
$Res = Join-Path $ZCodeDir "resources"
$Asar = Join-Path $Res "app.asar"
$baks = @(Get-ChildItem (Join-Path $Res "app.asar.tps-bak*") -ErrorAction SilentlyContinue)

# 当前 app.asar 是否含注入标记（node 字节扫描 data-tps-footer，无需 npx，与仓库目录名
# 无关；node 缺失时留空走单备份兜底）
$marked = ""
if ($node -and (Test-Path $Asar)) {
  $marked = & $node -e "const fs=require('fs'),b=fs.readFileSync(process.argv[1]);process.stdout.write(b.includes(Buffer.from('data-tps-footer'))?'yes':'no')" $Asar
}

# 从 asar 内 package.json 读版本：直接解析 asar 头（8 字节头 + [4B 长度][JSON 头]），
# 定位 package.json 条目按 offset 读取，免 npx、离线可用；失败返回空串。
# JS 一律用单引号：PS 5.1 给原生命令传参会吞掉内嵌双引号（"" → "）导致脚本损坏
function Read-AsarVersion($path) {
  $js = @'
const fs = require('fs'), p = process.argv[1];
const fd = fs.openSync(p, 'r');
const sb = Buffer.alloc(8); fs.readSync(fd, sb, 0, 8, 0);
const hs = sb.readUInt32LE(4);
const hb = Buffer.alloc(hs); fs.readSync(fd, hb, 0, hs, 8);
const j = JSON.parse(hb.slice(8, 8 + hb.readUInt32LE(4)).toString());
const e = j.files['package.json'];
if (!e || e.files) process.exit(2);
const buf = Buffer.alloc(e.size);
fs.readSync(fd, buf, 0, e.size, 8 + hs + Number(e.offset));
process.stdout.write(String(JSON.parse(buf.toString()).version || ''));
'@
  try { return [string](& $node -e $js $path 2>$null) } catch { return "" }
}

if ($marked -eq "no") {
  # 当前包就是官方原包（从未注入，或升级覆盖后尚未重注入）：无需恢复，清掉过期备份
  Write-Host "== 当前 app.asar 不含注入标记（官方原包），无需恢复"
  foreach ($b in $baks) { Remove-Item $b.FullName -Force; Write-Host "   已清理过期备份 $($b.Name)" }
  Write-Host "✅ app.asar 本就是官方原包，无需恢复。"
} elseif ($baks.Count -eq 0) {
  Write-Host "⚠️ 没找到原包备份（app.asar.tps-bak*，可能从未注入过）。"
} else {
  # 挑与当前注入包版本一致的备份；版本取不出或没有匹配时，仅有一份备份就沿用并提示
  $curVer = ""
  if ($node -and (Test-Path $Asar)) { $curVer = Read-AsarVersion $Asar }
  $bak = $null
  if ($curVer) {
    $bak = $baks | Where-Object Name -eq "app.asar.tps-bak.v$curVer" | Select-Object -First 1
    if (-not $bak) { Write-Host "⚠️ 没有与当前版本（$curVer）一致的备份，现有：$($baks.Name -join ', ')" -ForegroundColor Yellow }
  }
  if (-not $bak -and $baks.Count -eq 1) {
    $bak = $baks[0]
    Write-Host "⚠️ 仅有一份备份（$($bak.Name)），无法核对版本，按它恢复。" -ForegroundColor Yellow
  }
  if (-not $bak) {
    Write-Host "❌ 无法确定用哪份备份恢复：$($baks.Name -join ', ')。可手动把版本相符的一份复制为 app.asar 完成恢复。" -ForegroundColor Red
    exit 1
  }
  $running = Get-Process -Name "ZCode" -ErrorAction SilentlyContinue
  if ($running) {
    if ($KillZCode) {
      Stop-Process -Name "ZCode" -Force -ErrorAction SilentlyContinue
      Start-Sleep -Seconds 2
    } else {
      Write-Host "❌ ZCode 正在运行，恢复 app.asar 前请先完全退出（或带 -KillZCode 重跑）。" -ForegroundColor Red
      exit 1
    }
  }
  try {
    Copy-Item $bak.FullName $Asar -Force
    foreach ($b in $baks) { Remove-Item $b.FullName -Force -ErrorAction SilentlyContinue } # 恢复后备份即失效
    Write-Host "✅ 已用 $($bak.Name) 恢复原始 app.asar。"
    Write-Host "   app.asar.unpacked 保留未动：官方原包自身引用其中的外置文件（node-pty/ssh2 等），"
    Write-Host "   注入重打包产物的外置集合与原版一致，且版本已与恢复的 asar 匹配。"
    Write-Host "   完全退出 ZCode 再打开即回到纯原生。"
  } catch {
    Write-Host "❌ 恢复失败（可能无写权限）：$_  请以管理员身份重跑本脚本。" -ForegroundColor Red
    exit 1
  }
}

Write-Host "   仓库 logs/ 保留未删（日志），不用了可整仓删除。"
