# install.ps1 注入检测/重写核心逻辑的纯字符串测试（不碰真实 asar），另验 uninstall.ps1
# 的字节扫描判定。正则、fileUrl 计算、isCurrent 判定必须与 scripts/install.ps1 逐字一致，
# 改那边必须同步改这里；本测试用 Windows PowerShell 5.1 语法（无三元等 PS7 特性）。
#
# 运行：powershell -ExecutionPolicy Bypass -File tools\regress\inject-tag.test.ps1

$ErrorActionPreference = "Stop"

# ---------------------------------------------------------------- 与 install.ps1 逐字一致的逻辑
$InjTagRegex = '<script[^>]*\bsrc="file://[^"]*inject/inject\.js"[^>]*>'

function Get-FileUrl($injectSrc) {
  "file:///" + (($injectSrc -replace '\\', '/') -replace ' ', '%20')
}

function Get-InjectionState($html, $fileUrl) {
  $tags = [regex]::Matches($html, $InjTagRegex)
  $curSrc = ""
  if ($tags.Count -ge 1) { $curSrc = [regex]::Match($tags[0].Value, 'src="([^"]*)"').Groups[1].Value }
  $isCurrent = ($tags.Count -eq 1 -and $curSrc -eq $fileUrl -and $tags[0].Value -match 'data-tps-footer')
  [pscustomobject]@{ Count = $tags.Count; CurSrc = $curSrc; IsCurrent = $isCurrent }
}

function Invoke-Rewrite($html, $fileUrl) {
  $wantTag = "    <script defer data-tps-footer=`"1`" src=`"$fileUrl`"></script>"
  $html = [regex]::Replace($html, $InjTagRegex, "")
  $at = $html.IndexOf("</head>")
  if ($at -lt 0) { $at = $html.IndexOf("</body>") }
  if ($at -lt 0) { throw "index.html 里找不到 </head> 或 </body> 注入点" }
  $html.Insert($at, $wantTag + "`n")
}

# ---------------------------------------------------------------- 用例
$head = "<!doctype html>`n<html>`n<head>`n  <meta charset=`"utf-8`">`n  <title>ZCode</title>`n</head>`n<body>`n<div id=`"root`"></div>`n</body>`n</html>"
function With-Tag($html, $tag) { $html.Replace("</head>", "$tag`n</head>") }

$currentUrl = Get-FileUrl "D:\Projects\zhz\zcode-tps-footer\inject\inject.js"
$spaceUrl = Get-FileUrl "E:\my tps\inject\inject.js"   # 带空格路径：锁定 %20 转义行为

$cases = @(
  @{ name = "官方干净包 → fresh"; html = $head; url = $currentUrl; expect = "fresh" },
  @{ name = "路径匹配 → skip"; html = With-Tag $head "    <script defer data-tps-footer=`"1`" src=`"$spaceUrl`"></script>"; url = $spaceUrl; expect = "skip" },
  @{ name = "路径过期（仓库已移动） → rewrite"; html = With-Tag $head "    <script defer data-tps-footer=`"1`" src=`"$currentUrl`"></script>"; url = $spaceUrl; expect = "rewrite" },
  @{ name = "重复双标签 → rewrite"; html = With-Tag (With-Tag $head "    <script defer data-tps-footer=`"1`" src=`"$currentUrl`"></script>") "    <script defer data-tps-footer=`"1`" src=`"$currentUrl`"></script>"; url = $currentUrl; expect = "rewrite" }
)

$fail = 0
# 字面值断言：spaceUrl 两侧同源计算的自洽比较测不出转义写坏，必须锁死 %20 产物本身
if ($spaceUrl -cne "file:///E:/my%20tps/inject/inject.js") {
  Write-Host "FAIL spaceUrl %20 转义字面值：实际=$spaceUrl"
  $fail++
} else {
  Write-Host "PASS spaceUrl %20 转义字面值"
}
foreach ($c in $cases) {
  $st = Get-InjectionState $c.html $c.url
  $verdict = if ($st.IsCurrent) { "skip" } elseif ($st.Count -eq 0) { "fresh" } else { "rewrite" }
  $ok = $verdict -eq $c.expect
  $mark = if ($ok) { "PASS" } else { "FAIL" }
  Write-Host "$mark $($c.name) (tags=$($st.Count) expect=$($c.expect) got=$verdict)"
  if (-not $ok) { $fail++ }
  if ($verdict -eq "rewrite") {
    # 重写产物：恰好 1 个标签、指向当前 URL、带 data-tps-footer（即复判为 skip 态）
    $out = Invoke-Rewrite $c.html $c.url
    $st2 = Get-InjectionState $out $c.url
    $ok2 = ($st2.Count -eq 1 -and $st2.IsCurrent)
    $mark2 = if ($ok2) { "PASS" } else { "FAIL" }
    Write-Host "$mark2   重写产物：tags=$($st2.Count) current=$($st2.IsCurrent) src=$($st2.CurSrc)"
    if (-not $ok2) { $fail++ }
  }
}

# ---------------------------------------------------------------- uninstall.ps1 字节扫描判定
$node = (Get-Command node -ErrorAction SilentlyContinue).Source
if ($node) {
  $tmp = Join-Path ([IO.Path]::GetTempPath()) ("tps-injtag-test-" + [Guid]::NewGuid().ToString("N"))
  New-Item -ItemType Directory -Path $tmp | Out-Null
  try {
    $scanCases = @(
      @{ name = "官方干净包未注入"; data = $head; expect = "no" },
      @{ name = "已注入"; data = With-Tag $head "    <script defer data-tps-footer=`"1`" src=`"$currentUrl`"></script>"; expect = "yes" }
    )
    $i = 0
    foreach ($sc in $scanCases) {
      $i++
      $f = Join-Path $tmp "case$i.bin"
      [IO.File]::WriteAllText($f, $sc.data, [Text.UTF8Encoding]::new($false))
      $got = & $node -e "const fs=require('fs'),b=fs.readFileSync(process.argv[1]);process.stdout.write(b.includes(Buffer.from('data-tps-footer'))?'yes':'no')" $f
      $ok = $got -eq $sc.expect
      $mark = if ($ok) { "PASS" } else { "FAIL" }
      Write-Host "$mark bytescan: $($sc.name) (expect=$($sc.expect) got=$got)"
      if (-not $ok) { $fail++ }
    }
  } finally {
    Remove-Item $tmp -Recurse -Force -ErrorAction SilentlyContinue
  }
} else {
  Write-Host "SKIP bytescan: node 不可用"
}

if ($fail -gt 0) { Write-Host "INJECT-TAG-TEST-FAIL ($fail 失败)"; exit 1 }
Write-Host "INJECT-TAG-TEST-PASS"
