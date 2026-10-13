// 闲置退场策略独立验收（测试端口 3199/3200，绝不碰生产 3117/3118）。
// ZCode 进程硬门（IDLE-GATE）的验证靠「假 ZCode」：把 node.exe 复制成名为
// zcode-tps-stub(.exe) 的副本进程（tasklist 报映像名、ps comm 报可执行名，副本
// 两侧都能命中；名字 ≤15 字符避开 Linux comm 截断），并把监视器的探测名单
// TPS_ZCODE_PROCS 钉在 stub 名上——否则真机上正开着的 ZCode.exe 会让退场路径
// 永远验不到。四个阶段：
//   1. 起数据服务 + 监视器（TPS_IDLE_EXIT_S=15 加速），模拟渲染层持续取数
//      → 监视器必须不退（「ZCode 开着不误退」）；
//   2. 停止取数但 ZCode 在运行（stub 存活）→ 硬门拦下退场（[IDLE-GATE]），
//      服务与监视器必须双双在岗；
//   3. 杀掉 stub（模拟 ZCode 关闭）→ 闲置退场正常发生，3199/3200 清空；
//   4. 起 supervisor（模拟重开 ZCode 建立 MCP 连接）→ 补拉监视器 → 服务复活。
// 通过标准：四段全部满足，全链路 < 150s。
import { spawn } from "node:child_process";
import fs from "node:fs";
import http from "node:http";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const REPO_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const PORT = 3199;
const MPORT = 3200;
const STUB_BASE = "zcode-tps-stub"; // ≤15 字符：Linux ps comm 截断安全
const STUB_EXE = path.join(os.tmpdir(), `${STUB_BASE}${process.platform === "win32" ? ".exe" : ""}`);
const STUB_NAMES = `${STUB_BASE}.exe,${STUB_BASE}`; // win32 报 .exe、*nix 报裸名，两名都挂
const ENV = { ...process.env, TPS_FOOTER_PORT: String(PORT), TPS_MONITOR_PORT: String(MPORT), TPS_ZCODE_PROCS: STUB_NAMES, TPS_ZCODE_GATE_S: "3" };
const MON_LOG = path.join(REPO_ROOT, "logs", "monitor.log");

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function get(p) {
  return new Promise((resolve) => {
    const req = http.get({ host: "127.0.0.1", port: PORT, path: p, timeout: 3000 }, (res) => {
      let b = "";
      res.setEncoding("utf8");
      res.on("data", (c) => (b += c));
      res.on("end", () => resolve({ code: res.statusCode, body: b }));
    });
    req.on("timeout", () => { req.destroy(); resolve(null); });
    req.on("error", () => resolve(null));
  });
}
const healthz = async () => (await get("/healthz"))?.code === 200;
function portUp(port) {
  return new Promise((resolve) => {
    const s = net.connect({ host: "127.0.0.1", port, timeout: 800 });
    s.on("connect", () => { s.destroy(); resolve(true); });
    s.on("timeout", () => { s.destroy(); resolve(false); });
    s.on("error", () => resolve(false));
  });
}
const allDown = async () => !(await healthz()) && !(await portUp(MPORT));
// 读 monitor.log 自 offset 起的新增内容（测试期间监视器写的现场都在这里面）
function monLogSince(offset) {
  try {
    const fd = fs.openSync(MON_LOG, "r");
    const st = fs.fstatSync(fd);
    const buf = Buffer.alloc(Math.max(0, st.size - offset));
    if (buf.length) fs.readSync(fd, buf, 0, buf.length, offset);
    fs.closeSync(fd);
    return buf.toString("utf8");
  } catch {
    return "";
  }
}
const monLogSize = () => { try { return fs.statSync(MON_LOG).size; } catch { return 0; } };

// 假 ZCode：node 副本进程（映像名/comm = zcode-tps-stub），存活 10 分钟兜底
let stub = null;
function startStub() {
  // 清上一轮崩溃遗留的同名进程（否则副本文件被占用，copyFileSync 失败）
  if (process.platform === "win32") spawn("taskkill", ["/IM", `${STUB_BASE}.exe`, "/F"], { stdio: "ignore", windowsHide: true });
  else spawn("pkill", ["-x", STUB_BASE], { stdio: "ignore" });
  fs.copyFileSync(process.execPath, STUB_EXE);
  if (process.platform !== "win32") fs.chmodSync(STUB_EXE, 0o755);
  stub = spawn(STUB_EXE, ["-e", "setTimeout(()=>{}, 600000)"], { stdio: "ignore", windowsHide: true });
}
function stopStub() {
  if (stub) { try { stub.kill(); } catch { /* 已退出 */ } stub = null; }
}
process.on("exit", stopStub); // 断言失败提前退出时也不留孤儿

// 清场：上一轮残留（只杀 node 属主，端口被无关进程占用时报错退出而不是误杀）
async function sweep() {
  for (let i = 0; i < 3 && !(await allDown()); i++) {
    spawn("powershell", ["-NoProfile", "-Command",
      `Get-NetTCPConnection -LocalPort ${MPORT},${PORT} -State Listen -ErrorAction SilentlyContinue | ForEach-Object { $owner = Get-CimInstance Win32_Process -Filter "ProcessId = $($_.OwningProcess)"; if ($owner -and $owner.Name -like 'node*') { Stop-Process -Id $_.OwningProcess -Force -ErrorAction SilentlyContinue } else { Write-Output "FOREIGN pid=$($_.OwningProcess) name=$($owner.Name)" } }`],
      { stdio: ["ignore", "ignore", "inherit"], windowsHide: true });
    await sleep(1000);
  }
  if (!(await allDown())) { console.error("清场失败：3199/3200 仍有占用"); process.exit(1); }
}
await sweep();

let ok = true;
const t0 = Date.now();
const log0 = monLogSize();

// ---- 段1：服务 + 监视器在岗，模拟渲染层取数 25s（覆盖 15s 阈值），不得退场
const mon = spawn(process.execPath, [path.join(REPO_ROOT, "server", "monitor.mjs")], { env: { ...ENV, TPS_IDLE_EXIT_S: "15" }, stdio: "ignore", detached: true, windowsHide: true });
mon.unref();
for (let i = 0; !(await healthz()) && i < 30; i++) await sleep(500);
if (!(await healthz())) { console.error("FAIL: 服务未起"); process.exit(1); }
const hz = await get("/healthz");
console.log(`段1 服务就绪 (${((Date.now() - t0) / 1000).toFixed(1)}s)，healthz=${hz.body}`);
if (!hz.body.includes('"idle_s"')) { console.error("FAIL: healthz 未携带 idle_s"); ok = false; }
if (!monLogSince(log0).includes(`gate=${STUB_NAMES.split(",").join("/")}`)) { console.error("FAIL: [START] 未带上硬门名单（gate=...）"); ok = false; }

let keepAliveOk = true;
for (let i = 0; i < 25; i++) { // 每秒一次 /turns，模拟 inject 取数
  await get("/turns?limit=5");
  await sleep(1000);
  if (!(await healthz()) || !(await portUp(MPORT))) { keepAliveOk = false; break; }
}
console.log(`段1 持续取数 25s 后: service=${await healthz()} monitor=${await portUp(MPORT)}（阈值 15s，应双双在岗）`);
if (!keepAliveOk) { console.error("FAIL: 活跃取数期间误退场"); ok = false; }

// ---- 段2：停止取数但ZCode 进程在运行（stub 存活）→ 硬门拦下，不得退场
startStub();
let gateOk = true;
for (let i = 0; i < 40; i++) { // 停止取数 40s（15s 阈值 × 2 轮确认 + 余量），远超退场窗口
  await sleep(1000);
  if (!(await healthz()) || !(await portUp(MPORT))) { gateOk = false; break; }
}
const gateLog = monLogSince(log0);
console.log(`段2 停止取数且 ZCode 在运行 40s 后: service=${await healthz()} monitor=${await portUp(MPORT)}（硬门应拦下，双双在岗）`);
if (!gateOk) { console.error("FAIL: ZCode 在运行时仍然退场（硬门未生效）"); ok = false; }
if (!gateLog.includes("[IDLE-GATE]")) { console.error("FAIL: monitor.log 无 [IDLE-GATE] 记录"); ok = false; }
if (gateLog.includes("[IDLE-EXIT]")) { console.error("FAIL: ZCode 在运行时出现 [IDLE-EXIT]"); ok = false; }

// ---- 段3：杀掉假 ZCode（模拟 ZCode 关闭）→ 闲置退场
stopStub();
let exited = false;
let waited = 0;
for (let i = 0; i < 60; i++) {
  await sleep(1000);
  waited += 1;
  if (await allDown()) { exited = true; break; }
}
console.log(`段3 假 ZCode 退出后 ${waited}s: service=${await healthz()} monitor=${await portUp(MPORT)}（应双双退净，耗时 ${((Date.now() - t0) / 1000).toFixed(1)}s）`);
if (!exited) { console.error("FAIL: ZCode 退出后闲置退场未发生（60s 内 3199/3200 未清空）"); ok = false; }

// ---- 段4：重开 ZCode（supervisor MCP 连接）→ 守护链恢复
if (exited) {
  const sup = spawn(process.execPath, [path.join(REPO_ROOT, "mcp", "supervisor.mjs")], {
    env: { ...ENV, TPS_PLUGIN_ROOT: REPO_ROOT, TPS_IDLE_EXIT_S: "15" }, // 阈值/名单继承：测试收尾时不留 20 分钟残留
    stdio: ["pipe", "ignore", "inherit"],
  });
  sup.stdin.write(JSON.stringify({ jsonrpc: "2.0", id: 1, method: "initialize", params: { protocolVersion: "2025-11-25", capabilities: {}, clientInfo: { name: "t", version: "0" } } }) + "\n");
  let revived = false;
  let reviveS = 0;
  for (let i = 0; i < 20; i++) {
    await sleep(1000);
    reviveS += 1;
    if ((await healthz()) && (await portUp(MPORT))) { revived = true; break; }
  }
  console.log(`段4 supervisor 补拉 ${reviveS}s: service=${await healthz()} monitor=${await portUp(MPORT)}（守护链应恢复）`);
  sup.stdin.end();
  if (!revived) { console.error("FAIL: supervisor 未恢复守护链"); ok = false; }
  else {
    // 恢复的监视器继承了 15s 阈值且 stub 已死，等它自然退净（最多 ~40s），不给后续留残留
    for (let i = 0; i < 60 && !(await allDown()); i++) await sleep(1000);
    console.log(`段4 收尾自清: service=${await healthz()} monitor=${await portUp(MPORT)}（应退净）`);
  }
}

console.log(ok ? "IDLE-EXIT-TEST-PASS" : "IDLE-EXIT-TEST-FAIL");
process.exit(ok ? 0 : 1);
