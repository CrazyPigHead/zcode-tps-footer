#!/usr/bin/env node
/** TPS 统计服务的 MCP 监督者：零工具 stdio MCP server + 单例守护。
 *
 * 以普通用户级 MCP server 注册在 ~/.zcode/cli/config.json → mcp.servers（安装脚本写入，
 * 注册项只需 command + args，本进程按自身位置定位仓库根）。仓库即安装：**仓库目录须持久保留**。
 * 「ZCode 每个会话建立 MCP 连接」就是拉起时机（应用启动与会话创建/恢复都会连）：
 *   1. 立即完成 MCP 握手（零工具——本 server 只当进程锚点，不给 agent 暴露工具）；
 *   2. 探 http://127.0.0.1:3117/healthz 与监视器仲裁端口 3118（端口与数据面同源，
 *      测试可经 TPS_FOOTER_PORT/TPS_MONITOR_PORT 一并覆盖；用 node:http 裸请求，
 *      天然无视代理环境变量），服务或监视器不在岗则分离拉起常驻监视器
 *      monitor.mjs（detached + stdio ignore + unref：本进程被杀/退出都不牵连监视器；
 *      每次连接可能出现的预热双 spawn 亦无碍，监视器按仲裁端口自行去重）。
 *      监视器拥有数据服务子进程：捕获崩溃退出码与 stderr 落 logs/monitor.log，
 *      并在 healthz 失联时秒级重拉——守护不再依赖 MCP 连接存活（2026-10-08 实测
 *      纯远程工作时段服务死透过 8 分钟~2 小时无人拉）。数据面闲置超阈值时监视器
 *      回收服务并自行退场（「ZCode 关闭后后台退干净」），重新打开 ZCode 时这里
 *      对 3118 的补拉就是守护链恢复的入口；
 *   3. 常驻看门狗（双保险）：healthz 连续 2 次失败 → 再拉一只监视器（老监视器
 *      在岗时它会自行退出，新监视器接管服务）；连续失败按 1min→2min→4min… 指数
 *      退避（上限 30min），healthz 一旦恢复即清零。stdin（MCP 通道）关闭即随会话退出。
 *
 * 环境变量（可选，仅测试/特殊布局用；正常注册项不需要）：
 *   TPS_PLUGIN_ROOT  仓库根（默认取本脚本位置的上一级）
 *   TPS_MONITOR_PORT 监视器仲裁端口（默认数据端口+1，与监视器自身的默认一致）
 * 运行时文件：supervisor.log 在仓库 logs/ 目录。
 */
import { spawn } from "node:child_process";
import fs from "node:fs";
import http from "node:http";
import net from "node:net";
import path from "node:path";
import { fileURLToPath } from "node:url";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const PLUGIN_ROOT = process.env.TPS_PLUGIN_ROOT || path.dirname(HERE);
const MONITOR = path.join(PLUGIN_ROOT, "server", "monitor.mjs");
const LOG_PATH = path.join(PLUGIN_ROOT, "logs", "supervisor.log");
const STATE_PATH = path.join(PLUGIN_ROOT, "logs", "monitor.state"); // 监视器闲置退场标记（看门狗冷却用）
const VERSION = "0.3.0";
// 与数据面 server.mjs 同源：TPS_FOOTER_PORT 可覆盖（测试用），默认 3117。
// 不对齐的话，监督者探活 3117、服务起在别的端口，看门狗会永远探不到自己拉起的服务。
const PORT = Number(process.env.TPS_FOOTER_PORT) || 3117;
const MPORT = Number(process.env.TPS_MONITOR_PORT) || PORT + 1; // 监视器仲裁端口，同源覆盖
const READY_TIMEOUT_MS = 10_000;
const WATCHDOG_S = 60;
const WATCHDOG_MISSES = 2;
const RESPAWN_BACKOFF_MAX_MS = 30 * 60_000; // 连续重拉失败的退避上限

try {
  fs.mkdirSync(path.dirname(LOG_PATH), { recursive: true }); // logs 目录不存在则建（建不了就没日志，不影响功能）
} catch {
  /* ignore */
}

// 日志写仓库 logs/ 目录（诊断拉起问题用）；stdout 绝不碰——那是 MCP 通道
function log(msg) {
  const line = `${new Date().toISOString()} [supervisor] ${msg}\n`;
  try {
    fs.appendFileSync(LOG_PATH, line, "utf8");
  } catch {
    /* ignore */
  }
  process.stderr.write(line);
}

const sleep = (ms) => new Promise((res) => setTimeout(res, ms));

function healthz(timeoutMs = 800) {
  return new Promise((resolve) => {
    const req = http.get({ host: "127.0.0.1", port: PORT, path: "/healthz", timeout: timeoutMs }, (res) => {
      res.resume();
      resolve(res.statusCode === 200);
    });
    req.on("timeout", () => {
      req.destroy();
      resolve(false);
    });
    req.on("error", () => resolve(false));
  });
}

// 监视器在岗判定：它 bind 着仲裁端口（静默监听，不回数据）。connect 成功即在岗；
// ECONNREFUSED = 不在岗（闲置退场/被杀/从未启动）。别的错误码（防火墙等罕见情形）
// 一律按在岗处理——补拉是幂等的（监视器自行去重），宁可多拉不可漏拉。
function monitorAlive(timeoutMs = 800) {
  return new Promise((resolve) => {
    const sock = net.connect({ host: "127.0.0.1", port: MPORT, timeout: timeoutMs });
    sock.on("connect", () => {
      sock.destroy();
      resolve(true);
    });
    sock.on("timeout", () => {
      sock.destroy();
      resolve(true);
    });
    sock.on("error", (e) => {
      resolve(e && e.code === "ECONNREFUSED" ? false : true);
    });
  });
}

function spawnService() {
  // 拉起的是常驻监视器而非数据面本身：监视器拥有数据服务子进程（崩溃捕获 + 秒级
  // 重拉），且与 MCP 连接生命周期解耦。多拉无害：监视器按仲裁端口去重，数据面
  // 再按数据端口 bind 竞争去重，两层单例都由输家静默退场保证。
  const child = spawn(process.execPath, [MONITOR], {
    detached: true, // 新进程组：本进程退出/被杀不牵连监视器
    stdio: "ignore", // 监视器自带文件日志（monitor.log），stdout 无输出
    windowsHide: true,
    cwd: PLUGIN_ROOT, // 监视器按自身位置定位仓库根，cwd 仅约定俗成
  });
  child.unref();
  log(`[SPAWN] monitor pid=${child.pid} node=${process.execPath}`);
}

async function ensureService() {
  const svcUp = await healthz();
  const monUp = await monitorAlive();
  if (svcUp && monUp) {
    log("[UP] service & monitor already running");
    return true;
  }
  // 服务在而监视器不在（监视器闲置退场后服务被外部拉起、或监视器被单独杀掉）：
  // 补拉监视器，守护链不能缺环。监视器冷启动发现服务已在（healthz 通）只守护
  // 不重拉，行为不变。
  log(`[SPAWN-CHECK] service=${svcUp ? "up" : "down"} monitor=${monUp ? "up" : "down"}${svcUp && !monUp ? "（补拉监视器）" : ""}`);
  spawnService();
  const deadline = Date.now() + READY_TIMEOUT_MS;
  while (Date.now() < deadline) {
    await sleep(500);
    if (await healthz()) {
      log("[READY] service up");
      return true;
    }
  }
  log(`[TIMEOUT] service not ready in ${READY_TIMEOUT_MS}ms`);
  return false;
}

// 闲置退场冷却：监视器闲置退场时写 monitor.state（last_idle_exit + idle_exit_s）。
// 本会话若活着但不取数（典型：闲置的 CLI 会话——有 MCP 连接、没有渲染层 inject），
// 看门狗会在 2 分钟内把刚退场的守护链拉回来，20 分钟后又被退场——无限拉锯。读到
// 冷却窗口内的标记就跳过补拉；用户真开始用时新 MCP 连接的 ensureService（每次连接
// 都跑，不受此冷却限制）自然恢复守护链。真崩溃场景不写标记，不受影响。
function idleExitCooling() {
  try {
    const j = JSON.parse(fs.readFileSync(STATE_PATH, "utf8"));
    if (!j || typeof j.last_idle_exit !== "number" || typeof j.idle_exit_s !== "number") return false;
    const cooldownMs = Math.max(j.idle_exit_s, 600) * 2 * 1000; // 2× 阈值，给真故障留恢复窗
    return Date.now() - j.last_idle_exit < cooldownMs;
  } catch {
    return false;
  }
}

async function watchdogLoop() {
  let misses = 0;
  let failStreak = 0; // 连续「重拉后仍未恢复」次数；healthz 恢复即清零
  for (;;) {
    await sleep(WATCHDOG_S * 1000);
    if (await healthz()) {
      if (failStreak) log(`[WATCHDOG] 服务已恢复（此前连续 ${failStreak} 次重拉失败）`);
      misses = 0;
      failStreak = 0;
      continue;
    }
    misses += 1;
    if (misses < WATCHDOG_MISSES) continue;
    misses = 0;
    if (idleExitCooling()) {
      // 闲置退场冷却窗内：服务「失联」正是预期状态，不补拉（防拉锯循环，见函数注释）
      failStreak = 0;
      continue;
    }
    failStreak += 1;
    // 退避：第 1 次重拉后照旧等一个探活周期，连续失败则逐次翻倍（60s→2min→…→30min 封顶），
    // 端口被占、环境损坏等不可自愈场景下把重拉频率压到无害水平
    const backoffMs = Math.min(WATCHDOG_S * 1000 * 2 ** (failStreak - 1), RESPAWN_BACKOFF_MAX_MS);
    log(`[WATCHDOG] healthz 连续 ${WATCHDOG_MISSES} 次失败，重拉监视器（连续第 ${failStreak} 次，此后退避 ${Math.round(backoffMs / 1000)}s；若持续失败请查 Node 版本与 3117 端口占用）`);
    spawnService();
    await sleep(backoffMs);
  }
}

// ---------------------------------------------------------------- 零工具 stdio MCP server（行分隔 JSON-RPC）

function reply(obj) {
  process.stdout.write(`${JSON.stringify(obj)}\n`);
}

function handleMessage(msg) {
  const { id, method, params } = msg;
  if (method === "initialize") {
    return reply({
      jsonrpc: "2.0",
      id,
      result: {
        protocolVersion: params && params.protocolVersion ? params.protocolVersion : "2025-11-25",
        capabilities: {},
        serverInfo: { name: "zcode-tps-footer", version: VERSION },
      },
    });
  }
  if (typeof method === "string" && method.startsWith("notifications/")) return; // 通知不回包
  if (id === undefined) return; // 其余无 id 的消息一律不回
  if (method === "tools/list") return reply({ jsonrpc: "2.0", id, result: { tools: [] } });
  if (method === "ping") return reply({ jsonrpc: "2.0", id, result: {} });
  if (method === "resources/list") return reply({ jsonrpc: "2.0", id, result: { resources: [] } });
  if (method === "prompts/list") return reply({ jsonrpc: "2.0", id, result: { prompts: [] } });
  return reply({ jsonrpc: "2.0", id, error: { code: -32601, message: `Method not found: ${method}` } });
}

let buf = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => {
  buf += chunk;
  let idx;
  while ((idx = buf.indexOf("\n")) >= 0) {
    const line = buf.slice(0, idx).trim();
    buf = buf.slice(idx + 1);
    if (!line) continue;
    try {
      handleMessage(JSON.parse(line));
    } catch {
      /* 坏行丢弃 */
    }
  }
});
process.stdin.on("end", () => {
  log("[EXIT] stdin closed（会话结束，监督者退出；数据面服务继续常驻）");
  process.exit(0);
});
process.stdin.on("error", () => process.exit(0));

// ---------------------------------------------------------------- 启动：握手不等待服务，守护并行进行

// node:sqlite 自检：数据面 server.mjs 依赖它（Node ≥ 23.4）。缺失时数据面在 listen 之前
// 就崩溃且不留任何日志，看门狗若不知情会永久重拉。本进程与数据面用同一个 node 可执行文件
//（spawn 用 process.execPath），此处探测结果即数据面的运行环境。
const SQLITE_OK = await import("node:sqlite").then(
  () => true,
  (e) => {
    log(`[FATAL] 本 Node（${process.version}，${process.execPath}）不支持 node:sqlite，数据面无法运行（需 Node ≥ 23.4）。本次连接内不再拉起/看门狗；请升级 Node 后重开 ZCode 会话。原因：${e.message}`);
    return false;
  }
);

if (SQLITE_OK) {
  ensureService();
  watchdogLoop();
}
