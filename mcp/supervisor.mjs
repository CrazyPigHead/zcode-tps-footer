#!/usr/bin/env node
/** TPS 统计服务的 MCP 监督者：零工具 stdio MCP server + 单例守护。
 *
 * 以普通用户级 MCP server 注册在 ~/.zcode/cli/config.json → mcp.servers（安装脚本写入，
 * 注册项只需 command + args，本进程按自身位置定位仓库根）。仓库即安装：**仓库目录须持久保留**。
 * 「ZCode 每个会话建立 MCP 连接」就是拉起时机（应用启动与会话创建/恢复都会连）：
 *   1. 立即完成 MCP 握手（零工具——本 server 只当进程锚点，不给 agent 暴露工具）；
 *   2. 探 http://127.0.0.1:3117/healthz（端口与数据面同源，测试可经 TPS_FOOTER_PORT
 *      一并覆盖；用 node:http 裸请求，天然无视代理环境变量），
 *      不通则分离拉起数据面 server.mjs（detached + stdio ignore + unref：
 *      本进程被杀/退出都不牵连服务；每次连接可能出现的预热双 spawn 亦无碍，
 *      端口 bind 竞态由数据面自行仲裁，输家退出）；
 *   3. 常驻看门狗：healthz 连续 2 次失败 → 重拉（数据面常驻不主动退出，
 *      死了基本就是崩溃）；连续重拉失败按 1min→2min→4min… 指数退避（上限 30min），
 *      healthz 一旦恢复即清零，避免端口被占/环境损坏时无限每 60s 重拉。
 *      stdin（MCP 通道）关闭即随会话退出。
 *
 * 环境变量（可选，仅测试/特殊布局用；正常注册项不需要）：
 *   TPS_PLUGIN_ROOT  仓库根目录（默认取本脚本位置的上一级）
 * 运行时文件：supervisor.log 在仓库 logs/ 目录。
 */
import { spawn } from "node:child_process";
import fs from "node:fs";
import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const PLUGIN_ROOT = process.env.TPS_PLUGIN_ROOT || path.dirname(HERE);
const SERVER = path.join(PLUGIN_ROOT, "server", "server.mjs");
const LOG_PATH = path.join(PLUGIN_ROOT, "logs", "supervisor.log");
const VERSION = "0.3.0";
// 与数据面 server.mjs 同源：TPS_FOOTER_PORT 可覆盖（测试用），默认 3117。
// 不对齐的话，监督者探活 3117、服务起在别的端口，看门狗会永远探不到自己拉起的服务。
const PORT = Number(process.env.TPS_FOOTER_PORT) || 3117;
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

function spawnService() {
  const child = spawn(process.execPath, [SERVER], {
    detached: true, // 新进程组：本进程退出/被杀不牵连服务
    stdio: "ignore",
    windowsHide: true,
    cwd: PLUGIN_ROOT, // 数据面按自身位置定位仓库根，cwd 仅约定俗成
  });
  child.unref();
  log(`[SPAWN] service pid=${child.pid} node=${process.execPath}`);
}

async function ensureService() {
  if (await healthz()) {
    log("[UP] service already running");
    return true;
  }
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
    failStreak += 1;
    // 退避：第 1 次重拉后照旧等一个探活周期，连续失败则逐次翻倍（60s→2min→…→30min 封顶），
    // 端口被占、环境损坏等不可自愈场景下把重拉频率压到无害水平
    const backoffMs = Math.min(WATCHDOG_S * 1000 * 2 ** (failStreak - 1), RESPAWN_BACKOFF_MAX_MS);
    log(`[WATCHDOG] healthz 连续 ${WATCHDOG_MISSES} 次失败，重拉服务（连续第 ${failStreak} 次，此后退避 ${Math.round(backoffMs / 1000)}s；若持续失败请查 Node 版本与 3117 端口占用）`);
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
