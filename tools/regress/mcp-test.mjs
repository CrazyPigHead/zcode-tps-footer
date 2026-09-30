// supervisor.mjs 独立验收：握手 → tools/list（空）→ 服务已拉起 → 关 stdin 监督者退出 → 数据面仍存活
import { spawn } from "node:child_process";
import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";

// 仓库根 = 本脚本目录的上两级（tools/regress/ → 仓库根），与 cwd 无关
const REPO_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");

const healthz = () =>
  new Promise((resolve) => {
    const req = http.get({ host: "127.0.0.1", port: 3117, path: "/healthz", timeout: 800 }, (res) => {
      res.resume();
      resolve(res.statusCode === 200);
    });
    req.on("timeout", () => { req.destroy(); resolve(false); });
    req.on("error", () => resolve(false));
  });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const wasUp = await healthz();
if (wasUp) { console.error("3117 已被占用，先清场再测"); process.exit(1); }

const child = spawn(process.execPath, [path.join(REPO_ROOT, "mcp/supervisor.mjs")], {
  env: { ...process.env, TPS_PLUGIN_ROOT: REPO_ROOT },
  stdio: ["pipe", "pipe", "inherit"],
});

let out = "";
child.stdout.setEncoding("utf8");
child.stdout.on("data", (c) => (out += c));

const send = (obj) => child.stdin.write(JSON.stringify(obj) + "\n");
send({ jsonrpc: "2.0", id: 1, method: "initialize", params: { protocolVersion: "2025-11-25", capabilities: {}, clientInfo: { name: "test", version: "0" } } });
send({ jsonrpc: "2.0", method: "notifications/initialized" });
send({ jsonrpc: "2.0", id: 2, method: "tools/list" });

// 等握手回包 + 服务就绪（监督者异步拉起，最多 15s）
const t0 = Date.now();
while (Date.now() - t0 < 15000) {
  await sleep(400);
  const replies = out.trim().split("\n").filter(Boolean);
  if (replies.length >= 2 && (await healthz())) break;
}

const lines = out.trim().split("\n").filter(Boolean);
console.log("== MCP replies ==");
lines.forEach((l) => console.log(l.slice(0, 220)));

let ok = true;
const init = lines.map((l) => { try { return JSON.parse(l); } catch { return null; } }).find((m) => m && m.id === 1);
const tools = lines.map((l) => { try { return JSON.parse(l); } catch { return null; } }).find((m) => m && m.id === 2);
if (!init || !init.result || init.result.serverInfo.name !== "zcode-tps-footer") { console.error("FAIL: initialize 回包异常"); ok = false; }
if (!tools || !tools.result || !Array.isArray(tools.result.tools) || tools.result.tools.length !== 0) { console.error("FAIL: tools/list 应为空数组"); ok = false; }
const svcUp = await healthz();
if (!svcUp) { console.error("FAIL: 服务未被拉起"); ok = false; } else console.log("== service up ==");

// 关闭 stdin：监督者应退出，数据面应继续存活（detached 语义）
child.stdin.end();
const exited = await new Promise((resolve) => { const t = setTimeout(() => resolve(false), 5000); child.on("exit", () => { clearTimeout(t); resolve(true); }); });
console.log(`supervisor exited=${exited} code=${child.exitCode}`);
await sleep(1500);
const stillUp = await healthz();
console.log(`service alive after supervisor exit: ${stillUp}`);
if (!exited) { console.error("FAIL: 监督者未随 stdin 关闭退出"); ok = false; }
if (!stillUp) { console.error("FAIL: 监督者退出把数据面带走了"); ok = false; }

console.log(ok && exited && stillUp ? "SUPERVISOR-TEST-PASS" : "SUPERVISOR-TEST-FAIL");
process.exit(ok && exited && stillUp ? 0 : 1);
