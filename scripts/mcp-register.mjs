#!/usr/bin/env node
// 用户级 MCP server 注册（install.ps1 / uninstall.ps1 共用）。
// 直接读写 ~/.zcode/cli/config.json → mcp.servers，只增删本项目的条目，
// 其余内容（其他 MCP server、设置项）原样保留。
//
// 用法：
//   node mcp-register.mjs add <repoRoot>   注册/更新 zcode-tps-footer
//   node mcp-register.mjs remove           移除 zcode-tps-footer
//
// 仓库即安装：注册项只指向 <repoRoot>/mcp/supervisor.mjs，无环境变量；
// 监督者按自身位置定位仓库根，仓库目录须持久保留。
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const NAME = "zcode-tps-footer";
const configPath = path.join(os.homedir(), ".zcode", "cli", "config.json");

// 读配置：文件不存在才允许从空配置开始。读取/解析失败（ZCode 正在重写该文件的半写窗口、
// Windows EBUSY/EPERM 等）必须中止——拿空配置覆写会把用户全部 CLI 设置和其他 MCP 注册抹掉。
function loadConfig() {
  let text;
  try {
    text = fs.readFileSync(configPath, "utf8");
  } catch (e) {
    if (e.code === "ENOENT") return {};
    console.error(`❌ 读取 ${configPath} 失败（${e.message}）。中止以免覆写用户配置，请稍后重试。`);
    process.exit(1);
  }
  let parsed;
  try {
    parsed = JSON.parse(text);
  } catch (e) {
    console.error(`❌ 解析 ${configPath} 失败（${e.message}）。中止以免覆写用户配置，请手工修复该文件后重试。`);
    process.exit(1);
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    console.error(`❌ ${configPath} 顶层不是 JSON 对象。中止以免覆写用户配置，请手工修复该文件后重试。`);
    process.exit(1);
  }
  return parsed;
}

function saveConfig(cfg) {
  fs.mkdirSync(path.dirname(configPath), { recursive: true });
  try {
    fs.copyFileSync(configPath, `${configPath}.bak`); // 覆写前留上一份，写坏可捞
  } catch {
    /* 首次安装无原文件，无需备份 */
  }
  fs.writeFileSync(configPath, `${JSON.stringify(cfg, null, 2)}\n`, "utf8");
}

// loadConfig 在各命令分支内调用：配置异常时无参数调用仍应先报用法错，而不是配置错
const cmd = process.argv[2];
if (cmd === "add") {
  const cfg = loadConfig();
  const root = path.resolve(process.argv[3]);
  cfg.mcp = cfg.mcp || {};
  cfg.mcp.servers = cfg.mcp.servers || {};
  cfg.mcp.servers[NAME] = {
    command: "node",
    args: [path.join(root, "mcp", "supervisor.mjs")],
  };
  saveConfig(cfg);
  console.log(`✅ 已注册用户级 MCP server：${NAME} → ${configPath}`);
} else if (cmd === "remove") {
  const cfg = loadConfig();
  let touched = false;
  if (cfg.mcp?.servers?.[NAME]) {
    delete cfg.mcp.servers[NAME];
    if (Object.keys(cfg.mcp.servers).length === 0) delete cfg.mcp.servers;
    if (cfg.mcp && Object.keys(cfg.mcp).length === 0) delete cfg.mcp;
    touched = true;
  }
  if (touched) saveConfig(cfg);
  console.log(touched ? `✅ 已从 ${configPath} 移除 ${NAME}` : `ℹ️ ${configPath} 里没有 ${NAME} 的注册项，无需移除`);
} else {
  console.error("用法：node mcp-register.mjs add <repoRoot> | remove");
  process.exit(1);
}
