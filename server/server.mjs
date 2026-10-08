#!/usr/bin/env node
/** TPS 统计服务（跨平台 Node 版）：本地库 + 远端 SSH 库合并，供渲染层注入脚本取数。
 *
 * 由 MCP 监督者（../mcp/supervisor.mjs）按需拉起，单例常驻：
 *   - 端口被占（bind 失败）即退出 = 并发拉起的输家静默退场；
 *   - 常驻不退出；/turns 闲置超 10 分钟挂起远端 SSH 轮询（省掉空转连接），
 *     任何请求即刻恢复；崩溃由常驻监视器（monitor.mjs，监督者拉起、与 MCP 连接
 *     生命周期解耦）捕获退出码与 stderr 尾部后秒级重拉——2026-10 实测服务会在
 *     远端活跃期无声退出且不留任何痕迹（stdio 被父进程 ignore），必须先抓到现场。
 *
 * Remote SSH 模式下 ZCode 的 CLI 跑在远程机器上，用量写在远端 ~/.zcode/cli/db/db.sqlite，
 * 本地没有任何副本。因此本服务：
 *   - 本地侧：node:sqlite 只读 ~/.zcode/cli/db/db.sqlite（口径=DeepSeek Harness）；
 *   - 远端侧：脚本按内容 hash 缓存在远端 ~/.cache/zcode-tps-footer/，常态每轮只发一条
 *     「执行缓存脚本」的短命令（缓存写不进自动退回 stdin 直送整脚本），只回传折叠后的
 *     JSON（远端库可能几百 MB，绝不整库拉取），且已结算轮按 since 水位增量回传；
 *     每条远端 turn 打上 host 标签。
 *
 * 远端列表自动解析自 ZCode 桌面端设置（~/.zcode/v2/setting.json 的最近 SSH 会话，
 * 打开过新远端会自动出现）。各远端独立并行轮询：单台失败只退避它自己，绝不阻塞其他远端。
 *
 * 端点（live 条目另带 phase/gen_start_ms/gap_start_ms/steps_settled 步内实时字段；
 * 全部条目带 query_source/ctx_tokens/ctx_window——子会话上下文水位三件套，见下方
 * 目录解析与 foldLocal 注释，inject.js 只对 subagent/workflow_child 追加「上下文 K (百分比)」）：
 *   GET /healthz          → {"ok":true,"pid":pid,"idle_s":sec,"suspended":bool}
 *                            （idle_s = 距最近一次 /turns 请求的秒数，仅该端点计入
 *                             ——渲染层在世时 inject 每秒取数，idle_s 持续归零；长期
 *                             无请求 = ZCode 已关闭，监视器据此做闲置退场）
 *   GET /turns?limit=800[&since=ms] → {"turns":[...],"since":ms,"full":bool,"tps":bool}
 *                            （本地+全部远端合并，按 end_ms 降序；带 since 走增量：
 *                             live 全带 + 水位-边距内已结算轮；tps = 注入开关，见 /tps-state）
 *   GET /status           → 各数据源健康状态（排障用）
 *   GET /ping?secs=&snap= → 注入脚本诊断上报，仅落日志
 *   GET/POST /tps-state[?enabled=0|1] → {"enabled":bool}
 *     渲染层统计行的实机 A/B 开关：POST enabled=0 → inject.js 下一次取数（≤2s）即撤掉
 *     全部统计行，enabled=1 恢复，无需重启 ZCode。闪烁排查的对照实验靠它做：
 *     关掉后仍闪 = 应用原生行为，与注入无关。持久化 logs/tps-state.json，服务重启
 *     保持；文件缺失/损坏 = 默认开。
 *
 * 运行时文件：日志在仓库 logs/ 目录（按本脚本位置自动定位；仓库目录须持久保留）。
 * 不依赖任何环境变量。
 */
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { createServer } from "node:http";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { DatabaseSync } from "node:sqlite";

const HERE = path.dirname(fileURLToPath(import.meta.url)); // <仓库>/server
const ROOT = path.dirname(HERE); // 仓库根 = 安装根
const DB = path.join(os.homedir(), ".zcode", "cli", "db", "db.sqlite");
const REMOTE_FOLD = path.join(HERE, "remote_fold.py");
const LOG_PATH = path.join(ROOT, "logs", "server.log");
const LOG_KEEP_DAYS = 14;

// 测试可用 TPS_FOOTER_PORT 覆盖（正常安装固定 3117；监督者探活同用此变量，两者必须一致）
const PORT = Number(process.env.TPS_FOOTER_PORT) || 3117;
const CACHE_TTL_S = 0.5; // 本地库折叠缓存（含 live 轮，步级实时性靠它）
const LIVE_STALE_MS = 10 * 60 * 1000; // live 判定①：最近一步落库距今（覆盖长流式步）
const LIVE_TOOL_WINDOW_MS = 60 * 60 * 1000; // live 判定②：该轮仍有新近 running 的工具行（覆盖超长工具）
const FAST_POLL_S = 2.5; // 远端拉取间隔（渲染层活跃 且 该远端有 live 轮——只有这时才可能有秒级新数据）
const IDLE_REMOTE_POLL_S = 10.0; // 渲染层活跃但该远端无 live 轮：已结算轮不可变，慢节奏探新轮（新轮首现最多迟一个间隔）
const SLOW_POLL_S = 20.0; // 渲染层不活跃时的拉取间隔
const IDLE_AFTER_S = 60.0;
const SUSPEND_AFTER_S = 600; // /turns 闲置超此值 → 完全挂起远端 SSH 轮询（任何请求即恢复）
const SSH_TIMEOUT_S = 30;
const MAX_BACKOFF = 12; // 远端连续失败时的退避倍数上限
const INC_MARGIN_MS = 5 * 60 * 1000; // since 增量安全边距（与 remote_fold.py 的 INC_MARGIN_MS 同值）
const FULL_SYNC_MS = 10 * 60 * 1000; // 远端周期性全量自愈：平时增量，每过此刻数强拉一次全量
const EVICT_MS = 26 * 60 * 60 * 1000; // 远端按 24h 窗折叠，本地合并表比窗多留 2h 再逐出
const LIVE_FREEZE_MS = 5 * 60 * 1000; // live 冻结判定：live 条目的 end_ms 每次发射都刷新为
// 远端折叠时刻，健康轮询下秒级刷新（含降频/退避间隙，失败轮询不进逐出路径），超此未
// 刷新 = 远端已停发该条（轮次结束/崩溃残留被活性判定淘汰），及时逐出防僵尸「运行中」行

const processStartMs = Date.now();
fs.mkdirSync(path.dirname(LOG_PATH), { recursive: true });

// ---------------------------------------------------------------- 日志（单线程事件循环，无需锁）

function ts() {
  const d = new Date();
  const p = (n, w = 2) => String(n).padStart(w, "0");
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}

function log(msg) {
  try {
    fs.appendFileSync(LOG_PATH, `${ts()} ${msg}\n`, "utf8");
  } catch {
    /* 日志失败不致命 */
  }
}

function cleanupLog() {
  try {
    if (!fs.existsSync(LOG_PATH)) return;
    const lines = fs.readFileSync(LOG_PATH, "utf8").split("\n");
    const cut = Date.now() - LOG_KEEP_DAYS * 24 * 3600 * 1000;
    const kept = lines.filter((ln) => {
      if (!ln) return true; // 保留末尾空行结构
      const m = ln.match(/^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})/);
      if (!m) return true; // 无时间戳行保留（保守）
      return Date.parse(m[1].replace(" ", "T")) >= cut - 24 * 3600 * 1000; // 本地时区近似
    });
    if (kept.length !== lines.length) fs.writeFileSync(LOG_PATH, kept.join("\n"), "utf8");
  } catch (e) {
    log(`[LOG-CLEANUP-ERR] ${e && e.message}`);
  }
}

// ---------------------------------------------------------------- 进程死亡可观测
//
// 2026-10-08 排查：服务多次在远端活跃期退出且不留任何日志（父进程 stdio:ignore，
// stderr 上的 V8 致命错误/未处理 rejection 全部进了黑洞，WER 也无记录）。以下三个
// 钩子保证「死前最后一句话」落在 server.log；配合 monitor.mjs 捕获的退出码/stderr
// 尾部，下次复发即可定位。uncaught/unhandledRejection 记录后不退出：单例数据服务
// 全部状态可由下一个请求重建，带伤运行的代价远小于死掉等监督者（历史实测曾死透
// 8 分钟~2 小时无人重拉）。

function errBrief(e) {
  if (!e) return String(e);
  const stack = e.stack ? e.stack.split("\n").slice(0, 4).join(" | ") : "";
  return `${e.message || e}${stack ? ` @ ${stack}` : ""}`;
}

process.on("uncaughtException", (e) => log(`[UNCAUGHT] ${errBrief(e)}`));
process.on("unhandledRejection", (r) => log(`[UNHANDLED-REJECT] ${errBrief(r && r.stack ? r : String(r))}`));
process.on("exit", (code) => {
  // exit 回调里只允许同步操作；bind 竞争输家（[FATAL]→exit(1)）此前连一行痕迹都不留
  try {
    fs.appendFileSync(LOG_PATH, `${ts()} [EXIT] code=${code}\n`, "utf8");
  } catch {
    /* 日志失败不致命 */
  }
});

// ---------------------------------------------------------------- 远端配置与状态

// 远端来源：ZCode 桌面端设置（最近会话里的 SSH 目标，打开过新远端会自动出现）
const SETTING_PATH = path.join(os.homedir(), ".zcode", "v2", "setting.json");

function makeRemote(def) {
  return {
    name: def.name, host: def.host, port: def.port, src: def.src,
    turns: [], lastOk: 0, lastErr: "", consecFail: 0,
    inFlight: false, nextPollAt: 0, // 每台独立调度：并行拉取，失败只退避自己
    noCache: false, // 远端缓存目录写不进（只读/同名抢注）时置位：本进程内直接走 stdin 模式
    map: new Map(), // turn_id(或 m:msg_id) -> turn 增量合并表；turns 是它的降序物化
    since: 0, // 增量水位（0 = 下次拉全量：首拉/失败后/周期自愈都会归零）
    lastFullAt: 0,
    polled: false, // 本进程内是否已完成首次拉取尝试（成败均可）：全量响应的完整门控
    clockOffset: null, // 本地-远端时钟偏移最新采样（live 条目的 end_ms 即远端折叠时刻），
    //  供 live 冻结逐出换算时钟域用；map 里的 live 条目只能来自含 live 的响应，判定时必有采样
  };
}

let remotes = [];
let cfgSig = null; // null=从未读过

// setting.json 贡献的远端；解析失败（应用可能正在重写）返回 null = 本轮跳过，绝不清空已有配置
function defsFromSetting() {
  try {
    const j = JSON.parse(fs.readFileSync(SETTING_PATH, "utf8"));
    const out = [];
    for (const e of j.lastWorkspaceSession || []) {
      const t = e && e.target;
      if (!t || e.kind !== "remote" || t.kind !== "ssh" || !t.host) continue;
      const host = t.username ? `${t.username}@${t.host}` : String(t.host);
      out.push({ name: String(t.sshConfigAlias || host), host, port: Number(t.port) || 22, src: "app" });
    }
    return out;
  } catch (e) {
    if (e.code !== "ENOENT") log(`[CONFIG-ERR] setting.json: ${e && e.message}`);
    return null;
  }
}

function cfgFileSig() {
  try { return String(fs.statSync(SETTING_PATH).mtimeMs); } catch { return "-"; }
}

const defsKey = (d) => `${d.host}:${d.port}`;
const defsNorm = (defs) => JSON.stringify(defs.map(({ name, host, port }) => ({ name, host, port })));

function reloadConfigIfChanged() {
  const sig = cfgFileSig();
  if (sig === cfgSig) return;
  const defs = defsFromSetting();
  if (defs === null) return; // 不更新 sig，下一轮重试
  cfgSig = sig;
  // 同一台机器可能对应多条最近会话（不同工作目录），按 host:port 去重
  const seen = new Map();
  for (const d of defs) if (!seen.has(defsKey(d))) seen.set(defsKey(d), d);
  const uniq = [...seen.values()];
  if (defsNorm(uniq) === defsNorm(remotes)) return; // 派生结果没变（应用重写 setting.json 触碰 mtime），静默返回
  const old = new Map(remotes.map((r) => [defsKey(r), r]));
  remotes = uniq.map((d) => old.get(defsKey(d)) || makeRemote(d));
  log(`[CONFIG] remotes=[${uniq.map((d) => d.name).join(",") || "(none)"}]`);
}

// ---------------------------------------------------------------- 模型上下文窗口（ctx_window 分母）
//
// 窗口值不落用量库：官方在内存 registry 里按「个人覆盖层 → 内置目录正则规则」的顺序
// 叠加解析（packages/provider/src/config/model-config.ts 的 ModelConfigRules.resolve）。
// 这里按同一次序读盘重建，供上下文百分比用：
//   1) ~/.zcode/v2/provider_config.json 的 modelConfigRules.providerModelRules[]——精确
//      modelId（设置页改「上下文窗口」写的就是这里），后写覆盖先写；
//   2) 远端刷新后的内置目录（~/.zcode/v2/runtime/provider/<平台>/<版本>/endpoint-*/
//      zcode-builtin.json，取版本最高者）的 config.modelConfigRules.modelRules[]——
//      modelMatch 正则（/i）按序匹配 modelId，取最后一个带 contextWindow 的命中规则
//      （如 GLM-5.3-FlashX 不在任何模型列表里，依次命中 `.*`(200000)、`.*glm-5...`(200000)、
//      `.*glm-5\.3(?:-flash)?...`(1000000)，按最后命中得 1000000——last-match-wins 的意义
//      就在于此：目录追加更具体的新规则即生效，不会被开头 `.*` 的默认值永久遮蔽）。
// 两级都解析不到 → null：注入端只显示 K 不显示百分比（与官方 getRenderableTaskUsage
// 的 size>0 渲染门槛同语义）。文件按 mtime+size 签名缓存，30s 复查、变更重读；
// 覆盖层解析失败（应用重写文件撞上半写窗口）时沿用上一份，文件真被删除才清空。

const V2_DIR = path.join(os.homedir(), ".zcode", "v2");
const PERSONAL_PROVIDER_CONFIG = path.join(V2_DIR, "provider_config.json");
const RUNTIME_PROVIDER_DIR = path.join(V2_DIR, "runtime", "provider");
const CTXWIN_RECHECK_MS = 30 * 1000;

const ctxWin = { at: 0, sig: "", exact: new Map(), rules: [] };

const verKey = (v) => String(v).split(/[.\-_]/);
function cmpVer(a, b) {
  const ka = verKey(a);
  const kb = verKey(b);
  for (let i = 0; i < Math.max(ka.length, kb.length); i++) {
    const x = ka[i];
    const y = kb[i];
    if (x === undefined) return -1;
    if (y === undefined) return 1;
    const xn = Number(x);
    const yn = Number(y);
    if (!Number.isNaN(xn) && !Number.isNaN(yn) && xn !== yn) return xn > yn ? 1 : -1;
    const xs = String(x);
    const ys = String(y);
    if (xs !== ys) return xs > ys ? 1 : -1;
  }
  return 0;
}

// 内置目录 active 文件：runtime/provider/<平台>/<版本>/endpoint-*/zcode-builtin.json 里版本最高的
function activeBuiltinPath() {
  let best = null; // { ver, file }
  let plats = [];
  try {
    plats = fs.readdirSync(RUNTIME_PROVIDER_DIR);
  } catch {
    return null;
  }
  for (const plat of plats) {
    let versions = [];
    try {
      versions = fs.readdirSync(path.join(RUNTIME_PROVIDER_DIR, plat));
    } catch {
      continue;
    }
    for (const ver of versions) {
      let ends = [];
      try {
        ends = fs.readdirSync(path.join(RUNTIME_PROVIDER_DIR, plat, ver));
      } catch {
        continue;
      }
      for (const end of ends) {
        const file = path.join(RUNTIME_PROVIDER_DIR, plat, ver, end, "zcode-builtin.json");
        try {
          fs.accessSync(file);
        } catch {
          continue;
        }
        if (!best || cmpVer(ver, best.ver) > 0) best = { ver, file };
      }
    }
  }
  return best ? best.file : null;
}

function loadCtxWinCatalog() {
  const now = Date.now();
  if (now - ctxWin.at < CTXWIN_RECHECK_MS) return;
  ctxWin.at = now;
  const builtinFile = activeBuiltinPath();
  const sig = [PERSONAL_PROVIDER_CONFIG, builtinFile]
    .map((f) => {
      try {
        const s = fs.statSync(f);
        return `${s.mtimeMs}:${s.size}`;
      } catch {
        return "-";
      }
    })
    .join("|");
  if (sig === ctxWin.sig) return;
  ctxWin.sig = sig;
  // 覆盖层：精确 modelId -> contextWindow。半写窗口（应用原子重写覆盖层文件时恰被读中）
  // 解析失败且文件仍在 → 沿用上一份，等下次 mtime 变化重试；文件真被删除 → 清空。
  let overlayStatOk = false;
  let overlayParseOk = false;
  const exactNext = new Map();
  try {
    fs.statSync(PERSONAL_PROVIDER_CONFIG);
    overlayStatOk = true;
    const j = JSON.parse(fs.readFileSync(PERSONAL_PROVIDER_CONFIG, "utf8"));
    overlayParseOk = true;
    for (const r of j?.config?.modelConfigRules?.providerModelRules || []) {
      const win = r?.config?.properties?.contextWindow;
      if (typeof win === "number" && win > 0 && r.modelId) {
        exactNext.set(String(r.modelId).toLowerCase(), win);
      }
    }
  } catch {
    /* stat 失败 = 文件缺失；parse 失败 = 半写窗口，两者都走下面的选择逻辑 */
  }
  const exact = overlayParseOk ? exactNext : overlayStatOk ? ctxWin.exact : new Map();
  const rules = []; // 内置目录：保留目录出现顺序（后命中覆盖先命中）
  if (builtinFile) {
    try {
      const j = JSON.parse(fs.readFileSync(builtinFile, "utf8"));
      for (const r of j?.config?.modelConfigRules?.modelRules || []) {
        const win = r?.config?.properties?.contextWindow;
        if (typeof win !== "number" || win <= 0 || !r.modelMatch) continue;
        try {
          rules.push({ re: new RegExp(r.modelMatch, "i"), win });
        } catch {
          /* 非法正则跳过 */
        }
      }
    } catch (e) {
      log(`[CTXWIN-ERR] builtin: ${e && e.message}`);
    }
  }
  ctxWin.exact = exact;
  ctxWin.rules = rules;
  log(
    `[CTXWIN] exact=${exact.size} rules=${rules.length} builtin=${builtinFile ? path.basename(path.dirname(path.dirname(builtinFile))) : "-"}`,
  );
}

function resolveCtxWindow(modelId) {
  loadCtxWinCatalog();
  if (!modelId) return null;
  const hit = ctxWin.exact.get(String(modelId).toLowerCase());
  if (hit != null) return hit;
  let win = null;
  for (const r of ctxWin.rules) if (r.re.test(String(modelId))) win = r.win;
  return win;
}

// ---------------------------------------------------------------- 本地折叠（marker × settle × tool 三线合成）
//
// 每步模型请求的完整生命周期由三个落库源拼出（写入时机以 ZCode core 源码为准）：
//   marker  message ⋈ part(step-start)：请求发出前落库（step-start 全库唯一写入点在
//           runModelBackedTurnStep，compact summary/title sidecar 等旁路消息不带它），
//           定义「步的存在」与真实步起点；
//   settle  model_usage：流结束后才写终态行，按 assistant_message_id 关联 marker，
//           提供 token/ttft/时长；被 reactive compact 抛弃的步只有 marker 没有 settle；
//   tool    tool_usage：工具调度即写 running（含流中工具），结束写终态，提供「正在执行
//           的操作」。ModelRequest/ModelStreaming 事件只在内存 eventStore，不落库。
// 轮末 turn_usage.model_request_count 数的是 ModelRequest 事件 = marker 数 + 轮内
// compact 请求数（reactive compact 也向事件流追加 ModelRequest 但不写 marker），
// 因此 steps = marker 数 + compact 修正，与轮末账本严格守恒。

const SEGMENT_GAP_MS = 30 * 60 * 1000; // 相邻 marker 间隔超过此值切分段：隔离崩溃残留的半截轮

// 上下文水位（ctx_tokens）：官方口径 input+output，input 缺失按官方回退链退
// provider_total-output、再退 computed_total。见 foldLocalLocked 的 settle 注释。
function ctxTokensOfRow(r) {
  if (r.in_tok > 0) return r.in_tok + (r.out_tok || 0);
  if (r.prov_total != null && r.prov_total > 0) {
    const rest = r.prov_total - (r.out_tok || 0);
    return rest > 0 ? rest : r.prov_total;
  }
  return r.comp_total || 0;
}

// 持久只读连接：WAL 模式下读者不阻塞写者，长连接语义等价；此前每次折叠都
// open/close（活跃期每 0.5~2 秒一次），高频开关正在被 CLI 并发写的库是原生层
// 最可疑的崩溃源（2026-10-08 多次无声退出均发生在 /turns 密集期，详见进程死亡
// 可观测一节）。任何查询异常都弃置连接，下次折叠自动重开（库被替换/锁死等场景）。
let localConn = null;
function closeLocalConn() {
  try {
    if (localConn) localConn.close();
  } catch {
    /* close 失败不致命，连接对象一并丢弃 */
  }
  localConn = null;
}

function foldLocal() {
  const cut = Date.now() - 24 * 60 * 60 * 1000;
  if (!localConn) localConn = new DatabaseSync(DB, { readOnly: true });
  const conn = localConn;
  try {
    // 单只读事务：四张表读到同一 WAL 快照，杜绝「marker 可见、tool 未可见」的撕裂帧
    conn.exec("BEGIN DEFERRED");
    try {
      return foldLocalLocked(conn, cut);
    } finally {
      try {
        conn.exec("COMMIT");
      } catch {
        /* 读事务异常时丢弃连接（下方 catch 兜底重开） */
        closeLocalConn();
      }
    }
  } catch (e) {
    closeLocalConn(); // 连接可能已处坏状态，绝不带着坏连接进下一次折叠
    throw e;
  }
}

function foldLocalLocked(conn, cut) {
  const q = (sql, ...params) => conn.prepare(sql).all(...params);
  const nowMs = Date.now();
  // 规划器无统计信息（库从未 ANALYZE）时，带时间窗的 model_usage 查询会选择
  // query_source 单列索引 = 每次折叠近全表扫（行内含大 JSON blob，库大了很贵）。实测
  // INDEXED BY 强制走 started_at 复合索引结果完全一致；索引不存在（未来 schema 变更）
  // 则退回原 SQL，不报错。与 remote_fold.py 保持同步。
  const startedIdx = q(
    "SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = 'model_usage_started_model_idx'",
  ).length
    ? " INDEXED BY model_usage_started_model_idx"
    : "";

  // ---- 已结算轮 + 轮序号（口径不变）
  const turns = q(
    `SELECT session_id, turn_id, user_message_id, status,
            started_at, completed_at, time_to_first_token_ms, output_tokens,
            model_request_count
     FROM turn_usage
     WHERE started_at >= ? AND user_message_id IS NOT NULL AND user_message_id != ''
     ORDER BY started_at ASC`,
    cut,
  );
  const turnNo = new Map(); // `${sid}\u0000${tid}` -> 会话内序号
  for (const sid of new Set(turns.map((r) => r.session_id))) {
    q("SELECT started_at, turn_id FROM turn_usage WHERE session_id = ? ORDER BY started_at, turn_id", sid).forEach(
      (row, i) => turnNo.set(`${sid}\u0000${row.turn_id}`, i + 1),
    );
  }
  // doneIds 只需覆盖 live 候选（其段起点 >= cut，而 turn_usage.started_at 是回合起点、
  // 只比首个模型请求早秒级），限定 48h 窗走索引，避免随库龄无界增长的全表扫。
  const doneIds = new Set(
    q("SELECT turn_id FROM turn_usage WHERE started_at >= ?", cut - 24 * 60 * 60 * 1000).map(
      (r) => r.turn_id,
    ),
  );

  // ---- 步结算明细（一条查询喂多个消费者：tok/s 折叠、marker 关联、首 token、模型名、
  //      上下文水位）。query_source 白名单：main_turn（主会话）+ subagent/workflow_child
  //      （子会话，turn_id 独立不与主轮混）。session_title/compact 等旁路查询会借用主轮
  //      turn_id 落库，必须排除，否则污染主轮的 tok/s。
  //      dec 口径：decode 只计 ttft 齐备且 (duration-ttft)>0 且 output>0 的完成步。
  //      上下文水位 ctx 口径 = 官方 getModelUsageContextTokens（contracts/model/index.ts）：
  //      input + output（AI SDK v6 的 input 已含 cache read/write，cache 两列是 breakdown，
  //      不能重复相加）；取该 turn 最后一条 completed 行 = 轮末真实水位，压缩后下一步
  //      自然回落。input 缺失（0）按官方回退链退 provider_total-output、再退 computed_total。
  //      老 ZCode CLI 的库可能没有三个 token 列——探测缺列就退回无水位模式
  //      （与上面 INDEXED BY 的探测降级同款），保住原有统计。
  const hasCtxCols = q("PRAGMA table_info(model_usage)")
    .filter((r) => ["input_tokens", "provider_total_tokens", "computed_total_tokens"].includes(r.name))
    .length === 3;
  const ctxColsSql = hasCtxCols
    ? ",\n            input_tokens AS in_tok,\n            provider_total_tokens AS prov_total,\n            computed_total_tokens AS comp_total"
    : "";
  const dec = new Map(); // tid -> [解码ms, 输出tok]
  const modelsByTurn = new Map(); // tid -> [model]
  const settleByMid = new Map(); // assistant_message_id -> 步结算行
  const ttftByTid = new Map(); // tid -> 最早发起步的 ttft（不限状态）
  const ctxByTid = new Map(); // tid -> 最后一条 completed 行（含 .ctx/.qsrc/.model）
  const qsrcByTid = new Map(); // tid -> query_source（任一行即可，同轮同源）
  const qsrcBySid = new Map(); // sid -> query_source（live 段无结算行时的兜底来源）
  for (const r of q(
    `SELECT session_id AS sid, turn_id AS tid, assistant_message_id AS mid, model_id AS model,
            status, started_at, completed_at, duration_ms,
            time_to_first_token_ms AS ttft, output_tokens AS out_tok,
            query_source AS qsrc${ctxColsSql}
     FROM model_usage${startedIdx}
     WHERE started_at >= ? AND query_source IN ('main_turn', 'subagent', 'workflow_child')
     ORDER BY started_at`,
    cut,
  )) {
    if (r.status === "completed") {
      const decMs =
        r.ttft != null && r.duration_ms - r.ttft > 0 && r.out_tok > 0 ? r.duration_ms - r.ttft : 0;
      const decTok = r.ttft != null && r.out_tok > 0 ? r.out_tok : 0;
      const [d, tok] = dec.get(r.tid) || [0, 0];
      dec.set(r.tid, [d + decMs, tok + decTok]);
      if (r.model) {
        const arr = modelsByTurn.get(r.tid) || [];
        modelsByTurn.set(r.tid, arr);
        if (!arr.includes(r.model)) arr.push(r.model);
      }
      if (hasCtxCols) {
        r.ctx = ctxTokensOfRow(r);
        ctxByTid.set(r.tid, r); // 迭代按 started_at 升序，最后一行胜出
      }
    }
    if (r.tid) qsrcByTid.set(r.tid, r.qsrc);
    if (r.sid) qsrcBySid.set(r.sid, r.qsrc);
    if (r.ttft != null && !ttftByTid.has(r.tid)) ttftByTid.set(r.tid, r.ttft);
    if (r.mid) settleByMid.set(r.mid, r);
  }

  // ---- 轮内 reactive compact 修正：compact 请求占用一次 ModelRequest 事件但不产生 marker
  const compactByTid = new Map();
  for (const r of q(
    `SELECT turn_id AS tid, COUNT(*) AS n FROM model_usage${startedIdx}
     WHERE started_at >= ? AND query_source = 'compact' GROUP BY turn_id`,
    cut,
  )) {
    compactByTid.set(r.tid, r.n);
  }

  // ---- 运行中工具（含流中工具），按会话分桶（升序），按时间挂到 live 段
  const runningBySid = new Map(); // sid -> [{tid, name, start, approval}]
  for (const r of q(
    `SELECT session_id AS sid, turn_id AS tid, tool_name AS name,
            started_at AS start, approval_status AS approval
     FROM tool_usage
     WHERE status = 'running' AND started_at >= ?
     ORDER BY started_at`,
    nowMs - LIVE_TOOL_WINDOW_MS,
  )) {
    const arr = runningBySid.get(r.sid) || [];
    runningBySid.set(r.sid, arr);
    arr.push(r);
  }

  // ---- 会话发现：session.time_updated 随每次消息/part 写入被 touch，
  //      覆盖「首步尚未落任何 usage 行」的全新会话（首步可见的前提）
  const sids = new Set();
  for (const r of q("SELECT id AS sid FROM session WHERE time_updated >= ?", cut)) sids.add(r.sid);
  for (const r of turns) sids.add(r.session_id);
  for (const r of settleByMid.values()) sids.add(r.sid);

  const markerStmt = conn.prepare(
    `SELECT m.id AS mid, m.time_created AS t0, json_extract(m.data, '$.parentID') AS parent
     FROM message m
     WHERE m.session_id = ? AND m.time_created >= ?
       AND json_extract(m.data, '$.role') = 'assistant'
       AND EXISTS (
         SELECT 1 FROM part p
         WHERE p.message_id = m.id AND json_extract(p.data, '$.type') = 'step-start'
       )
     ORDER BY m.time_created, m.id`,
  );
  const boundaryStmt = conn.prepare(
    // 边界 = 轮终点（completed_at，缺失退 started_at）：live 轮自己没有 turn_usage 行，
    // 只有上一轮的「终点」能把它的 markers 从上一轮切开（轮起点做不到——上一轮的
    // markers 全在上一轮起点之后）
    "SELECT COALESCE(completed_at, started_at) AS bend FROM turn_usage WHERE session_id = ? AND started_at >= ? ORDER BY started_at",
  );
  const priorStmt = conn.prepare(
    "SELECT COUNT(*) AS n FROM turn_usage WHERE session_id = ? AND started_at < ?",
  );

  // ---- live 段合成：子智能体/workflow 子会话同样合成（其侧边栏与主界面共用
  //      section[data-turn-id] 渲染栈，turn_id 直接匹配）
  const live = [];
  for (const sid of sids) {
    const markers = markerStmt.all(sid, cut);
    if (!markers.length) continue;
    // 切段取尾段：跨过任一已结算轮起点、或相邻 marker 空洞 > 30min 都开新段；
    // 尾段即当前 live 轮（轮内 steering 只追加 marker 不换段）。
    const boundaries = boundaryStmt.all(sid, cut).map((r) => r.bend);
    const segs = [];
    let cur = [markers[0]];
    let bi = 0;
    for (let i = 1; i < markers.length; i++) {
      const prev = cur[cur.length - 1];
      const mk = markers[i];
      while (bi < boundaries.length && boundaries[bi] <= prev.t0) bi += 1;
      const crossedBoundary = bi < boundaries.length && boundaries[bi] <= mk.t0;
      if (mk.t0 - prev.t0 > SEGMENT_GAP_MS || crossedBoundary) {
        segs.push(cur);
        cur = [mk];
      } else {
        cur.push(mk);
      }
    }
    segs.push(cur);
    const seg = segs[segs.length - 1];
    const segStart = seg[0].t0;

    // settle 关联：每个 marker 至多一条结算行（model_usage id 按 assistant_message 幂等 upsert）
    let settledCount = 0;
    let lastEnd = 0;
    let segTid = null;
    let ctxRow = null; // 段内最后已结算步 = 当前上下文水位（与官方 ModelComplete 更新粒度一致）
    const settledTids = new Set();
    for (const mk of seg) {
      const s = settleByMid.get(mk.mid);
      if (!s) continue;
      settledCount += 1;
      if (s.tid) {
        settledTids.add(s.tid);
        segTid = s.tid;
      }
      if (s.ctx != null && (!ctxRow || (s.started_at || 0) >= (ctxRow.started_at || 0))) ctxRow = s;
      const end = s.completed_at ?? s.started_at + (s.duration_ms || 0);
      if (end > lastEnd) lastEnd = end;
    }
    // 步流水线是串行的，真正在飞的只可能是段尾 marker；段中无 settle 的 marker 是被
    // reactive compact / Start Plan 重试抛弃的步（永不结算）。若拿它们当进行中步，
    // 轮末 turn_usage 已接管后 live 条目会被 `!inFlight` 卡住，滞留到 freshness 过期。
    const segTail = seg[seg.length - 1];
    const inFlight = !settleByMid.has(segTail.mid) ? segTail : null;
    // 整段结算且轮已关闭 → turn_usage 已接管，live 退出（最终值原地覆盖）
    if (!inFlight && settledTids.size > 0 && [...settledTids].every((t) => doneIds.has(t))) continue;
    // 活性：最近 10 分钟有 marker 或结算落库，或窗口内仍有 running 工具
    //（长工具/长流式步期间无新行落库）。两者皆无 = 崩溃残留的半截轮，不合成。
    const freshStep = Math.max(lastEnd, seg[seg.length - 1].t0) >= nowMs - LIVE_STALE_MS;
    const tools = (runningBySid.get(sid) || []).filter((t) => t.start >= segStart);
    const freshTool = tools.length > 0; // 查询已按窗口过滤
    if (!freshStep && !freshTool) continue;

    // 操作态：tool > generating > gap；等待确认优先展示；计时取本批最早 running（批起点）
    let phase = inFlight ? "generating" : "gap";
    let tool = null;
    let toolStart = null;
    let toolPending = false;
    if (tools.length) {
      phase = "tool";
      toolStart = tools[0].start;
      const pending = tools.findLast((t) => t.approval === "requested");
      tool = (pending || tools[tools.length - 1]).name;
      toolPending = Boolean(pending);
    }

    const [decMs, decTok] = dec.get(segTid) || [0, 0];
    const tps = decMs > 0 ? (decTok * 1000) / decMs : null;
    const prior = priorStmt.get(sid, segStart).n;
    live.push({
      turn_id: segTid ?? null,
      msg_id: (seg.find((mk) => mk.parent) || {}).parent ?? null,
      session_id: sid,
      status: "running",
      live: true,
      phase, // tool | generating | gap
      start_ms: segStart,
      end_ms: nowMs, // 仅用于排序置顶；前端时间戳取 start_ms
      run_ms: Math.max(0, nowMs - segStart),
      gen_start_ms: inFlight ? inFlight.t0 : null, // 进行中步的真实起点
      gap_start_ms: phase === "gap" ? lastEnd || null : null,
      ttft_ms: ttftByTid.get(segTid) ?? null,
      tps: tps ? Math.round(tps * 100) / 100 : null,
      out_tokens: decTok || 0,
      turn_no: prior + 1,
      steps: seg.length + (compactByTid.get(segTid) || 0), // 与轮末 model_request_count 守恒
      steps_settled: settledCount, // 已结算步数（诊断用）
      models: modelsByTurn.get(segTid) || [],
      query_source: qsrcBySid.get(sid) || null, // 子会话判定（inject 只对 subagent/workflow_child 追加上下文段）
      ctx_tokens: ctxRow ? ctxRow.ctx : null, // 当前上下文水位；首步结算前为 null（不显示）
      ctx_window: ctxRow ? resolveCtxWindow(ctxRow.model) : null, // 分母解析不到 = 只显示 K
      tool, // 正在运行的工具（无则 null）
      tool_start_ms: toolStart,
      tool_pending: toolPending,
    });
  }
  const out = turns.map((r) => {
      const completed = r.completed_at ?? r.started_at;
      const [decodeMs, decodeTok] = dec.get(r.turn_id) || [0, 0];
      const tps = decodeMs > 0 ? (decodeTok * 1000) / decodeMs : null;
      const ctxRow = ctxByTid.get(r.turn_id);
      return {
        turn_id: r.turn_id,
        msg_id: r.user_message_id,
        session_id: r.session_id,
        status: r.status,
        start_ms: r.started_at,
        end_ms: completed,
        run_ms: Math.max(0, completed - r.started_at),
        ttft_ms: r.time_to_first_token_ms,
        tps: tps ? Math.round(tps * 100) / 100 : null,
        out_tokens: r.output_tokens || 0,
        turn_no: turnNo.get(`${r.session_id}\u0000${r.turn_id}`) ?? null,
        steps: r.model_request_count || 0,
        models: modelsByTurn.get(r.turn_id) || [],
        query_source: (ctxRow ? ctxRow.qsrc : qsrcByTid.get(r.turn_id)) || null,
        ctx_tokens: ctxRow ? ctxRow.ctx : null, // 轮末真实上下文水位（该轮最后一步 input+output）
        ctx_window: ctxRow ? resolveCtxWindow(ctxRow.model) : null,
      };
    });
    out.push(...live);
    out.sort((a, b) => b.end_ms - a.end_ms);
    return out;
}

const localCache = { at: 0, turns: [] };

function getLocalTurns() {
  const now = Date.now();
  if (now - localCache.at > CACHE_TTL_S * 1000) {
    try {
      localCache.turns = foldLocal();
    } catch {
      // 库忙/暂不可读时沿用上次结果
    }
    localCache.at = now;
  }
  return localCache.turns;
}

// ---------------------------------------------------------------- 远端拉取（fold-on-remote，不搬库）

// 聚合脚本的远端缓存：文件名带内容 hash，仓库脚本升级后自然换名重传；清理只按 mtime 删
// 7 天以上的陈年文件，不删异版本——两台本地机（仓库版本不同、hash 不同）共用同一远端
// 账号时，按名字互删会造成每轮 91→重传的 ping-pong。常态每轮只有一条「执行缓存脚本」
// 的短命令（~100B 上行）；缓存目录建不了/被同名抢注则退回 stdin 直送整个脚本，
// 并对该远端置 noCache 不再反复尝试。
const RDIR = "~/.cache/zcode-tps-footer";
const remoteScript = (() => {
  try {
    const bytes = fs.readFileSync(REMOTE_FOLD);
    return { hash: createHash("sha256").update(bytes).digest("hex").slice(0, 16) };
  } catch {
    return null; // 脚本读不了：远端拉取只能失败，原因会出现在 [REMOTE-ERR]
  }
})();
const rscriptPath = remoteScript ? `${RDIR}/tps_${remoteScript.hash}.py` : null;

function remoteFail(r, err) {
  // 记一次失败：保留旧数据（已画出的统计行不消失），退避由 consecFail 驱动。
  r.consecFail += 1;
  r.lastErr = err;
  log(`[REMOTE-ERR] ${r.name} fail#${r.consecFail} ${err.slice(0, 300)}`);
}

// 单次 ssh 执行：command 作为一个 argv 整体交远端 shell 解析（本地不经 shell，特殊字符
// 安全）；inputFile 提供时把该文件接到 ssh 的 stdin（上传脚本用）。resolve
// { code, stdout, stderr }；超时/起不了进程统一 code:-1、原因在 stderr。
function sshRun(r, command, inputFile) {
  return new Promise((resolve) => {
    const args = [
      "ssh",
      "-o", "BatchMode=yes", // 免密失败立即退出，绝不交互卡死
      "-o", "ConnectTimeout=6",
      "-o", "StrictHostKeyChecking=accept-new",
      "-o", "LogLevel=ERROR",
      "-p", String(r.port),
      r.host,
      command,
    ];
    let fd = null;
    let child = null;
    try {
      if (inputFile) fd = fs.openSync(inputFile, "r");
      child = spawn(args[0], args.slice(1), { stdio: [fd ?? "ignore", "pipe", "pipe"], windowsHide: true });
      if (fd != null) fs.closeSync(fd); // 子进程已持有自己的描述符副本
    } catch (e) {
      if (fd != null) {
        try { fs.closeSync(fd); } catch { /* ignore */ }
      }
      return resolve({ code: -1, stderr: `ssh prepare failed: ${e && e.message}` });
    }
    let stdout = "";
    let stderr = "";
    let settled = false;
    const finish = (out) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve(out);
    };
    const timer = setTimeout(() => {
      try { child.kill(); } catch { /* ignore */ }
      finish({ code: -1, stderr: `ssh timeout >${SSH_TIMEOUT_S}s` });
    }, SSH_TIMEOUT_S * 1000);
    child.stdout.on("data", (c) => (stdout += c));
    child.stderr.on("data", (c) => (stderr += c));
    child.on("error", (e) =>
      finish({ code: -1, stderr: e.code === "ENOENT" ? "ssh executable not found in PATH" : String(e) }),
    );
    child.on("close", (code) => finish({ code: code ?? -1, stdout, stderr }));
  });
}

// 增量合并表操作：主键 = turn_id，首步未结算的 live 条目退用 msg_id。同一轮的 live 与
// 已结算版本靠 turn_id/msg_id 双向去重（后到覆盖先到），live→settled 迁移不留双份。
function mergeRemoteTurn(map, t) {
  if (!t || (!t.turn_id && !t.msg_id)) return;
  const key = t.turn_id || `m:${t.msg_id}`;
  for (const [k, v] of map) {
    if (k === key) continue;
    if ((t.turn_id && v.turn_id === t.turn_id) || (t.msg_id && v.msg_id === t.msg_id)) map.delete(k);
  }
  map.set(key, t);
}

// 三段式取数（常态只有 ① 的 1 次 ssh 往返）：
//   ① 缓存命中执行：`test -f <缓存脚本> || exit 91; exec python3 <缓存脚本> [since]`
//   ② 91 时上传并执行：mkdir + 写 .tmp 原子 mv + chmod + 清 7 天以上陈年缓存 + exec python3
//   ③ 92/93（目录建不了/同名文件写不进）时退回 `python3 -` stdin 直送，并置 noCache
// 约定码 91/92/93 只由我们的命令串产生；ssh 自身失败是 255，远端命令失败原样透传。
async function fetchRemote(r) {
  // 增量判定：有水位且距上次全量不满自愈周期才带 since；否则全量（首拉/失败后/周期自愈）
  const sinceMs = r.since > 0 && Date.now() - r.lastFullAt < FULL_SYNC_MS ? r.since : 0;
  const arg = sinceMs ? ` ${sinceMs}` : "";
  let res;
  if (rscriptPath && !r.noCache) {
    res = await sshRun(r, `test -f ${rscriptPath} || exit 91; exec python3 ${rscriptPath}${arg}`);
    if (res.code === 91) {
      res = await sshRun(
        r,
        // 写 .tmp 再 mv：mv 在同目录下是原子改名，传输中途断连（远端 shell 随连接被杀，
        // 来不及走 || 分支）只会留下 .tmp 残渣由 find 慢清，最终文件名永远不会出现半截
        // 脚本——否则 test -f 命中且 hash 与本地一致，该远端会陷入「执行损坏脚本失败、
        // 无任何自愈路径」的死循环。.tmp 带 $$（远端 shell PID）：两台本地机同 hash 并发
        // 上传时各写各的临时名，避免互相 O_TRUNC 截断出带零洞的文件
        `mkdir -p ${RDIR} && chmod 700 ${RDIR} || exit 92; cat > ${rscriptPath}.$$.tmp || exit 93; ` +
          `mv ${rscriptPath}.$$.tmp ${rscriptPath} && chmod 600 ${rscriptPath}; ` +
          `find ${RDIR} -maxdepth 1 -name 'tps_*' -mtime +7 -delete; ` +
          `exec python3 ${rscriptPath}${arg}`,
        REMOTE_FOLD,
      );
      if (res.code === 0) log(`[REMOTE-SYNC] ${r.name} uploaded tps_${remoteScript.hash}.py`);
    }
    if (res.code === 92 || res.code === 93) {
      r.noCache = true; // 别每轮为同一结果多付两次失败往返
      log(`[REMOTE-CACHE-OFF] ${r.name} rc=${res.code} ${res.stderr.slice(0, 160)}，退回 stdin 模式`);
    }
  }
  if (!rscriptPath || r.noCache) res = await sshRun(r, `exec python3 -${arg}`, REMOTE_FOLD);
  // 失败一律清水位：下次成功走全量，增量状态机不背着脏水位继续跑
  const fail = (err) => {
    r.since = 0;
    // polled 门控的语义是「本进程对这台远端的空表有代表性」。从未成功过（lastOk=0，
    // 典型：服务重启恰逢断网/VPN 掉线）就置 polled=true，会把一张因故障而空的表
    // 当成完整快照下发，注入端整体替换后远端轮次从界面集体消失——正是要修的 bug
    // 换了个触发条件复发。只有成功过的远端失败才算「有代表性」（表里还留着旧数据）。
    if (r.lastOk > 0) r.polled = true;
    return remoteFail(r, err);
  };
  if (res.code !== 0) return fail(`ssh rc=${res.code} ${res.stderr.slice(0, 200)}`);
  let data;
  try {
    data = JSON.parse(res.stdout);
  } catch (e) {
    return fail(`bad JSON from remote: ${e && e.message}`);
  }
  // 远端脚本报错（库损坏/读不了）→ 失败但保留旧数据；
  // 空结果且无 error = 远端 24h 内确实没用量 → 正常清空。
  if (data.error) return fail(`remote: ${data.error}`);
  const full = !sinceMs || data.full === true;
  if (full) r.map.clear();
  for (const t of data.turns || []) mergeRemoteTurn(r.map, t);
  // 逐出：滑出窗口的老条目（比远端 24h 折叠窗多留 2h，2h 松量足以吸收任何现实时钟
  // 偏差）+ 冻结的 live 条目。live 的 end_ms 是远端时钟域的折叠时刻，冻结判定必须先
  // 换算到同一时钟域——远端慢时钟超 LIVE_FREEZE_MS 时若直接比本地时钟，新鲜 live 会
  // 在合并的同一轮被误逐出（其「运行中」行永远出不来）；这里用本次响应的 live 条目
  // （end_ms 即远端 now，同轮所有 live 同一时刻）刷新偏移采样，再按校正后的时钟判定。
  // 失败轮询不进本路径，故障期间旧数据按原样保留。
  for (const t of data.turns || []) {
    if (t.live) {
      r.clockOffset = Date.now() - t.end_ms; // 采样含 ssh 往返延迟，秒级、远小于判定窗口
      break;
    }
  }
  const evictBefore = Date.now() - EVICT_MS;
  const liveFrozenBefore = r.clockOffset != null ? Date.now() - r.clockOffset - LIVE_FREEZE_MS : 0;
  for (const [k, t] of r.map) {
    if (t.end_ms < evictBefore || (t.live && t.end_ms < liveFrozenBefore)) r.map.delete(k);
  }
  r.turns = [...r.map.values()].sort((a, b) => b.end_ms - a.end_ms);
  // 水位取远端按全量口径算好的值（remote_fold.py 保证存在且 > 0；异常时归 0 走全量）
  r.since = Number(data.since) || 0;
  if (full) r.lastFullAt = Date.now();
  r.lastOk = Date.now();
  r.consecFail = 0;
  r.lastErr = "";
  r.polled = true;
  log(`[REMOTE-OK] ${r.name} turns=${r.turns.length} ${full ? "full" : `inc+${(data.turns || []).length}`}`);
}

let lastHttpRequest = 0; // 最近一次 /turns 请求（仅此端点计入：inject 靠它每秒取数，是「渲染层在世」的判据；/status、/ping 等排障端点不续命，也不给 /healthz 续命——否则监视器探活会阻止闲置判定）

function idleState() {
  const anchor = lastHttpRequest || processStartMs;
  const idleS = Math.max(0, Math.round((Date.now() - anchor) / 1000));
  return { idle_s: idleS, suspended: idleS > SUSPEND_AFTER_S };
}

// 每台远端独立调度：500ms 一tick，到点的、不在飞的远端并行拉起，互不等待。
// 单台失败只有它自己退避（指数、封顶 MAX_BACKOFF），健康远端保持正常节奏不被拖慢。
async function remotePollLoop() {
  let wasSuspended = false;
  for (;;) {
    try {
      reloadConfigIfChanged();
      const anchor = lastHttpRequest || processStartMs; // 起步阶段从进程启动计时
      const idleMs = Date.now() - anchor;
      const suspended = idleMs > SUSPEND_AFTER_S * 1000;
      if (suspended !== wasSuspended) {
        log(suspended ? `[SUSPEND] idle=${Math.round(idleMs / 1000)}s，挂起远端轮询` : "[RESUME] 收到请求，恢复远端轮询");
        wasSuspended = suspended;
      }
      const active = !suspended && idleMs / 1000 < IDLE_AFTER_S;
      const now = Date.now();
      for (const r of remotes) {
        if (suspended || r.inFlight || now < r.nextPollAt) continue;
        r.inFlight = true;
        // .catch 兜底：fetchRemote 理论上不抛（sshRun 只 resolve、解析均有 try/catch），
        // 但这里若真抛出会成为无人接住的 rejection——Node 默认直接退进程，正是历史上
        // 「无声死亡」的头号嫌疑路径
        fetchRemote(r)
          .catch((e) => log(`[REMOTE-THROW] ${r.name} ${errBrief(e)}`))
          .finally(() => {
            r.inFlight = false;
            // 分档：渲染层活跃且该远端有 live 轮 → 快；活跃但远端无 live → 中档
            //（已结算轮不可变，探新轮用）；渲染层不活跃 → 慢档。失败退避乘在三档之上。
            const hasLive = r.turns.some((t) => t.live);
            const base = active ? (hasLive ? FAST_POLL_S : IDLE_REMOTE_POLL_S) : SLOW_POLL_S;
            const interval = base * Math.min(2 ** r.consecFail, MAX_BACKOFF);
            r.nextPollAt = Date.now() + interval * 1000;
          });
      }
    } catch (e) {
      log(`[POLL-ERR] ${e && e.stack ? e.stack.split("\n")[0] : e}`);
    }
    await new Promise((res) => setTimeout(res, 500));
  }
}

// ---------------------------------------------------------------- HTTP

// 全量口径合并（本地折叠 + 各远端合并表），供 /turns 与增量过滤共用。
// sinceQ > 0 时只返回 live 条目 + end_ms >= sinceQ - INC_MARGIN_MS 的已结算轮
//（与远端侧同一边距语义）；响应的 since 水位取全量口径的最新 end_ms——live 条目的
// end_ms 即合成时刻，天然领先任何已结算轮，用它做下次过滤的锚不会漏新结算。
// full 标记额外要求各远端都完成过一次拉取尝试：服务刚重启的 1~2 秒里远端表还是
// 空的，此刻把「全量」应答交给注入端会被其整体替换语义采纳，远端轮次从界面上
// 集体消失（2026-10-08 实测 NOMATCH dom=11 cache=10 就是这个窗口）。置 false 后
// 注入端退化为合并语义，旧缓存原样保留。
function mergedTurns(limit, sinceQ = 0) {
  const out = [...getLocalTurns()];
  for (const r of remotes) {
    for (const t of r.turns) {
      out.push({
        ...t,
        host: r.name,
        // 远端不做目录解析（分母），统一由本地按水位行的模型补齐；解析不到保持 null。
        // ctx_model 是远端折叠导出的「水位行精确模型」，无水位时退 models 去重列表
        // 末位——仅轮内切换过模型时有纯展示层偏差
        ctx_window: t.ctx_window ?? resolveCtxWindow(t.ctx_model ?? (t.models || [])[t.models?.length - 1]),
      });
    }
  }
  out.sort((a, b) => b.end_ms - a.end_ms);
  const watermark = out.length ? out[0].end_ms : 0;
  const list = sinceQ > 0 ? out.filter((t) => t.live || t.end_ms >= sinceQ - INC_MARGIN_MS) : out;
  // cfgSig !== null = 本进程至少成功解析过一次远端配置。启动瞬间恰好撞上桌面端
  // 原子重写 setting.json（半写 JSON 解析失败、remotes 暂为空表）时，空表 every
  // 恒真会把不含远端数据的应答标记成完整快照——加这道门让它按合并语义交付。
  const full = sinceQ <= 0 && remotes.every((r) => r.polled) && cfgSig !== null;
  return { turns: list.slice(0, limit), since: watermark, full };
}

function statusPayload() {
  const now = Date.now();
  let localExists = false;
  try {
    localExists = fs.existsSync(DB);
  } catch {
    /* ignore */
  }
  return {
    local: { db: DB, exists: localExists, turns: getLocalTurns().length },
    remotes: remotes.map((r) => ({
      name: r.name,
      host: `${r.host}:${r.port}`,
      src: r.src,
      turns: r.turns.length,
      ok_age_s: r.lastOk ? Math.round(((now - r.lastOk) / 1000) * 10) / 10 : null,
      consec_fail: r.consecFail,
      error: r.lastErr || null,
    })),
  };
}

function send(res, code, body, ctype = "application/json") {
  const buf = Buffer.from(body, "utf8");
  res.writeHead(code, {
    "Content-Type": ctype,
    "Content-Length": buf.length,
    "Access-Control-Allow-Origin": "*",
  });
  res.end(buf);
}

// ---------------------------------------------------------------- 注入开关（实机 A/B 免重启）

// 闪烁排查对照实验：POST /tps-state?enabled=0 → inject.js 下一次取数（≤2s）撤掉全部
// 统计行，enabled=1 恢复，全程不用重启 ZCode。持久化 logs/tps-state.json（服务重启
// 保持上次状态）；文件缺失/损坏 = 默认开。
const STATE_PATH = path.join(ROOT, "logs", "tps-state.json");
let tpsEnabled = true;
try {
  tpsEnabled = JSON.parse(fs.readFileSync(STATE_PATH, "utf8")).enabled !== false;
} catch {
  /* 文件缺失/损坏 = 默认开 */
}

const server = createServer((req, res) => {
  try {
    const u = new URL(req.url, "http://127.0.0.1");
    if (u.pathname === "/healthz") return send(res, 200, JSON.stringify({ ok: true, pid: process.pid, ...idleState() }));
    if (u.pathname === "/tps-state") {
      if (req.method === "POST") {
        const v = u.searchParams.get("enabled") !== "0";
        if (v !== tpsEnabled) {
          tpsEnabled = v;
          try {
            fs.writeFileSync(STATE_PATH, JSON.stringify({ enabled: tpsEnabled }, null, 2));
          } catch {
            /* 状态文件写不了就只留内存态（下次重启回默认开） */
          }
          log(`[TPS] inject ${tpsEnabled ? "enabled" : "disabled"}`);
        }
      }
      return send(res, 200, JSON.stringify({ enabled: tpsEnabled }));
    }
    if (u.pathname === "/turns") {
      lastHttpRequest = Date.now();
      const limit = Math.min(parseInt(u.searchParams.get("limit") || "500", 10) || 500, 2000);
      // since（可选，ms 水位）：>0 走增量（live 全带 + 水位-边距内已结算轮），响应回传
      // 新水位供下次使用；缺省/0 = 全量。渲染层 fetch 失败会自行归零重拉全量自愈。
      const sinceQ = Math.max(0, parseInt(u.searchParams.get("since") || "0", 10) || 0);
      return send(res, 200, JSON.stringify({ ...mergedTurns(limit, sinceQ), tps: tpsEnabled }));
    }
    if (u.pathname === "/status") return send(res, 200, JSON.stringify(statusPayload()));
    if (u.pathname === "/ping") {
      log(`[PING] secs=${u.searchParams.get("secs") ?? "?"} snap=${(u.searchParams.get("snap") || "").slice(0, 1500)}`);
      return send(res, 200, "pong", "text/plain");
    }
    return send(res, 404, '{"error":"not found"}');
  } catch {
    try {
      send(res, 500, '{"error":"internal"}');
    } catch {
      /* ignore */
    }
  }
});

// ---------------------------------------------------------------- 启动

cleanupLog();
setInterval(cleanupLog, 24 * 3600 * 1000).unref();
// listen 之前先把远端表建好：首个 /turns（全量）就能给出正确的 full 完整性标记，
// 不依赖 remotePollLoop 的首个 500ms tick
reloadConfigIfChanged();
remotePollLoop();
log(`[START] pid=${process.pid} node=${process.version} port=${PORT} db=${DB} remote_fold=${REMOTE_FOLD} root=${ROOT}`);
server.on("error", (e) => {
  log(`[FATAL] cannot bind 127.0.0.1:${PORT}: ${e && e.message}`);
  process.exit(1);
});
server.listen(PORT, "127.0.0.1");
