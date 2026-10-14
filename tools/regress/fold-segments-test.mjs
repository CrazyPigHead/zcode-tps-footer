// 折叠回归：合成最小用量库，同一 fixture 分别喂给两个实现——server.mjs 的 /turns 与
// remote_fold.py 的 fold()——断言同一组期望值（两 fold 是 1:1 同构，必须同步通过）。
//
// 隔离：合成库 TPS_FOOTER_DB + 合成配置目录 TPS_FOOTER_V2_DIR + 测试端口（3211/3212）；
// 不碰生产 3117/3118，不读真实 provider 配置，不连任何远端（合成目录里没有 setting.json，
// 远端列表为空）。Python 侧按仓库约定走 `uv run python`（勿用裸 python）。
//
// 用例（T1-T6 就是历史故障的现场形状；T7 为无空洞回归锁；T8 为老 schema 降级）：
//   T1 长流式步空洞（55min 单步）→ 切段回并，start_ms/步数恢复整轮真值（修复前腰斩）
//   T2 崩溃残留 + 新轮 → 不并（turn_id 无交集、洞内已完成工具远离切点）
//   T3 长工具空洞（40min 工具，完成后 20s 紧贴下一步 marker）→ 回并
//   T4 流式活性：尾步 12min 前开始 → 仍发射（修复前熄灭）；3h 前开始 → 不发射
//   T5 长工具活性：running 工具 70min → 仍发射且 phase=tool（修复前出窗熄灭）
//   T6 窗口作用域：(providerId, modelId) 双匹配；孤儿规则不泄漏；缺 provider_id 退 modelId-only
//   T7 无空洞正常轮：live 与已结算轮逐键等于金标（= 修复前形状 + Fix B 新增的 ctx_provider）
//   T8 老 schema（无 provider_id / 无三个 token 列）：统计不整体消失，水位字段为 null
import { spawn, spawnSync } from "node:child_process";
import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { fileURLToPath } from "node:url";

const REPO_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const WORK = path.join(os.tmpdir(), "tps-footer-fold-test");
const PORT_MAIN = 3211;
const PORT_LEGACY = 3212;
const MIN = 60_000;
const HOUR = 60 * MIN;
const NOW = Date.now();
const fwd = (p) => p.replace(/\\/g, "/"); // sqlite URI（py）与 node:sqlite 都吃正斜杠
const DUMP = process.argv.includes("--dump");

const PLAT = "testplat";
const VER = "9.9.9";
const PROVIDER_A = "prov-A";
const PROVIDER_B = "prov-B";
const CODE_PLAN = "account:bigmodel-individual-coding-plan"; // ea43 故障轮的 provider
const GATEWAY = "c480de10-5909-4dff-8d9f-289525331dd1"; // 个人覆盖规则所属 provider

// ---------------------------------------------------------------- 断言

const fails = [];
function check(name, cond, detail = "") {
  if (cond) console.log(`   ✓ ${name}`);
  else {
    console.log(`   ✗ ${name}${detail ? ` — ${detail}` : ""}`);
    fails.push(name);
  }
}
const j = (v) => JSON.stringify(v);
function eq(name, got, want) {
  check(name, j(got) === j(want), `got ${j(got)} want ${j(want)}`);
}
function eqFields(prefix, row, want, volatileKeys = []) {
  if (!row) {
    check(`${prefix} 条目存在`, false, "未找到该条目");
    return;
  }
  for (const [k, v] of Object.entries(want)) {
    if (volatileKeys.includes(k)) continue;
    eq(`${prefix}.${k}`, row[k], v);
  }
}
function eqShape(name, row, wantKeys) {
  const keys = row ? Object.keys(row).sort() : [];
  const want = [...wantKeys].sort();
  check(name, j(keys) === j(want), `got [${keys.join(",")}] want [${want.join(",")}]`);
}

// ---------------------------------------------------------------- 合成配置目录

function buildV2dir() {
  const dir = path.join(WORK, "v2");
  fs.mkdirSync(path.join(dir, "runtime", "provider", PLAT, VER, `endpoint-${PLAT}`), { recursive: true });
  fs.writeFileSync(
    path.join(dir, "provider_config.json"),
    JSON.stringify(
      {
        schemaVersion: 1,
        config: {
          modelConfigRules: {
            providerModelRules: [
              // 个人精确规则：作用域 = (providerId, modelId) 双匹配
              { providerId: GATEWAY, modelId: "glm-5.3", config: { properties: { contextWindow: 300000 } } },
              // 孤儿规则（该 provider 不服务此模型；语义仍需双匹配，不得泄漏给 coding-plan）
              { providerId: GATEWAY, modelId: "glm-5.3-flash", config: { properties: { contextWindow: 300000 } } },
              // 无窗口值 → 跳过（只改 enabled 之类的规则不该影响分母）
              { providerId: CODE_PLAN, modelId: "GLM-5.3-FlashX", config: { enabled: true } },
            ],
            manualProviderModelRules: [{ providerId: "prov-M", modelId: "glm-5.3", config: { properties: { contextWindow: 400000 } } }],
          },
        },
      },
      null,
      2,
    ),
  );
  fs.writeFileSync(
    path.join(dir, "runtime", "provider", PLAT, VER, `endpoint-${PLAT}`, "zcode-builtin.json"),
    JSON.stringify(
      {
        schemaVersion: 1,
        config: {
          modelConfigRules: {
            // 内置正则目录：全锚定 /i，后命中覆盖先命中（开头 `.*` 的兜底值不被永久遮蔽）
            modelRules: [
              { modelMatch: ".*", config: { properties: { contextWindow: 200000 } } },
              { modelMatch: ".*glm-5\\.3(?:-flash)?(?:[.\\-:/\\[].*)?", config: { properties: { contextWindow: 1000000 } } },
              { modelMatch: ".*test-model-x(?:[.\\-:/\\[].*)?", config: { properties: { contextWindow: 262144 } } },
              { modelMatch: ".*deepseek-flash(?:[.\\-:/\\[].*)?", config: { properties: { contextWindow: 1000000 } } },
            ],
            // 内置 provider 作用域精确规则（当前真实目录里这 30 条都不带窗口值，这里补一条带值的
            // 以验证读取与作用域；真实目录行为变化时该层自然生效）
            builtinProviderModelRules: [
              { providerId: PROVIDER_A, modelId: "test-model-x", config: { enabled: true, properties: { contextWindow: 512000 } } },
              { providerId: PROVIDER_A, modelId: "GLM-5.3", config: { enabled: true } },
            ],
          },
        },
      },
      null,
      2,
    ),
  );
  return dir;
}

// ---------------------------------------------------------------- 合成库

const SCHEMA_COMMON = `
CREATE TABLE session (id TEXT PRIMARY KEY, time_created INTEGER, time_updated INTEGER);
CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, data TEXT);
CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT, data TEXT);
CREATE TABLE turn_usage (id TEXT PRIMARY KEY, session_id TEXT, turn_id TEXT, user_message_id TEXT, status TEXT,
                         started_at INTEGER, completed_at INTEGER, time_to_first_token_ms INTEGER,
                         output_tokens INTEGER, model_request_count INTEGER);
CREATE TABLE tool_usage (id TEXT PRIMARY KEY, session_id TEXT, turn_id TEXT, tool_name TEXT, approval_status TEXT,
                         status TEXT, started_at INTEGER, completed_at INTEGER);
`;

function makeDb(file, legacy = false) {
  const db = new DatabaseSync(file);
  db.exec(SCHEMA_COMMON);
  db.exec(
    legacy
      ? // 老 ZCode CLI：无 provider_id、无三个 token 列（两处探测降级都要走到）
        `CREATE TABLE model_usage (id TEXT PRIMARY KEY, session_id TEXT, turn_id TEXT, assistant_message_id TEXT,
                                   query_source TEXT, model_id TEXT, status TEXT, started_at INTEGER, completed_at INTEGER,
                                   duration_ms INTEGER, time_to_first_token_ms INTEGER, output_tokens INTEGER);
         CREATE INDEX model_usage_started_model_idx ON model_usage(started_at, model_id);`
      : `CREATE TABLE model_usage (id TEXT PRIMARY KEY, session_id TEXT, turn_id TEXT, assistant_message_id TEXT,
                                   query_source TEXT, provider_id TEXT, model_id TEXT, status TEXT, started_at INTEGER,
                                   completed_at INTEGER, duration_ms INTEGER, time_to_first_token_ms INTEGER,
                                   input_tokens INTEGER, output_tokens INTEGER, provider_total_tokens INTEGER,
                                   computed_total_tokens INTEGER);
         CREATE INDEX model_usage_started_model_idx ON model_usage(started_at, provider_id, model_id);`,
  );
  return db;
}

let seq = 0;
const uid = (p) => `${p}${++seq}`;

function sess(db, sid, updated = NOW - 30_000) {
  db.prepare("INSERT INTO session (id, time_created, time_updated) VALUES (?,?,?)").run(sid, updated - HOUR, updated);
}
// marker = assistant 消息 + 一条 step-start part（请求发出前落库）；parent = 轮内用户消息 ID（msg_id 桥）
function marker(db, sid, t0, parent) {
  const mid = uid("msg_");
  db.prepare("INSERT INTO message (id, session_id, time_created, data) VALUES (?,?,?,?)").run(
    mid,
    sid,
    t0,
    JSON.stringify({ role: "assistant", parentID: parent }),
  );
  db.prepare("INSERT INTO part (id, message_id, data) VALUES (?,?,?)").run(uid("part_"), mid, JSON.stringify({ type: "step-start" }));
  return mid;
}
function settle(db, { sid, tid, mid, model, prov = null, started, duration, ttft = null, out = 0, inTok = 0, qsrc = "main_turn", completed = null }) {
  db.prepare(
    `INSERT INTO model_usage (id, session_id, turn_id, assistant_message_id, query_source, provider_id, model_id, status,
       started_at, completed_at, duration_ms, time_to_first_token_ms, input_tokens, output_tokens,
       provider_total_tokens, computed_total_tokens) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
  ).run(uid("mu_"), sid, tid, mid, qsrc, prov, model, "completed", started, completed ?? started + duration, duration, ttft, inTok, out, null, null);
}
function turnRow(db, { sid, tid, userMsg, started, completed, reqCount, ttft = null, out = 0 }) {
  db.prepare(
    `INSERT INTO turn_usage (id, session_id, turn_id, user_message_id, status, started_at, completed_at,
       time_to_first_token_ms, output_tokens, model_request_count) VALUES (?,?,?,?,?,?,?,?,?,?)`,
  ).run(uid("tu_"), sid, tid, userMsg, "completed", started, completed, ttft, out, reqCount);
}
function tool(db, { sid, tid, name, started, completed, status, approval = "approved" }) {
  db.prepare(
    "INSERT INTO tool_usage (id, session_id, turn_id, tool_name, approval_status, status, started_at, completed_at) VALUES (?,?,?,?,?,?,?,?)",
  ).run(uid("tou_"), sid, tid, name, approval, status, started, completed);
}

// ---------------------------------------------------------------- fixture：T1-T7

const S = {
  t1: "sess-t1", t2: "sess-t2", t3: "sess-t3", t4a: "sess-t4a", t4b: "sess-t4b",
  t5: "sess-t5", t7: "sess-t7",
};
const T6 = [
  { sid: "sess-t6a", tid: "T6A", model: "glm-5.3", prov: GATEWAY, want: 300000, why: "个人精确规则作用域命中（覆盖内置正则 1M）" },
  { sid: "sess-t6b", tid: "T6B", model: "GLM-5.3-Flash", prov: CODE_PLAN, want: 1000000, why: "个人规则作用域不符 → 内置正则（ea43 故障）" },
  { sid: "sess-t6c", tid: "T6C", model: "test-model-x", prov: PROVIDER_A, want: 512000, why: "内置 provider 精确规则命中" },
  { sid: "sess-t6d", tid: "T6D", model: "test-model-x", prov: PROVIDER_B, want: 262144, why: "内置 provider 精确规则作用域不符 → 退回正则" },
  { sid: "sess-t6e", tid: "T6E", model: "glm-5.3", prov: null, want: 400000, why: "缺 provider_id 的老数据退 modelId-only（个人与 manual 均按 modelId 命中，最后叠加的 manual 胜出）" },
  { sid: "sess-t6f", tid: "T6F", model: "glm-5.3", prov: "prov-M", want: 400000, why: "manualProviderModelRules 参与且最后叠加" },
];

const T = {}; // 各用例的关键时刻/金标值，供断言复用

function buildFixture(db) {
  // ---- T1：单轮内一次 55min 长流式步（marker 空洞）→ 修复前尾段起点被重置、用时腰斩
  sess(db, S.t1);
  T.t1b = NOW - 70 * MIN;
  {
    const b = T.t1b;
    const m1 = marker(db, S.t1, b, "user-t1");
    settle(db, { sid: S.t1, tid: "T1", mid: m1, model: "GLM-5.3-Flash", prov: CODE_PLAN, started: b, duration: 15000, ttft: 3000, out: 100, inTok: 1000 });
    const t2 = b + MIN; // 第 2 步：长流式，55min 后才结算
    const m2 = marker(db, S.t1, t2, "user-t1");
    settle(db, { sid: S.t1, tid: "T1", mid: m2, model: "GLM-5.3-Flash", prov: CODE_PLAN, started: t2, duration: 55 * MIN, ttft: 4000, out: 50000, inTok: 2000 });
    const t3 = t2 + 55 * MIN + 50; // 下一步 marker 距上步结算仅 50ms（步流水线串行）
    const m3 = marker(db, S.t1, t3, "user-t1");
    settle(db, { sid: S.t1, tid: "T1", mid: m3, model: "GLM-5.3-Flash", prov: CODE_PLAN, started: t3, duration: 20000, ttft: 5000, out: 200, inTok: 3000 });
    const m4 = marker(db, S.t1, t3 + MIN, "user-t1");
    settle(db, { sid: S.t1, tid: "T1", mid: m4, model: "GLM-5.3-Flash", prov: CODE_PLAN, started: t3 + MIN, duration: 20000, ttft: 5000, out: 200, inTok: 3000 });
    const m5 = marker(db, S.t1, t3 + 2 * MIN, "user-t1");
    settle(db, { sid: S.t1, tid: "T1", mid: m5, model: "GLM-5.3-Flash", prov: CODE_PLAN, started: t3 + 2 * MIN, duration: 20000, ttft: 5000, out: 200, inTok: 3000 });
    marker(db, S.t1, NOW - 30_000, "user-t1"); // 尾步在飞（未结算）
  }

  // ---- T2：旧轮崩在流式中（尾步未结算）+ 洞内已完成工具远离切点 + 40min 后新轮 → 禁止回并
  sess(db, S.t2);
  {
    const b = NOW - 3 * HOUR;
    const m1 = marker(db, S.t2, b, "user-t2a");
    settle(db, { sid: S.t2, tid: "TO", mid: m1, model: "glm-x", started: b, duration: 10000, ttft: 2000, out: 50, inTok: 500 });
    marker(db, S.t2, b + 5 * MIN, "user-t2a"); // 崩点：永久未结算的尾步
    tool(db, { sid: S.t2, tid: "TO", name: "Bash", started: b + 5 * MIN + 1000, completed: b + 6 * MIN, status: "completed" });
    T.t2m3 = NOW - 6 * MIN; // 新轮首 marker，与旧轮残留相隔约 2h49min
    const m3 = marker(db, S.t2, T.t2m3, "user-t2b");
    settle(db, { sid: S.t2, tid: "TW", mid: m3, model: "glm-x", started: T.t2m3, duration: 5000, ttft: 1000, out: 20, inTok: 800 });
    marker(db, S.t2, T.t2m3 + MIN, "user-t2b"); // 新轮尾步在飞
  }

  // ---- T3：一次 40min 长工具（完成后 20s 紧贴下一步 marker）→ 桥接回并
  sess(db, S.t3);
  {
    T.t3b = NOW - 70 * MIN;
    const m1 = marker(db, S.t3, T.t3b, "user-t3");
    settle(db, { sid: S.t3, tid: "TT", mid: m1, model: "glm-x", started: T.t3b, duration: 20000, ttft: 2000, out: 30, inTok: 900 });
    const toolStart = T.t3b + 30_000;
    tool(db, { sid: S.t3, tid: "TT", name: "Bash", started: toolStart, completed: toolStart + 40 * MIN, status: "completed" });
    marker(db, S.t3, toolStart + 40 * MIN + 20_000, "user-t3"); // 尾步在飞
  }

  // ---- T4：流式活性上限（12min 前开始的尾步仍在活跃区间；3h 前的不再发射）
  sess(db, S.t4a);
  marker(db, S.t4a, NOW - 12 * MIN, "user-t4a");
  sess(db, S.t4b);
  marker(db, S.t4b, NOW - 3 * HOUR, "user-t4b");

  // ---- T5：running 工具已跑 70min → 仍视为活跃且 phase=tool
  sess(db, S.t5);
  {
    const b = NOW - 80 * MIN;
    const m1 = marker(db, S.t5, b, "user-t5");
    settle(db, { sid: S.t5, tid: "TV", mid: m1, model: "glm-x", started: b, duration: 10000, ttft: 2000, out: 40, inTok: 700 });
    T.t5ToolStart = NOW - 70 * MIN;
    tool(db, { sid: S.t5, tid: "TV", name: "Bash", started: T.t5ToolStart, completed: null, status: "running" });
  }

  // ---- T6：窗口作用域（每个 case 一个已结算轮，便于直接看 ctx_window）
  T6.forEach((c, i) => {
    sess(db, c.sid);
    c.start = NOW - (20 + i * 2) * MIN;
    const m1 = marker(db, c.sid, c.start, `user-${c.tid}`);
    settle(db, { sid: c.sid, tid: c.tid, mid: m1, model: c.model, prov: c.prov, started: c.start, duration: 30000, ttft: 2000, out: 100, inTok: 1000 });
    turnRow(db, { sid: c.sid, tid: c.tid, userMsg: `user-${c.tid}`, started: c.start, completed: c.start + 30000, reqCount: 1, ttft: 2000, out: 100 });
  });

  // ---- T7：无空洞的正常轮（subagent 会话，锁全字段形状与取值）。尾步刻意落在 10min
  //      新鲜窗内：修复前后都发射，黄金值才是真正的前后对比基线
  sess(db, S.t7);
  {
    T.t7b = NOW - 30 * MIN;
    const m1 = marker(db, S.t7, T.t7b, "user-t7");
    settle(db, { sid: S.t7, tid: "TN", mid: m1, model: "deepseek-flash", prov: "deepseek", started: T.t7b, duration: 20000, ttft: 2000, out: 500, inTok: 10000, qsrc: "subagent" });
    const m2 = marker(db, S.t7, T.t7b + 40_000, "user-t7");
    settle(db, { sid: S.t7, tid: "TN", mid: m2, model: "deepseek-flash", prov: "deepseek", started: T.t7b + 40_000, duration: 30000, ttft: 3000, out: 700, inTok: 11000, qsrc: "subagent" });
    marker(db, S.t7, NOW - 5 * MIN, "user-t7"); // 尾步在飞（< 10min：新鲜）
  }
}

// ---- T8：老 schema（无 provider_id / 无三个 token 列）——统计不整体消失，水位字段为 null
function buildLegacyFixture(db) {
  const insMu = (sid, tid, mid, model, started, duration, ttft, out) =>
    db
      .prepare(
        `INSERT INTO model_usage (id, session_id, turn_id, assistant_message_id, query_source, model_id, status,
           started_at, completed_at, duration_ms, time_to_first_token_ms, output_tokens) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`,
      )
      .run(uid("mu_"), sid, tid, mid, "main_turn", model, "completed", started, started + duration, duration, ttft, out);
  sess(db, "sess-t8");
  const b = NOW - 30 * MIN;
  const m1 = marker(db, "sess-t8", b, "user-t8");
  insMu("sess-t8", "TL", m1, "glm-x", b, 15000, 2000, 30);
  marker(db, "sess-t8", NOW - 8 * MIN, "user-t8"); // 在飞尾步（10min 新鲜窗内：修复前后都发射）
  sess(db, "sess-t8b");
  const c = NOW - 25 * MIN;
  const m2 = marker(db, "sess-t8b", c, "user-t8b");
  insMu("sess-t8b", "TC", m2, "glm-x", c, 8000, 1500, 10);
  turnRow(db, { sid: "sess-t8b", tid: "TC", userMsg: "user-t8b", started: c, completed: c + 8000, reqCount: 1 });
  return { t8b0: b };
}

// ---------------------------------------------------------------- 两个实现的取数

function get(port, p) {
  return new Promise((resolve) => {
    const req = http.get({ host: "127.0.0.1", port, path: p, timeout: 3000 }, (res) => {
      let b = "";
      res.setEncoding("utf8");
      res.on("data", (c) => (b += c));
      res.on("end", () => resolve({ code: res.statusCode, body: b }));
    });
    req.on("timeout", () => { req.destroy(); resolve(null); });
    req.on("error", () => resolve(null));
  });
}

// server.mjs 的 /turns：起服务 → 取数 → 杀掉（务必 finally 收尾，断言失败也不留进程）
async function jsTurns(dbFile, port, v2dir) {
  const child = spawn(process.execPath, [path.join(REPO_ROOT, "server", "server.mjs")], {
    env: { ...process.env, TPS_FOOTER_PORT: String(port), TPS_FOOTER_DB: fwd(dbFile), TPS_FOOTER_V2_DIR: fwd(v2dir) },
    stdio: ["ignore", "ignore", "pipe"],
    windowsHide: true,
  });
  let stderr = "";
  child.stderr.on("data", (c) => (stderr += c));
  try {
    const t0 = Date.now();
    let up = false;
    while (Date.now() - t0 < 20000) {
      const h = await get(port, "/healthz");
      if (h && h.code === 200) { up = true; break; }
      if (child.exitCode !== null) break;
      await new Promise((r) => setTimeout(r, 200));
    }
    if (!up) throw new Error(`服务未就绪（port=${port} exit=${child.exitCode}）: ${stderr.slice(0, 400)}`);
    const r = await get(port, "/turns?limit=500");
    if (!r || r.code !== 200) throw new Error(`/turns 失败: ${r && r.code} ${r && r.body?.slice(0, 200)}`);
    return JSON.parse(r.body).turns;
  } finally {
    child.kill();
    for (let i = 0; i < 20 && child.exitCode === null; i++) await new Promise((r) => setTimeout(r, 100));
    if (child.exitCode === null) child.kill("SIGKILL");
  }
}

// remote_fold.py 的 fold()：stdout 必须是纯 JSON（末行），取整行解析
function pyTurns(dbFile) {
  const res = spawnSync("uv", ["run", "python", path.join(REPO_ROOT, "server", "remote_fold.py")], {
    env: { ...process.env, TPS_FOOTER_DB: fwd(dbFile) },
    encoding: "utf8",
    windowsHide: true,
  });
  if (res.error) throw new Error(`uv run python 起不来: ${res.error.message}`);
  if (res.status !== 0) throw new Error(`remote_fold.py rc=${res.status}: ${(res.stderr || "").slice(0, 400)}`);
  const lines = String(res.stdout).trim().split("\n");
  const data = JSON.parse(lines[lines.length - 1]);
  if (data.error) throw new Error(`remote_fold.py 报错: ${data.error}`);
  return data.turns;
}

// ---------------------------------------------------------------- 断言集

const liveOf = (turns, sid) => turns.find((t) => t.live && t.session_id === sid);
const byTid = (turns, tid) => turns.find((t) => t.turn_id === tid);

function assertSegments(impl, turns) {
  // T1：长流式空洞回并 —— 起点/步数恢复整轮真值（修复前：起点 = 洞后首 marker、步数只剩尾段）
  const t1 = liveOf(turns, S.t1);
  if (!t1) check(`[${impl}] T1 live 条目存在`, false, "长流式轮整条未发射或被腰斩");
  else {
    eqFields(`[${impl}] T1`, t1, {
      turn_id: "T1", msg_id: "user-t1", session_id: S.t1, status: "running", live: true, phase: "generating",
      start_ms: T.t1b, gen_start_ms: NOW - 30_000, gap_start_ms: null, ttft_ms: 3000, tps: 15.12, out_tokens: 50700,
      turn_no: 1, steps: 6, steps_settled: 5, models: ["GLM-5.3-Flash"], query_source: "main_turn",
      ctx_tokens: 3200, ctx_provider: CODE_PLAN, tool: null, tool_start_ms: null, tool_pending: false,
    }, ["end_ms", "run_ms"]);
  }

  // T2：崩溃残留不得并进新轮
  const t2 = liveOf(turns, S.t2);
  if (!t2) check(`[${impl}] T2 live 条目存在`, false, "新轮被误并/被丢弃");
  else {
    eqFields(`[${impl}] T2`, t2, {
      turn_id: "TW", msg_id: "user-t2b", start_ms: T.t2m3, steps: 2, steps_settled: 1, turn_no: 1, phase: "generating",
    }, ["end_ms", "run_ms", "gen_start_ms"]);
    check(`[${impl}] T2 起点未被并到旧轮`, t2.start_ms !== NOW - 3 * HOUR, `start_ms=${t2.start_ms}`);
  }

  // T3：长工具空洞（工具完成紧贴下一步 marker）→ 桥接回并
  const t3 = liveOf(turns, S.t3);
  if (!t3) check(`[${impl}] T3 live 条目存在`, false, "长工具轮整条未发射");
  else eqFields(`[${impl}] T3`, t3, { turn_id: "TT", msg_id: "user-t3", start_ms: T.t3b, steps: 2, steps_settled: 1, phase: "generating" }, ["end_ms", "run_ms", "gen_start_ms"]);

  // T4：尾步 12min 在飞 → 发射；3h 前 → 不发射
  const t4a = liveOf(turns, S.t4a);
  if (!t4a) check(`[${impl}] T4a 12min 在飞尾步仍发射`, false, "长流式期 DB 无写入时被误杀");
  else eqFields(`[${impl}] T4a`, t4a, { turn_id: null, msg_id: "user-t4a", start_ms: NOW - 12 * MIN, steps: 1, steps_settled: 0, ttft_ms: null, tps: null, out_tokens: 0, ctx_tokens: null, ctx_provider: null, phase: "generating" }, ["end_ms", "run_ms", "gen_start_ms"]);
  check(`[${impl}] T4b 3h 前的未结算尾步不发射（幽灵窗口上限）`, !liveOf(turns, S.t4b));

  // T5：running 工具 70min → 仍发射且 phase=tool（工具窗已统一到 LIVE_MAX_MS）
  const t5 = liveOf(turns, S.t5);
  if (!t5) check(`[${impl}] T5 长工具轮仍发射`, false, "running 工具出窗 → 熄火");
  else eqFields(`[${impl}] T5`, t5, { turn_id: "TV", phase: "tool", tool: "Bash", tool_start_ms: T.t5ToolStart, tool_pending: false, start_ms: NOW - 80 * MIN, steps: 1 }, ["end_ms", "run_ms", "gap_start_ms"]);
}

// T6 走已结算轮：ctx_window 只有本地/注入侧解析（py 侧只导出 ctx_tokens/ctx_provider）
function assertCtxWindow(impl, turns, withWindow) {
  for (const c of T6) {
    const row = byTid(turns, c.tid);
    if (!row) { check(`[${impl}] ${c.tid} 条目存在`, false, "缺条目"); continue; }
    eq(`[${impl}] ${c.tid} ctx_tokens`, row.ctx_tokens, 1100);
    eq(`[${impl}] ${c.tid} ctx_provider (${c.why})`, row.ctx_provider, c.prov);
    if (withWindow) eq(`[${impl}] ${c.tid} ctx_window (${c.why})`, row.ctx_window, c.want);
  }
}

function assertNormalTurn(impl, turns) {
  const t7 = liveOf(turns, S.t7);
  if (!t7) { check(`[${impl}] T7 live 条目存在`, false, "正常轮未发射"); return; }
  eqFields(`[${impl}] T7`, t7, {
    turn_id: "TN", msg_id: "user-t7", session_id: S.t7, status: "running", live: true, phase: "generating",
    start_ms: T.t7b, gen_start_ms: NOW - 5 * MIN, gap_start_ms: null, ttft_ms: 2000, tps: 26.67, out_tokens: 1200,
    turn_no: 1, steps: 3, steps_settled: 2, models: ["deepseek-flash"], query_source: "subagent",
    ctx_tokens: 11700, ctx_provider: "deepseek", tool: null, tool_start_ms: null, tool_pending: false,
  }, ["end_ms", "run_ms"]);
  if (impl === "js") eq(`${impl} T7 ctx_window`, t7.ctx_window, 1000000);
  eqShape(`[${impl}] T7 live 字段集与修复前一致（+Fix B 的 ctx_provider）`, t7, impl === "js" ? SHAPE_JS_LIVE : SHAPE_PY_LIVE);
  const closed = byTid(turns, "T6A");
  eqShape(`[${impl}] 已结算轮字段集与修复前一致（+Fix B 的 ctx_provider）`, closed, impl === "js" ? SHAPE_JS_SETTLED : SHAPE_PY_SETTLED);
}

// 形状锁：修复前实测形状 + Fix B 新增的 ctx_provider（--dump 可重新打印）
const SHAPE_JS_LIVE = [
  "turn_id", "msg_id", "session_id", "status", "live", "phase", "start_ms", "end_ms", "run_ms", "gen_start_ms",
  "gap_start_ms", "ttft_ms", "tps", "out_tokens", "turn_no", "steps", "steps_settled", "models", "query_source",
  "ctx_tokens", "ctx_window", "ctx_provider", "tool", "tool_start_ms", "tool_pending",
];
const SHAPE_PY_LIVE = [
  "turn_id", "msg_id", "session_id", "status", "live", "phase", "start_ms", "end_ms", "run_ms", "gen_start_ms",
  "gap_start_ms", "ttft_ms", "tps", "out_tokens", "turn_no", "steps", "steps_settled", "models", "query_source",
  "ctx_tokens", "ctx_model", "ctx_provider", "tool", "tool_start_ms", "tool_pending",
];
const SHAPE_JS_SETTLED = [
  "turn_id", "msg_id", "session_id", "status", "start_ms", "end_ms", "run_ms", "ttft_ms", "tps", "out_tokens",
  "turn_no", "steps", "models", "query_source", "ctx_tokens", "ctx_window", "ctx_provider",
];
const SHAPE_PY_SETTLED = [
  "turn_id", "msg_id", "session_id", "status", "start_ms", "end_ms", "run_ms", "ttft_ms", "tps", "out_tokens",
  "turn_no", "steps", "models", "query_source", "ctx_tokens", "ctx_model", "ctx_provider",
];

function assertLegacy(impl, turns, fx) {
  const live = liveOf(turns, "sess-t8");
  if (!live) check(`[${impl}] T8 老库 live 条目仍发射`, false, "老 schema 下统计整体消失");
  else eqFields(`[${impl}] T8`, live, { turn_id: "TL", status: "running", start_ms: fx.t8b0, steps: 2, steps_settled: 1, ctx_tokens: null, ctx_provider: null }, ["end_ms", "run_ms", "gen_start_ms"]);
  const closed = byTid(turns, "TC");
  if (!closed) check(`[${impl}] T8 老库已结算轮仍在`, false, "缺 TC");
  else eqFields(`[${impl}] T8 已结算轮`, closed, { status: "completed", steps: 1, ctx_tokens: null, ctx_provider: null }, ["end_ms", "run_ms"]);
  // 老库没有 provider_id 列：降级后不得凭空给出窗口（本地侧 ctx_window 由水位行解析，无水位即无分母）
  if (impl === "js" && closed) eq(`${impl} T8 老库无水位 → 无 ctx_window`, closed.ctx_window, null);
}

// ---------------------------------------------------------------- 主流程

console.log(`合成库目录：${WORK}`);
fs.rmSync(WORK, { recursive: true, force: true });
fs.mkdirSync(WORK, { recursive: true });
const v2dir = buildV2dir();

const dbMain = path.join(WORK, "fixture.sqlite");
{
  const db = makeDb(dbMain);
  buildFixture(db);
  db.close();
}
const dbLegacy = path.join(WORK, "legacy.sqlite");
{
  const db = makeDb(dbLegacy, true);
  T.t8b0 = buildLegacyFixture(db).t8b0; // T8 轮首 marker 时刻
  db.close();
}

// JS 侧（本地折叠）
console.log("\n== server.mjs /turns ==");
const jsMain = await jsTurns(dbMain, PORT_MAIN, v2dir);
const jsLegacy = await jsTurns(dbLegacy, PORT_LEGACY, v2dir);
// Python 侧（远端折叠）
console.log("== remote_fold.py ==");
const pyMain = pyTurns(dbMain);
const pyLegacy = pyTurns(dbLegacy);

if (DUMP) {
  // 形状锁的来源：修复前实测键集（live 行取 T1——它在修复前也发射；已结算取 T6A）
  for (const [name, rows] of [
    ["js live T1", [liveOf(jsMain, S.t1)]],
    ["py live T1", [liveOf(pyMain, S.t1)]],
    ["js live T7", [liveOf(jsMain, S.t7)]],
    ["py live T7", [liveOf(pyMain, S.t7)]],
    ["js settled T6A", [byTid(jsMain, "T6A")]],
    ["py settled T6A", [byTid(pyMain, "T6A")]],
  ]) {
    for (const r of rows) console.log(name, "keys:", r ? Object.keys(r).join(",") : "(missing)");
  }
  console.log("js T7:", j(liveOf(jsMain, S.t7)));
  console.log("py T7:", j(liveOf(pyMain, S.t7)));
  console.log("js T1:", j(liveOf(jsMain, S.t1)));
  console.log("py T1:", j(liveOf(pyMain, S.t1)));
  console.log("js T6A:", j(byTid(jsMain, "T6A")));
  console.log("live sids(js):", jsMain.filter((t) => t.live).map((t) => `${t.session_id}:${t.steps}`).join(" "));
}

console.log("\n== JS 侧断言 ==");
assertSegments("js", jsMain);
assertCtxWindow("js", jsMain, true);
assertNormalTurn("js", jsMain);
assertLegacy("js", jsLegacy, T);

console.log("\n== Python 侧断言（两 fold 必须同构）==");
assertSegments("py", pyMain);
assertCtxWindow("py", pyMain, false);
assertNormalTurn("py", pyMain);
assertLegacy("py", pyLegacy, T);

if (fails.length) {
  console.log(`\nFOLD-SEGMENTS-TEST-FAIL (${fails.length} 项)`);
  process.exit(1);
}
console.log("\nFOLD-SEGMENTS-TEST-PASS");
