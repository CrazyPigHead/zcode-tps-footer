// 两份 /turns 快照（改动前后各抓一份）的规范化对比：非 live 条目逐键严格相等；
// live 条目稳定键严格相等、时变键按捕获间隙容忍。live 的操作态（phase/tool/
// gen_start_ms/gap_start_ms/steps_settled/tool_*）随落库节奏秒级变化，不参与
// 严格对比；steps 容忍 ±1 步。
import fs from "node:fs";

const A = JSON.parse(fs.readFileSync(process.argv[2], "utf8")).turns;
const B = JSON.parse(fs.readFileSync(process.argv[3], "utf8")).turns;

// live 条目 turn_id 可能为 null（首步尚未落任何 settle 的 marker-only 轮），退化用会话+消息键
const keyOf = (t) => t.turn_id || (t.live ? `live:${t.session_id}:${t.msg_id}` : t.turn_id);
const liveStableKeys = ["msg_id", "session_id", "status", "live", "start_ms", "turn_no", "models", "ttft_ms"];
const issues = [];

if (A.length !== B.length) issues.push(`COUNT old=${A.length} new=${B.length}`);

const mapB = new Map(B.map((t) => [keyOf(t), t]));
const mapA = new Map(A.map((t) => [keyOf(t), t]));

for (const a of A) {
  const b = mapB.get(keyOf(a));
  if (!b) { issues.push(`MISSING(new): ${keyOf(a)}${a.live ? " live" : ""}`); continue; }
  if (!!a.live !== !!b.live) { issues.push(`LIVE-FLAG ${keyOf(a)}: old=${!!a.live} new=${!!b.live}`); continue; }
  const keys = a.live ? liveStableKeys.filter((k) => a[k] !== undefined) : Object.keys(a);
  for (const k of keys) {
    // models 的收集顺序（按 started_at 首见）对多模型轮无语义，排序后对比（显示层用 / 连接）
    const norm = (v) => (k === "models" && Array.isArray(v) ? [...v].sort() : v);
    if (JSON.stringify(norm(a[k])) !== JSON.stringify(norm(b[k]))) {
      issues.push(`DIFF ${keyOf(a).slice(0, 18)} ${a.live ? "(live) " : ""}${k}: old=${JSON.stringify(a[k])} new=${JSON.stringify(b[k])}`);
    }
  }
  if (a.live) {
    if (Math.abs((b.steps || 0) - (a.steps || 0)) > 1) issues.push(`VOLATILE ${keyOf(a)} steps ${a.steps}->${b.steps}`);
    if ((b.out_tokens || 0) < (a.out_tokens || 0)) issues.push(`VOLATILE ${keyOf(a)} out_tokens ${a.out_tokens}->${b.out_tokens}`);
    if ((b.tps || 0) && (a.tps || 0) && Math.abs((b.tps || 0) - (a.tps || 0)) > 15) issues.push(`VOLATILE ${keyOf(a)} tps ${a.tps}->${b.tps}`);
    if ((b.run_ms || 0) + 1000 < (a.run_ms || 0)) issues.push(`VOLATILE ${keyOf(a)} run_ms ${a.run_ms}->${b.run_ms}`);
  }
}
for (const b of B) if (!mapA.has(keyOf(b))) issues.push(`EXTRA(new): ${keyOf(b)}${b.live ? " live" : ""}`);

if (issues.length) {
  console.log(`FAIL (${issues.length} issues)`);
  issues.slice(0, 40).forEach((i) => console.log(" -", i));
  process.exit(1);
} else {
  console.log(`PASS: ${A.length} turns 一致（live=${A.filter(t => t.live).length}）`);
}
