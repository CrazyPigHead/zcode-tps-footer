#!/usr/bin/env node
/** TPS 数据服务的常驻监视器：owning 数据服务子进程，死亡即捕获现场并秒级重拉。
 *
 * 为什么需要它（2026-10-08 排查结论）：监督者的看门狗寄生在「活跃的本地 MCP 连接」
 * 里，Remote SSH 会话的 MCP 连接发生在远端、本地不产生监督者；本地会话空闲时其
 * MCP 连接也会断开。于是纯远程工作时段服务死了没人拉——实测死透过 8 分钟和 2 小时
 * （切换工作区建立新 MCP 连接才被拉起，用户看到的就是「子智能体统计行时有时无」）。
 * 监视器由监督者/安装脚本 detached 拉起，与 MCP 连接生命周期完全解耦，常驻到关机。
 *
 * 职责与边界：
 *   - 单例仲裁：bind 127.0.0.1:MONITOR_PORT，被占说明已有一只监视器，静默退场
 *    （与数据服务的端口仲裁同款；多只监视器并存无害但没必要）；
 *   - 数据服务作为本进程子进程运行（stdio 管道）：退出码/信号 + stderr 尾部落
 *     logs/monitor.log——历史上服务由 stdio:ignore 的父进程拉起，V8 致命错误/
 *     未处理 rejection 全部进了黑洞，「无声死亡」连一行线索都不留；
 *   - 每 5 秒探 healthz：正常不动；失联则重拉，持续失联按 1→2→4…60s 退避；
 *     自己拉起的子进程活着但 20 秒后仍不服务（事件循环挂死）则先杀再拉；
 *   - 闲置退场：healthz 应答里的 idle_s 反映「数据面多久没人用了」。渲染层在世
 *     时 inject 每秒取数，idle_s 持续归零；ZCode 关闭后 /turns 断流，idle_s 走高
 *     （远端轮询 10 分钟起完全挂起，服务零流量）。连续 IDLE_CONFIRM_ROUNDS 轮
 *     （间隔 CHECK_S，要求跨越服务自身 SUSPEND_AFTER_S=600s 的挂起点且再留
 *     IDLE_EXTRA_S 观察期，防挂起瞬间的抖动误判）都确认闲置 → 杀掉服务、退出
 *     自己——「ZCode 关闭一段时间后所有后台进程退干净」。下次任何 ZCode 会话
 *     建立 MCP 连接，监督者发现监视器不在岗会补拉，守护链原样恢复；
 *   - 服务不是自己的子进程（安装脚本/别的监视器拉的）也照常守护：healthz 通就
 *     不理，不通就自己拉一只——端口 bind 竞争由数据服务自行仲裁，输家退出；
 *     （连带推论：闲置退场把「别人的服务」也一并回收——卸载/另装场景下 leftover
 *     服务本就该清；真有外部消费方时它每几秒的请求会让 idle_s 归零，退场永不触发）
 *   - 只用 node 标准库（http/child_process/fs），绝不 import node:sqlite——
 *     监视器自己没有可崩溃的面。
 *
 * 环境变量（与数据面同源，正常无需设置）：
 *   TPS_FOOTER_PORT   数据服务端口（默认 3117）
 *   TPS_MONITOR_PORT  监视器仲裁端口（默认数据端口+1）
 *   TPS_IDLE_EXIT_S   闲置退场阈值秒数（默认 1200；测试可调小加速验证）
 * 运行时文件：logs/monitor.log（SPAWN/EXIT/crash 现场，14 天由数据面日志清理顺带不管，
 * 本文件量极小，按 512KB 截顶防无界增长）；logs/monitor.state（闲置退场时间戳标记，
 * 监督者看门狗的冷却依据——防「闲置 CLI 会话不取数」场景下看门狗把刚退场的后台
 * 拉锯式反复拉起）。
 */
import { spawn } from "node:child_process";
import { createServer } from "node:http";
import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const HERE = path.dirname(fileURLToPath(import.meta.url)); // <仓库>/server
const ROOT = path.dirname(HERE); // 仓库根
const SERVER = path.join(HERE, "server.mjs");
const LOG_PATH = path.join(ROOT, "logs", "monitor.log");
const LOG_MAX_BYTES = 512 * 1024;
const STATE_PATH = path.join(ROOT, "logs", "monitor.state"); // 闲置退场标记（监督者看门狗读）

const PORT = Number(process.env.TPS_FOOTER_PORT) || 3117;
const MPORT = Number(process.env.TPS_MONITOR_PORT) || PORT + 1;
const CHECK_S = 5;
const START_GRACE_S = 20; // 子进程活着但 healthz 不通的宽限：覆盖冷启动，超过即视为挂死
const MAX_BACKOFF_S = 60;
// 闲置退场：idle_s 超过该值即计一轮闲置确认，连续 IDLE_CONFIRM_ROUNDS 轮（每轮
// CHECK_S 秒）都确认才动手——校验跨多轮，防瞬时抖动误判。1200s = 服务自身
// SUSPEND_AFTER_S(600s，远端轮询完全挂起点) + 600s 观察期：ZCode 开着时 inject
// 每秒取数，idle_s 到不了两位数，此门槛不影响任何在用会话。设 0 或负数 = 禁用
// 闲置退场（常驻语义，与旧版行为一致）。
const IDLE_EXIT_S = process.env.TPS_IDLE_EXIT_S !== undefined ? Number(process.env.TPS_IDLE_EXIT_S) : 1200;
const IDLE_CONFIRM_ROUNDS = 2;

function ts() {
  const d = new Date();
  const p = (n, w = 2) => String(n).padStart(w, "0");
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}

function log(msg) {
  try {
    fs.appendFileSync(LOG_PATH, `${ts()} ${msg}\n`, "utf8");
    // 量极小，超限粗暴截半防无界（监视器自身绝不因日志问题死亡）
    try {
      const st = fs.statSync(LOG_PATH);
      if (st.size > LOG_MAX_BYTES) {
        const lines = fs.readFileSync(LOG_PATH, "utf8").split("\n");
        fs.writeFileSync(LOG_PATH, lines.slice(Math.floor(lines.length / 2)).join("\n"), "utf8");
      }
    } catch {
      /* 截顶失败忽略 */
    }
  } catch {
    /* 日志失败不致命 */
  }
}

const sleep = (ms) => new Promise((res) => setTimeout(res, ms));

// 超时给足 3s：healthz 与 /turns 共用同一事件循环，本地大库上单次折叠完全可能
// 超过 1.5s——探测超时 ≠ 挂死，挂死判定另有宽限 + 连续失败门槛兜着。
// 解析 JSON 应答体取 idle_s（数据面 ≥ 此版本在 /healthz 里上报闲置状态；解析不了
// 视为 idle 0，兼容旧版纯文本 "ok" 应答——最多不触发闲置退场，不会误退场）。
async function probe(timeoutMs = 3000) {
  return new Promise((resolve) => {
    const req = http.get({ host: "127.0.0.1", port: PORT, path: "/healthz", timeout: timeoutMs }, (res) => {
      if (res.statusCode !== 200) {
        res.resume();
        return resolve({ up: false, idleS: 0 });
      }
      let body = "";
      res.setEncoding("utf8");
      res.on("data", (c) => (body += c));
      res.on("end", () => {
        let idleS = 0;
        try {
          const j = JSON.parse(body);
          if (typeof j.idle_s !== "number" || !Number.isFinite(j.idle_s)) idleS = 0;
          else idleS = Math.max(0, j.idle_s);
        } catch {
          /* 旧版 "ok" 文本应答：视为 0 */
        }
        resolve({ up: true, idleS });
      });
      res.on("error", () => resolve({ up: false, idleS: 0 }));
    });
    req.on("timeout", () => {
      req.destroy();
      resolve({ up: false, idleS: 0 });
    });
    req.on("error", () => resolve({ up: false, idleS: 0 }));
  });
}

function healthz(timeoutMs = 3000) {
  return probe(timeoutMs).then((r) => r.up);
}

// ---------------------------------------------------------------- 服务子进程

let child = null; // { proc, startedAt, errTail }
const ERR_TAIL_MAX = 64 * 1024; // stderr 环形缓冲上限，落日志时再截 4KB

function spawnService() {
  const proc = spawn(process.execPath, [SERVER], {
    stdio: ["ignore", "pipe", "pipe"], // stdout/stderr 都要：V8 致命错误走 stderr
    windowsHide: true,
    cwd: ROOT,
  });
  const st = { proc, startedAt: Date.now(), errTail: "" };
  child = st;
  proc.stdout.on("data", () => {}); // 数据面不写 stdout，读到也丢弃
  proc.stderr.setEncoding("utf8");
  proc.stderr.on("data", (c) => {
    st.errTail = (st.errTail + c).slice(-ERR_TAIL_MAX);
  });
  proc.on("error", (e) => {
    log(`[SPAWN-ERR] ${e && e.message}`);
    if (child === st) child = null;
  });
  proc.on("exit", (code, signal) => {
    const upS = Math.round((Date.now() - st.startedAt) / 1000);
    const tail = st.errTail.trim().slice(-4096);
    log(`[SERVICE-EXIT] pid=${proc.pid} code=${code} signal=${signal} 运行${upS}s${tail ? ` stderr尾部:\n${tail}` : "（stderr 无输出）"}`);
    if (child === st) child = null;
  });
  log(`[SPAWN-SERVICE] pid=${proc.pid} port=${PORT}`);
  return st;
}

async function killChild() {
  if (!child) return;
  const c = child;
  try {
    c.proc.kill();
  } catch {
    /* 已退出 */
  }
  // once：HANG-KILL 反复重试的病态场景下不再叠加监听器
  await new Promise((resolve) => {
    const t = setTimeout(resolve, 3000);
    c.proc.once("exit", () => {
      clearTimeout(t);
      resolve();
    });
  });
}

// 按端口属主杀掉数据服务（闲置退场用）：服务不是本进程子进程时（安装脚本/别的
// 守护者拉的），killChild 够不着，靠 netstat 找 127.0.0.1:PORT 的 LISTEN 属主。
// 双重身份核对后动手：映像是 node 且命令行含本仓库 server.mjs（Get-CimInstance，
// tasklist 无命令行、wmic 在新 Windows 已移除）；本地地址按列全等比较，排除
// 127.0.0.1:3117x 这类前缀端口的子串误命中。核对不过 = 绝不动手。
function killPortOwner() {
  return new Promise((resolve) => {
    const netstat = spawn("netstat", ["-aon", "-p", "TCP"], { stdio: ["ignore", "pipe", "ignore"], windowsHide: true });
    let out = "";
    netstat.stdout.setEncoding("utf8");
    netstat.stdout.on("data", (c) => (out += c));
    netstat.on("error", () => resolve(false));
    netstat.on("close", () => {
      const pids = new Set();
      const needle = `127.0.0.1:${PORT}`;
      for (const line of out.split("\n")) {
        // TCP    127.0.0.1:3117    0.0.0.0:0    LISTENING    12345
        const cols = line.trim().split(/\s+/);
        if (cols.length >= 5 && cols[3] === "LISTENING" && cols[1] === needle) {
          const pid = Number(cols[4]);
          if (pid > 0) pids.add(pid);
        }
      }
      if (!pids.size) return resolve(false);
      // settled 防双计：spawn 失败时 error 与 close 都会触发（close 携带负 errno），
      // 同一 pid 只允许结算一次，否则 pending 被多减
      let pending = pids.size;
      let killed = false;
      const settled = new Set();
      const done = (pid) => {
        if (settled.has(pid)) return;
        settled.add(pid);
        if (--pending === 0) resolve(killed);
      };
      const serverPathPs = SERVER.replace(/'/g, "''");
      for (const pid of pids) {
        // 退出码约定：0 = 已杀；3 = 身份核对不过（不杀）；其他 = PowerShell 自身出错。
        // 不能裸靠 0/非 0：条件不成立时脚本同样正常结束（exit 0），会把「没杀」谎报成「已回收」
        const ps = spawn(
          "powershell",
          ["-NoProfile", "-Command",
            `$p = Get-CimInstance Win32_Process -Filter "ProcessId = ${pid}"; if ($p -and $p.Name -like 'node*' -and $p.CommandLine -and $p.CommandLine.IndexOf('${serverPathPs}', [StringComparison]::OrdinalIgnoreCase) -ge 0) { Stop-Process -Id ${pid} -Force -ErrorAction Stop; exit 0 } else { exit 3 }`],
          { stdio: ["ignore", "ignore", "ignore"], windowsHide: true }
        );
        ps.on("error", () => done(pid));
        ps.on("close", (code) => {
          if (code === 0) {
            killed = true;
            log(`[IDLE-EXIT] 已回收外部数据服务 pid=${pid}（${PORT} 端口属主，命令行含本仓库 server.mjs）`);
          } else if (code === 3) {
            log(`[IDLE-EXIT] 端口属主 pid=${pid} 未通过身份核对（非 node 或命令行不含本仓库 server.mjs），不动它`);
          } else {
            log(`[IDLE-EXIT] 端口属主 pid=${pid} 的回收命令异常退出（code=${code}），请手动确认`);
          }
          done(pid);
        });
      }
    });
  });
}

// ---------------------------------------------------------------- 主循环

async function mainLoop() {
  let consecFail = 0; // 服务连续「不存在且不健康」的轮数：驱动重拉退避
  let hangMisses = 0; // 子进程存活但 healthz 失败的连续轮数：单次超时可能是大库折叠
  //（同步查询阻塞事件循环数秒），连续 2 轮（间隔 5s，即挂死 10s+ 且已过启动宽限）
  // 才判真挂死，避免周期性误杀「慢而健康」的服务
  let idleRounds = 0; // 连续确认「闲置超阈值」的轮数：跨多轮确认才退场，防抖动
  let seenExternal = false; // 上一轮确认过「外部服务在岗」（child 为 null 但 healthz 通）
  for (;;) {
    try {
      const st = await probe();
      if (st.up) {
        consecFail = 0;
        hangMisses = 0;
        if (!child) seenExternal = true;
        // 闲置退场判定：数据面报告 idle_s 超阈值，且连续 IDLE_CONFIRM_ROUNDS 轮
        // 一致（CHECK_S 间隔，两轮间任何 /turns 请求都会把 idle_s 打回去）。
        // 服务不是自己拉的情形（外部消费方/别的守护者拉的服务）idle_s 同样归零，
        // 不会误杀在用的服务。IDLE_EXIT_S ≤ 0 = 禁用（常驻语义）
        if (IDLE_EXIT_S > 0 && st.idleS >= IDLE_EXIT_S) {
          idleRounds += 1;
          if (idleRounds >= IDLE_CONFIRM_ROUNDS) {
            log(`[IDLE-EXIT] 数据面闲置 ${st.idleS}s（阈值 ${IDLE_EXIT_S}s，连续 ${idleRounds} 轮确认），回收服务并退出——下次 ZCode 会话连接时由监督者重新拉起`);
            // 退场标记：存活的监督者看门狗读到它就不在冷却窗口内补拉（否则「闲置 CLI
            // 会话不取数」场景会拉锯：看门狗 2 分钟拉起 → 20 分钟又退场，无限循环）
            try {
              fs.writeFileSync(STATE_PATH, JSON.stringify({ last_idle_exit: Date.now(), idle_exit_s: IDLE_EXIT_S }), "utf8");
            } catch {
              /* 写不进标记只是退回到无冷却语义 */
            }
            if (child) await killChild();
            // 服务不是自己的子进程（外部拉的）：按端口属主兜底回收，与退场语义
            // 配套——「退干净」不能只退自己人。拿不到属主也不阻塞退出（此时服务
            // 大概率已死）
            if (await healthz()) await killPortOwner();
            // 退出竞态兜底：确认到动手之间有数秒窗口，若用户恰好此时重开 ZCode
            //（新请求把 idle_s 打回去/服务已被外部重新拉起），中止退场回到守护，
            // 避免刚要被使用的服务被杀出 ~2 分钟统计行空窗
            const fin = await probe();
            if (fin.up && fin.idleS < IDLE_EXIT_S) {
              log(`[IDLE-ABORT] 退场窗口内收到新请求（idle_s=${fin.idleS}），中止退场继续守护`);
              if (!child && !(await healthz())) spawnService();
              idleRounds = 0;
            } else {
              process.exit(0);
            }
          }
        } else {
          idleRounds = 0;
        }
      } else {
        idleRounds = 0;
        if (child && Date.now() - child.startedAt > START_GRACE_S * 1000) {
          hangMisses += 1;
          if (hangMisses >= 2) {
            log(`[HANG-KILL] pid=${child.proc.pid} 过启动宽限后连续 ${hangMisses} 轮不响应 healthz，杀掉重拉`);
            await killChild();
            hangMisses = 0;
          }
        }
        if (!child) {
          // 外部服务（安装脚本/别的守护者拉的）：本轮失联前若刚确认过在岗，先按
          // 单次超时处理（大库折叠可阻塞事件循环数秒），连续 2 轮失联才判真死，
          // 与子进程的 hangMisses 语义对称——避免瞬时超时触发一次必输的 bind 竞争
          // 白拉（exit 1 还会在日志里留下貌似崩溃的现场）
          const externalSeen = seenExternal;
          seenExternal = false;
          consecFail += 1;
          if (externalSeen && consecFail < 2) {
            log(`[DOWN] 外部服务探活超时（第 1 次，可能是大库折叠阻塞），下轮复查再定`);
          } else {
            const backoff = Math.min(2 ** Math.min(consecFail - 1, 6), MAX_BACKOFF_S);
            log(`[DOWN] healthz 失败（连续第 ${consecFail} 次），${backoff}s 后重拉服务`);
            await sleep(backoff * 1000);
            spawnService();
          }
        }
      }
    } catch (e) {
      log(`[MON-ERR] ${e && e.message}`);
    }
    await sleep(CHECK_S * 1000);
  }
}

// ---------------------------------------------------------------- 启动：单例仲裁
//
// bind 仲裁端口失败的两条出路，都不能让「3118 被占」变成服务永远起不来的单点故障：
//   - 服务在运行 → 端口持有者就是另一只监视器（或任何守护者在岗），本实例退场；
//   - 服务没在运行 → 端口被无关进程占用，此时退出 = 无人拉服务——降级为无仲裁
//     模式照常守护。降级的代价：可能与别的监视器并存，重复拉起由数据面自身的
//     bind 竞争兜底（输家退出），最坏每分钟多一次白拉的进程开销。

fs.mkdirSync(path.dirname(LOG_PATH), { recursive: true });
const arbiter = createServer(() => {});
arbiter.on("error", async (e) => {
  if (await healthz()) {
    log(`[ARBITER-BUSY] 端口 ${MPORT} 被占且服务在运行（已有守护者在岗，${e && e.code}），本实例退出`);
    process.exit(0);
  }
  // 确认窗：并发冷启动时（多会话同时恢复），仲裁赢家拉的服务可能还在 1~2s 冷启动
  // 窗口里（bind 前），立刻判「服务未运行」会把自己错降级成无仲裁模式——双监视器
  // 长期并存，闲置退场要两个周期才退净。等一拍再探，赢家服务起来就正常退场
  await sleep(12 * 1000);
  if (await healthz()) {
    log(`[ARBITER-BUSY] 端口 ${MPORT} 被占，12s 确认窗后服务已在运行（并发冷启动的赢家），本实例退出`);
    process.exit(0);
  }
  log(`[ARBITER-BUSY] 端口 ${MPORT} 被占（${e && e.code}）且服务持续未运行，降级为无仲裁守护模式`);
  mainLoop();
});
arbiter.listen(MPORT, "127.0.0.1", () => {
  log(`[START] monitor pid=${process.pid} node=${process.version} service=${SERVER} port=${PORT} arbiter=${MPORT} idle_exit=${IDLE_EXIT_S}s×${IDLE_CONFIRM_ROUNDS}`);
  mainLoop();
});
