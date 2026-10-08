/**
 * ZCode 每答统计行注入脚本（Windows + Remote SSH 版）
 *
 * 样式：`14:05 · 第9轮·4步 · 终端 运行中 45秒 · 用时 3分04秒 · 首 token 4.2秒 · 51 tok/s · GLM-5.3`
 * 时长分级（fmtDur）：秒 → 分+秒 → 时+分+秒 → 天+时+分+秒 → 年+天+时+分+秒（年按 365 天折算）
 * 子会话（subagent/workflow_child）条目在模型名前多一段 `上下文 126K (63%)`——官方容量表
 * 只挂在主会话输入栏工具条（composer 工具条），只读子面板没有 composer 摆不下，这里补上。
 *
 * 实时：轮次进行中，数据服务按 marker（message⋈step-start）/ settle（model_usage）/
 * tool（tool_usage）三线落库事实合成 live:true 条目（phase = tool|generating|gap），
 * 统计行以 `第N轮·M步 · 思考/工具计时 · 用时…` 逐秒刷新；步数在请求发出时 +1，
 * 与轮末 turn_usage.model_request_count 守恒，轮次结束后被最终值原地覆盖
 * （同一条 DOM，只换文本）。
 *
 * 挂载方式：安装脚本在 app.asar 渲染层 index.html 加 <script> 标签，直接指向仓库内的
 * 本文件（无部署副本；仓库目录须持久保留），改完重启 ZCode 生效。
 * 数据源：http://127.0.0.1:3117/turns（本地库 + SSH 远端库合并，按 turn_id 全局唯一直接桥接，
 * 无需区分消息来自哪个会话/主机）。带 since 水位增量取数：稳态响应只有 live 条目与新增轮
 * （几 KB），本地按键合并；失败或连续零匹配自动归零水位回全量。
 * 定位锚：<section data-turn-id="...">（虚拟滚动，滚到哪渲染哪；进行中的最后一轮
 * 常驻 live-tail 区域，不会因滚动卸载）。取值随版本可能是 turn_<uuid> 或用户消息 ID
 * （msg_xxx），两种都匹配。
 * 设计约束：任何异常静默吞掉，绝不影响主界面；React 若删掉注入节点，observer 会重画。
 *
 * 零布局足迹（注入自身任何时刻不改变宿主布局/不叠加可见闪变；「侧边栏闪烁」经
 * 注入开关 A/B 对照已确认是 ZCode 原生渲染行为——滚动渐隐 mask 全层重绘、虚拟测高
 * 估算→实测跳变等，与注入无关）：
 *  1) 零布局 delta：统计行绝对定位，覆盖在 section 固有的 pb-5（20px）空间内，追加/走字/
 *     轮末覆盖任何时刻都不改变 section 高度——时间线对每个轮次做动态测高
 *     （TanStack measureElement），流内追加一行会带来 ~40px 延迟增量触发重新测高与吸底重滚。
 *  2) 同帧补画：React 重挂载（轮末 live→history 迁移、虚拟窗口挂卸）销毁注入行时，
 *     observer 回调（微任务，先于本帧上屏）立即用缓存数据重画——行与 section 本体同帧
 *     出现，无「缺线一帧」空窗。
 *  3) 零动画：所有路径一律即画即终态（opacity 0.55，无过渡、无定时器）。实测（logs 里
 *     新轮首扫必 NOMATCH：section 先出现、数据 1~2s 后才落库）每条新线都走「数据晚到」
 *     补画，淡入意味着每个轮次一次可见的透明度爬升，且其兜底时序在部分环境不可靠。
 *     逐秒走字同理改为文本节点 nodeValue 原地变更（textContent 整写是删节点+建节点）。
 *
 * 诊断通道（/ping，只落 logs/server.log）：
 *   secs=-2 INVISIBLE 首条线渲染后量出零尺寸（挂载策略失效信号；只查首条，逐条量是
 *                    纯强制布局开销，历史日志为空已证明挂载健康）
 *   A/B 开关：/turns 响应携带 tps 布尔（服务端 GET/POST /tps-state 控制）。false 时
 *   撤掉全部统计行并停画，true 时恢复——「界面异常是否注入引起」两秒内出结论，不用
 *   改文件重启 ZCode。字段缺失（服务未起）= 开。
 */
(() => {
  if (window.__tpsFooterLoaded) return;
  window.__tpsFooterLoaded = true;

  const API = "http://127.0.0.1:3117";
  const MARK = "data-tps-footer";
  const CLASS = "tps-footer-line";
  const SECTION_SEL = "section[data-turn-id]";
  const CACHE_MS = 2000;
  const FULL_REFETCH_MS = 5 * 60 * 1000; // 周期性全量自愈间隔（与服务端对远端的周期全量对称）

  let turns = [];
  let byMsg = new Map(); // norm(msg_id) -> turn（首见优先，列表按 end_ms 降序 = 最新优先）
  let byTurn = new Map(); // norm(turn_id) -> turn
  let fetchedAt = 0;
  let since = 0; // 服务端增量水位：>0 时 /turns 带 &since= 只取增量，失败/异常归零重拉全量
  let lastFullAt = 0; // 上次拿到全量响应的时刻（NOMATCH 全量自愈的限频锚点）
  let missStreak = 0; // 连续「整轮零匹配」的扫描次数（≥3 且离全量已久 → 疑似增量漏数据，自愈）
  let enabled = true; // 实机 A/B 开关（/turns 的 tps 字段；缺失即开）
  let visChecked = false; // 首条线可见性自检只做一次（见头部 secs=-2 注释）

  const norm = (s) => String(s || "").replace(/^turn_/, "");

  function indexTurns(list) {
    byMsg = new Map();
    byTurn = new Map();
    for (const t of list) {
      const m = norm(t.msg_id);
      if (m && !byMsg.has(m)) byMsg.set(m, t);
      const k = norm(t.turn_id);
      if (k && !byTurn.has(k)) byTurn.set(k, t);
    }
  }

  // 增量合并：与 indexTurns 同一套键（turn_id，缺失退 msg_id）。增量条目后放覆盖同键旧
  // 条目（live 轮的逐秒新版、轮末的已结算终版都靠这条路径原地换血）；同一轮的 live 与
  // 已结算版本按 turn_id/msg_id 双向去重，迁移不留双份。结果按 end_ms 降序并截顶防无界增长。
  function mergeTurns(oldTurns, inc) {
    const map = new Map();
    const put = (t) => {
      if (!t || (!t.turn_id && !t.msg_id)) return;
      const key = t.turn_id || `m:${t.msg_id}`;
      for (const [k, v] of map) {
        if (k === key) continue;
        if ((t.turn_id && v.turn_id === t.turn_id) || (t.msg_id && v.msg_id === t.msg_id)) map.delete(k);
      }
      map.set(key, t);
    };
    for (const t of oldTurns) put(t);
    for (const t of inc) put(t);
    return [...map.values()].sort((a, b) => b.end_ms - a.end_ms).slice(0, 3000);
  }

  function applyEnabled(v) {
    if (v === enabled) return;
    enabled = v;
    if (!v) {
      try {
        document.querySelectorAll(`[${MARK}]`).forEach((n) => n.remove());
      } catch {
        /* 静默 */
      }
    } else {
      setTimeout(scan, 0); // 开回来立即补画，不等下一秒兜底
    }
  }

  async function fetchTurns() {
    const ttl = turns.some((t) => t.live) ? 1000 : CACHE_MS; // 有 live 时与服务端 0.5s 折叠缓存
    // 及 scan 的 1s 周期对齐——取更密只会重复解析同一份数据
    if (Date.now() - fetchedAt < ttl) return;
    // 周期性全量：/turns 的水位混合本地与各远端的时钟域，任一源时钟超前边距（5 分钟）
    // 以上时，慢源新结算轮会被增量响应永久过滤，且 NOMATCH 自愈救不回（页面上任何旧
    // 轮匹配都会复位 missStreak）——定期归零水位强制全量，把最坏漏发窗口钉在 5 分钟内
    if (since > 0 && Date.now() - lastFullAt > FULL_REFETCH_MS) since = 0;
    try {
      // since 增量：>0 时服务端只回 live 条目 + 水位-边距内的已结算轮（几 KB），本地
      // 合并；不带 since（全量）的响应整体替换
      const r = await fetch(`${API}/turns?limit=800${since > 0 ? `&since=${since}` : ""}&_=${Date.now()}`);
      const j = await r.json();
      if (Array.isArray(j.turns)) {
        if (since === 0) {
          turns = j.turns;
          lastFullAt = Date.now();
        } else {
          turns = mergeTurns(turns, j.turns);
        }
        since = typeof j.since === "number" && j.since > 0 ? j.since : 0;
        indexTurns(turns);
        fetchedAt = Date.now();
        missStreak = 0;
      }
      if (typeof j.tps === "boolean") applyEnabled(j.tps);
    } catch {
      since = 0; // 取数异常：清水位，下次全量重拉，不在坏水位上继续增量
    }
  }

  // 五级时长，每级都精确到秒：秒 → 分+秒 → 时+分+秒 → 天+时+分+秒 →
  // 年+天+时+分+秒（按 365 天/年折算），大单位整数 + 两位补零小单位
  function fmtDur(ms) {
    const t = Math.max(0, Math.floor((ms || 0) / 1000)); // 兜底字段缺失/NaN：否则五级比较全落空，落到年级行输出 NaN年…
    if (t < 60) return `${t}秒`;
    const p = (n) => String(n).padStart(2, "0");
    const s = t % 60;
    const m = Math.floor(t / 60);
    if (m < 60) return `${m}分${p(s)}秒`;
    const h = Math.floor(m / 60);
    const mm = m % 60;
    if (h < 24) return `${h}时${p(mm)}分${p(s)}秒`;
    const d = Math.floor(h / 24);
    const hh = h % 24;
    if (d < 365) return `${d}天${p(hh)}时${p(mm)}分${p(s)}秒`;
    const y = Math.floor(d / 365);
    return `${y}年${p(d % 365)}天${p(hh)}时${p(mm)}分${p(s)}秒`;
  }
  function fmtLat(ms) {
    const s = Math.max(0, (ms || 0) / 1000);
    return s < 10 ? String(+s.toFixed(1)) : String(Math.round(s));
  }
  function fmtTps(v) {
    return v >= 10 ? String(Math.round(v)) : String(+Number(v).toFixed(1));
  }
  // 上下文水位段（仅子会话条目）：126431→"126K"、1250000→"1.25M"；分母缺失只显示 K
  function fmtCtx(tokens, win) {
    const used = tokens >= 1e6 ? `${+(tokens / 1e6).toFixed(2)}M` : `${Math.round(tokens / 1000)}K`;
    if (!(win > 0)) return `上下文 ${used}`;
    const pct = Math.min(100, Math.max(0, Math.round((tokens / win) * 100)));
    return `上下文 ${used} (${pct}%)`;
  }
  function fmtStamp(ms) {
    const d = new Date(ms);
    const now = new Date();
    const hm = `${String(d.getHours()).padStart(2, "0")}:${String(d.getMinutes()).padStart(2, "0")}`;
    if (d.toDateString() === now.toDateString()) return hm;
    const sameYear = d.getFullYear() === now.getFullYear();
    return sameYear
      ? `${d.getMonth() + 1}月${d.getDate()}日 ${hm}`
      : `${d.getFullYear()}年${d.getMonth() + 1}月${d.getDate()}日 ${hm}`;
  }

  // 工具名 → 操作标签（查得到映射用中文，mcp 前缀归 MCP，其余显示原名）
  const TOOL_LABELS = {
    Bash: "终端",
    Read: "读取",
    Grep: "检索",
    Glob: "检索",
    LS: "检索",
    Edit: "编辑",
    Write: "编辑",
    MultiEdit: "编辑",
    NotebookEdit: "编辑",
    WebFetch: "联网",
    WebSearch: "联网",
    Task: "子智能体",
    Agent: "子智能体",
    TaskOutput: "子智能体",
    TaskStop: "子智能体",
    Workflow: "工作流",
    Skill: "技能",
    TodoWrite: "清单",
    EnterPlanMode: "规划",
    ExitPlanMode: "规划",
  };
  function toolLabel(name) {
    if (!name) return "";
    if (TOOL_LABELS[name]) return TOOL_LABELS[name];
    if (String(name).startsWith("mcp__")) return "MCP";
    return name;
  }

  function opText(t, now) {
    if (t.phase === "tool") {
      return `${toolLabel(t.tool)}${t.tool_pending ? " 等待确认" : " 运行中"} ${fmtDur(Math.max(0, now - (t.tool_start_ms || now)))}`;
    }
    if (t.phase === "generating") {
      // 进行中步的真实起点（marker 落库时刻）
      return `思考 ${fmtDur(Math.max(0, now - (t.gen_start_ms || t.start_ms || now)))}`;
    }
    return `准备 ${fmtDur(Math.max(0, now - (t.gap_start_ms || now)))}`;
  }

  function lineText(t, now) {
    const parts = [fmtStamp(t.live ? t.start_ms : t.end_ms)];
    if (t.turn_no) parts.push(`第${t.turn_no}轮${t.steps ? `·${t.steps}步` : ""}`);
    if (t.live) {
      // 步内实时态（phase 由数据服务按 marker/settle/tool 三线落库事实合成）：
      // 步数在请求发出时 +1，操作名/计时全部锚在落库行上
      parts.push(opText(t, now));
    }
    parts.push(`用时 ${fmtDur(t.live ? Math.max(0, now - (t.start_ms || now)) : t.run_ms)}`);
    if (t.ttft_ms != null && t.ttft_ms >= 0) parts.push(`首 token ${fmtLat(t.ttft_ms)}秒`);
    if (t.tps) parts.push(`${fmtTps(t.tps)} tok/s`);
    // 子会话上下文水位：口径与官方一致（最后已结算步 input+output ÷ 模型上下文窗口，
    // 服务端折算），首步结算前 / 分母解析不到时不出该段；主会话本来就有容量表，不重复
    if ((t.query_source === "subagent" || t.query_source === "workflow_child") && t.ctx_tokens > 0) {
      parts.push(fmtCtx(t.ctx_tokens, t.ctx_window));
    }
    if (Array.isArray(t.models) && t.models.length) parts.push(t.models.join("/"));
    return parts.join(" · ");
  }

  function render(section, t) {
    let line = section.querySelector(`[${MARK}]`);
    // 步与步的落库间隙（亚秒级为主）：<1s 冻结上一帧文本防「思考→工具」闪变
    const frozenGap =
      t.live && t.phase === "gap" && line && line.textContent &&
      Date.now() - (t.gap_start_ms || 0) < 1000;
    const txt = frozenGap ? line.textContent : lineText(t, Date.now());
    if (!line) {
      line = document.createElement("div");
      line.setAttribute(MARK, "1");
      line.className = CLASS;
      // 绝对定位覆盖 section 固有的 pb-5（20px），行高 16px 完全落在 padding 盒内；
      // left/right 用 0 + padding-inline:inherit 与内容列对齐（px-4 / @md:px-6）并随
      // 容器断点自适应。contain 把行的布局与绘制影响封在自己盒内。即画即终态：
      // opacity 0.55，任何路径都没有过渡与定时器（防闪烁第 3 阶段）。
      line.style.cssText =
        "position:absolute;left:0;right:0;bottom:2px;height:16px;line-height:16px;" +
        "padding-inline:inherit;font-size:11px;user-select:none;white-space:nowrap;" +
        "overflow:hidden;text-overflow:ellipsis;pointer-events:none;opacity:0.55;" +
        "contain:layout paint;";
      line.textContent = txt;
      section.appendChild(line);
      if (!visChecked) {
        visChecked = true;
        requestAnimationFrame(() => {
          try {
            const r = line.getBoundingClientRect();
            if (r.height === 0 || r.width === 0)
              ping(-2, `INVISIBLE host=${section.tagName} cls=${(section.className || "").slice(0, 60)}`);
          } catch {}
        });
      }
      return;
    }
    // 已有统计行：文本变了才动 DOM，且只改文本节点的 nodeValue（原地走字，
    // 不删旧建新——textContent 整写每秒制造一次子结构变更）
    if (line.textContent !== txt) {
      const tn = line.firstChild;
      if (tn && tn.nodeType === 3) tn.nodeValue = txt;
      else line.textContent = txt;
    }
  }

  let lastSecs = -1;
  function ping(secs, extra) {
    if (secs === lastSecs && !extra) return;
    lastSecs = secs;
    try {
      let snap = "";
      if (secs === 0) {
        snap =
          [...document.body.children]
            .map(
              (e) =>
                e.tagName +
                "[" +
                [...e.attributes].map((a) => a.name + "=" + (a.value || "").slice(0, 30)).join(",") +
                "]" +
                ">" +
                e.children.length
            )
            .join(" | ")
            .slice(0, 1400);
      } else if (extra) {
        snap = extra.slice(0, 1400);
      }
      fetch(`${API}/ping?secs=${secs}&snap=${encodeURIComponent(snap)}`).catch(() => {});
    } catch {}
  }

  // section ↔ 回合桥接（scan 与 observer 同帧补画共用一套匹配逻辑，改必须同步）
  function matchTurn(sec) {
    // section[data-turn-id] 可能是用户消息 ID（msg_xxx）或 turn_<uuid>，两种都试
    //（msg_ 与 uuid 命名空间不重叠，两个 Map 各自 O(1)）
    const domId = norm(sec.getAttribute("data-turn-id"));
    let t = byMsg.get(domId) || byTurn.get(domId);
    if (!t && domId.length >= 16) {
      // 防御：DOM 属性若是截断版，退化为前缀匹配（罕见路径，仅扫 msg 键）
      for (const [m, cand] of byMsg) {
        if (m.startsWith(domId) || domId.startsWith(m)) {
          t = cand;
          break;
        }
      }
    }
    return t;
  }

  let pending = false;
  let lastExpect = -2;
  async function scan() {
    if (pending) return;
    pending = true;
    try {
      // 先取数再查 DOM：render 用的 NodeList 与数据同代。旧顺序先抓列表再等取数，
      // 等待窗口里 React 若重挂载 section，整轮就画在了已卸载节点上，只能等下轮
      await fetchTurns();
      if (!enabled) return; // A/B 关闭态：保持取数轮询开关，但不画
      const secs = document.querySelectorAll(SECTION_SEL);
      if (!secs.length) {
        ping(0);
        return;
      }
      // 首扫上报真实 DOM 的 turn-id 原值（诊断匹配用）
      if (lastSecs < 0 || secs.length !== lastExpect) {
        lastExpect = secs.length;
        ping(
          secs.length,
          "IDS " +
            [...secs]
              .slice(0, 10)
              .map((s) => (s.getAttribute("data-turn-id") || "?").slice(0, 44))
              .join(",")
        );
      } else {
        ping(secs.length);
      }
      let matched = 0,
        rendered = 0,
        missed = 0;
      secs.forEach((s) => {
        // 轮次仍需逐秒重画，见 render 注释
        const t = matchTurn(s);
        if (t) {
          // 已画过条的 section 也必须进 render：React 流式输出只 patch section 内部，
          // 不会重建 section，跳过已画条 = 统计行永远停在首次绘制的内容，只有切换
          // 会话触发整个时间线重挂载才更新。live 计时/步数逐秒刷新、轮末 live→final
          // 的原地覆盖都靠这里（render 内文本没变就不动 DOM）。
          matched++;
          render(s, t);
        } else if (s.querySelector(`[${MARK}]`)) {
          rendered++; // 有旧条但本轮数据里没匹配到（如超出 limit 的老轮）：保留旧文本
        } else {
          missed++;
        }
      });
      if (missed > 0 && matched === 0 && rendered === 0) {
        ping(-1, `NOMATCH dom=${secs.length} cache=${turns.length} first=${(secs[0].getAttribute("data-turn-id") || "?").slice(0, 44)} latest=${turns[0] ? (turns[0].turn_id || "?").slice(0, 44) : "?"}`);
        // 增量自愈：连续 3 轮整页零匹配且离上次全量超 30s → 疑似增量漏数据（水位异常），
        // 清水位下次全量。30s 限频兜住「全量也无解」的场景（如超出 24h 窗的老轮），
        // 最坏退化成每 30s 一次普通请求，不会抖成请求风暴。
        if (++missStreak >= 3 && Date.now() - lastFullAt > 30000) {
          missStreak = 0;
          since = 0;
        }
      } else {
        missStreak = 0;
      }
    } catch {
      /* 静默 */
    } finally {
      pending = false;
    }
  }

  // 注入节点自身判定：线元素或线内文本/子节点。observer 用它区分「注入脚本自己写的
  // 变更」（追加线、走字）与「外部（React）变更」，前者不反馈调度扫描。
  function isOwnNode(n) {
    if (!n) return true;
    if (n.nodeType === 1) return !!(n.closest && n.closest(`[${MARK}]`));
    const p = n.parentElement;
    return !!(p && p.closest && p.closest(`[${MARK}]`));
  }

  function start() {
    try {
      const mo = new MutationObserver((muts) => {
        let foreign = false;   // 本批含注入节点之外的变更
        let unpainted = false; // 有缺线 section 但缓存无数据，需取数后补画
        for (const m of muts) {
          if (m.type === "childList") {
            const nodes = [...m.addedNodes, ...m.removedNodes];
            if (nodes.some((n) => !isOwnNode(n))) {
              foreign = true;
              for (const n of m.addedNodes) {
                if (n.nodeType !== 1) continue;
                const sec =
                  n.tagName === "SECTION" && n.hasAttribute("data-turn-id")
                    ? n
                    : n.querySelector(SECTION_SEL);
                if (sec && !sec.querySelector(`[${MARK}]`)) {
                  // 同帧补画：observer 回调是微任务，跑在浏览器为这批变更上屏之前，
                  // 此刻画线 = 统计行与 section 本体同帧出现，重挂载（轮末迁移、
                  // 虚拟窗口挂卸、会话切换）不再有「缺线一帧」的空窗闪烁
                  const t = matchTurn(sec);
                  if (t && enabled) render(sec, t);
                  else unpainted = true;
                }
              }
            }
          } else if (!isOwnNode(m.target)) {
            foreign = true;
          }
        }
        if (!foreign) return; // 自反馈：本批全是注入节点自身的变更，不扫描
        clearTimeout(mo._t);
        if (unpainted) {
          // 缓存没数据的新 section：等取数后补画；
          // _f 去重防同帧多批重复调度
          if (!mo._f) mo._f = requestAnimationFrame(() => {
            mo._f = 0;
            scan();
          });
        } else {
          mo._t = setTimeout(scan, 150);
        }
      });
      mo.observe(document.body, { childList: true, subtree: true });
      // 1s 兜底：长工具运行期间 DOM 可能完全静默（observer 不触发），
      // 运行中计时/用时靠这个间隔逐秒走字
      setInterval(scan, 1000);
      scan();
    } catch {
      /* 静默 */
    }
  }

  if (document.body) start();
  else document.addEventListener("DOMContentLoaded", start);
})();
