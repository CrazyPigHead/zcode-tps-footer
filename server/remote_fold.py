#!/usr/bin/env python3
"""远端聚合脚本：在远程服务器上执行，读取该机的 ZCode 用量库并按轮折叠。

调用方式（由本地 server.mjs 发起；脚本按内容 hash 缓存在 ~/.cache/zcode-tps-footer/，
常态执行缓存副本，缓存不可用时经 stdin 直送）：
    ssh -p <port> <user@host> 'test -f <缓存脚本> || exit 91; exec python3 <缓存脚本> [since_ms]'
    ssh -p <port> <user@host> python3 - [since_ms] < remote_fold.py

argv[1] 可选 since_ms（增量水位）：
    提供 → 增量模式：已结算轮只返回 end_ms >= since_ms - INC_MARGIN_MS 的条目 +
           全部 live 条目（live 每轮都在变，必带）；重复条目本地按 turn_id 幂等覆盖
    缺省 → 全量模式：返回 24h 窗内全部条目

stdout 输出单行 JSON：
  {"turns": [...], "since": <窗内最新已结算轮 end_ms，无则当前时刻>, "full": <是否全量>}
turns 字段与本地 foldLocal 完全一致：
  turn_id / msg_id / session_id / status / start_ms / end_ms / run_ms /
  ttft_ms / tps / out_tokens / turn_no / steps / models /
  query_source / ctx_tokens（上下文水位，分母 ctx_window 由本地侧按模型目录解析，
  远端不读目录——见 server.mjs 的「模型上下文窗口」节）
进行中的轮以 live:true、status:"running" 条目返回，另带步内实时态：
  phase（tool|generating|gap）/ gen_start_ms / gap_start_ms /
  tool / tool_start_ms / tool_pending / steps_settled

三线合成口径（与本地侧 1:1，详见 server.mjs foldLocal 注释）：
  marker  message ⋈ part(step-start)：请求发出前落库，定义步的存在与真实步起点
  settle  model_usage：流结束落库，按 assistant_message_id 关联 marker，提供 token/ttft
  tool    tool_usage：调度即 running（含流中工具），提供「正在执行的操作」
  steps   = marker 数 + 轮内 compact 修正，与轮末 model_request_count 严格守恒
live 轮 = 该会话 marker 流的尾段；长流式步/长工具在 marker 流上是大空洞，会按证据回并
          （切段与回并规则见 fold 内注释，与 server.mjs 逐行同构）

只用 Python 标准库；库不存在或查询失败时输出 {"turns": [], "error": ...}，
绝不向 stderr 之外的地方写杂音（stdout 必须是纯 JSON）。
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
import time

DB = os.environ.get("TPS_FOOTER_DB") or os.path.expanduser("~/.zcode/cli/db/db.sqlite")  # 测试可指向合成库
WINDOW_MS = 24 * 60 * 60 * 1000
LIVE_STALE_MS = 10 * 60 * 1000        # live 判定①：最近 marker/结算落库距今
LIVE_MAX_MS = 2 * 60 * 60 * 1000      # live 判定②：尾步起点/运行中工具的活性上限。长流式期间
                                      # DB 完全安静（实测 126s 采样 session.time_updated/part 全
                                      # 冻结），判活只能用「步已开始多久」；实测极值单步 54.6min
                                      # （128k 输出＝服务端单步上限），2h 留足余量。代价：真崩溃
                                      #（尾 marker 永久未结算）的幽灵「思考中」行窗口由 10min
                                      # 放宽到 2h——罕见场景换频繁场景
SEGMENT_GAP_MS = 30 * 60 * 1000       # 相邻 marker 间隔超过此值切分段：隔离崩溃残留（长流式步/
                                      # 长工具的同类空洞由回并规则救回，见 fold）
GAP_BRIDGE_EPS_MS = 60 * 1000         # gap 回并的桥接容差：前段结算结束（或洞内已完成工具）距
                                      # 尾段首 marker 的最大间隔——步流水线串行，实测故障轮仅
                                      # 71ms，60s 留 800 倍余量
INC_MARGIN_MS = 5 * 60 * 1000         # 增量水位安全边距：过滤时回看这么多，吸收多会话
                                      # 交错落库（B 轮结算时刻早于 A 轮形成的水位）与时钟
                                      # 微抖；多回的条目本地按 turn_id 幂等覆盖，无副作用


def ctx_tokens(in_tok, out_tok, prov_total, comp_total) -> int:
    """上下文水位 = 官方口径 input+output（AI SDK v6 的 input 已含 cache read/write，
    cache 两列是 breakdown，不能重复相加）；input 缺失（0）按官方回退链退
    provider_total-output，再退 computed_total。与 server.mjs ctxTokensOfRow 1:1。"""
    if in_tok and in_tok > 0:
        return in_tok + (out_tok or 0)
    if prov_total:
        rest = prov_total - (out_tok or 0)
        return rest if rest > 0 else prov_total
    return comp_total or 0


def fold(since_ms: int = 0) -> dict:
    if not os.path.exists(DB):
        # 库整个没了（远端 ~/.zcode 被清空/重置）：按全量应答，让调用方立即清空旧合并表
        return {"turns": [], "since": int(time.time() * 1000), "full": True}
    cut = int(time.time() * 1000) - WINDOW_MS
    conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    live: list = []
    try:
        # 只读 deferred 事务：十几条查询共享同一 WAL 快照（与 server.mjs 的 BEGIN
        # DEFERRED 对齐）——python 的隐式事务只包 DML，不手动 BEGIN 的话各 SELECT
        # 独立快照，会读到「marker 可见、tool 未可见」的撕裂帧
        conn.execute("BEGIN")
        # 具名取列（sqlite3.Row）：model_usage 的列集随 schema 演进（provider_id、三个 token 列
        # 都可能缺失），下标访问会在增列时静默错位；两 fold 的字段语义按名字对齐
        conn.row_factory = sqlite3.Row
        # 规划器无统计信息时会为带时间窗的 model_usage 查询选择 query_source
        # 单列索引（= 近全表扫，行内含大 JSON blob）；INDEXED BY 强制走 started_at 复合
        # 索引，索引不存在（未来 schema 变更）则退回原 SQL。与 server.mjs foldLocal 同步。
        started_idx = (
            " INDEXED BY model_usage_started_model_idx"
            if conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'index'"
                " AND name = 'model_usage_started_model_idx'"
            ).fetchone()
            else ""
        )
        now_ms = int(time.time() * 1000)
        turns = conn.execute(
            """
            SELECT session_id, turn_id, user_message_id, status,
                   started_at, completed_at, time_to_first_token_ms, output_tokens,
                   model_request_count
            FROM turn_usage
            WHERE started_at >= ? AND user_message_id IS NOT NULL AND user_message_id != ''
            ORDER BY started_at ASC
            """,
            (cut,),
        ).fetchall()
        # 轮次序号：按会话内 started_at 全量排序（不受 24h 窗口截断影响）
        turn_no: dict = {}
        for sid in {r[0] for r in turns}:
            rows = conn.execute(
                "SELECT started_at, turn_id FROM turn_usage WHERE session_id = ? ORDER BY started_at, turn_id",
                (sid,),
            ).fetchall()
            for i, (_started, tid) in enumerate(rows, 1):
                turn_no[(sid, tid)] = i
        # done_ids 限定 48h 窗（live 候选的段起点必然贴近窗口内），避免随库龄无界增长的全表扫
        done_ids = {
            r[0]
            for r in conn.execute(
                "SELECT turn_id FROM turn_usage WHERE started_at >= ?", (cut - WINDOW_MS,)
            )
        }

        # ---- 步结算明细：一条查询喂多个消费者（tok/s 折叠、marker 关联、首 token、模型名、
        #      上下文水位）。query_source 白名单排除 session_title/compact 等借用主轮
        #      turn_id 的旁路查询。
        dec: dict = {}            # turn_id -> (解码ms, 输出tok)
        models_by_turn: dict = {}
        settle_by_mid: dict = {}  # assistant_message_id -> 结算行（具名取列，见 conn.row_factory）
        ttft_by_tid: dict = {}
        ctx_by_tid: dict = {}     # turn_id -> (ctx_tokens, query_source, model, provider)，最后一条 completed 行胜出
        ctx_by_mid: dict = {}     # assistant_message_id -> (ctx_tokens, model, provider)，live 段取水位用
        qsrc_by_sid: dict = {}    # session_id -> query_source（同会话恒同源）
        # 老 ZCode CLI 的库可能没有三个 token 列、也没有 provider_id——探测缺列就退回
        # 无水位/无 provider 模式（与上面 INDEXED BY 的探测降级同款），保住该远端原有统计
        # 不整体消失。query_source 恒真列传：WHERE 已依赖它，不可能缺。
        _cols = {r[1] for r in conn.execute("PRAGMA table_info(model_usage)")}
        has_ctx_cols = {"input_tokens", "provider_total_tokens", "computed_total_tokens"}.issubset(_cols)
        has_prov = "provider_id" in _cols
        ctx_cols = ", input_tokens, provider_total_tokens, computed_total_tokens" if has_ctx_cols else ""
        prov_col = ", provider_id" if has_prov else ""
        for r in conn.execute(
            f"""
            SELECT session_id, turn_id, assistant_message_id, model_id,
                   status, started_at, completed_at, duration_ms,
                   time_to_first_token_ms, output_tokens, query_source{prov_col}{ctx_cols}
            FROM model_usage{started_idx}
            WHERE started_at >= ?
              AND query_source IN ('main_turn', 'subagent', 'workflow_child')
            ORDER BY started_at
            """,
            (cut,),
        ):
            _sid, tid, mid, model, status = (
                r["session_id"], r["turn_id"], r["assistant_message_id"], r["model_id"], r["status"],
            )
            duration, ttft, out_tok = r["duration_ms"], r["time_to_first_token_ms"], r["output_tokens"]
            qsrc = r["query_source"]
            prov = r["provider_id"] if has_prov else None  # 窗口分母的作用域键（本地侧解析用）
            in_tok, prov_total, comp_total = (
                (r["input_tokens"], r["provider_total_tokens"], r["computed_total_tokens"])
                if has_ctx_cols
                else (0, None, 0)
            )
            if status == "completed":
                dec_ms = (
                    duration - ttft
                    if ttft is not None and duration is not None
                    and duration - ttft > 0 and (out_tok or 0) > 0
                    else 0
                )
                dec_tok = out_tok if ttft is not None and (out_tok or 0) > 0 else 0
                d, tok = dec.get(tid, (0, 0))
                dec[tid] = (d + dec_ms, tok + dec_tok)
                if model:
                    models_by_turn.setdefault(tid, [])
                    if model not in models_by_turn[tid]:
                        models_by_turn[tid].append(model)
                if has_ctx_cols:
                    ctx = ctx_tokens(in_tok, out_tok, prov_total, comp_total)
                    ctx_by_tid[tid] = (ctx, qsrc, model, prov)
                    if mid:
                        ctx_by_mid[mid] = (ctx, model, prov)
            qsrc_by_sid[_sid] = qsrc
            if ttft is not None and tid not in ttft_by_tid:
                ttft_by_tid[tid] = ttft
            if mid:
                settle_by_mid[mid] = r

        # ---- 轮内 reactive compact 修正（compact 请求占用一次 ModelRequest 事件但不产生 marker）
        compact_by_tid: dict = {}
        for tid, n in conn.execute(
            f"""
            SELECT turn_id, COUNT(*)
            FROM model_usage{started_idx}
            WHERE started_at >= ? AND query_source = 'compact'
            GROUP BY turn_id
            """,
            (cut,),
        ):
            compact_by_tid[tid] = n

        # ---- 运行中工具（含流中工具），按会话分桶（升序），按时间挂到 live 段。
        #      窗口与 live 判定② 同一上限（LIVE_MAX_MS）：>60min 的长工具曾整体出窗，
        #      把活跃轮误判成死轮并让 phase 从「工具 运行中」错误回落
        running_by_sid: dict = {}
        for sid, tid, name, start, approval in conn.execute(
            "SELECT session_id, turn_id, tool_name, started_at, approval_status FROM tool_usage"
            " WHERE status = 'running' AND started_at >= ? ORDER BY started_at",
            (now_ms - LIVE_MAX_MS,),
        ):
            running_by_sid.setdefault(sid, []).append((tid, name, start, approval))

        # gap 回并的第③条证据：懒查询（只在 gap 切点且①②都不成立时发一次；正常折叠零开销）
        def has_done_tool_near(sid_: str, center_ms: int) -> bool:
            return (
                conn.execute(
                    "SELECT 1 FROM tool_usage WHERE session_id = ? AND status != 'running'"
                    " AND COALESCE(completed_at, started_at) BETWEEN ? AND ? LIMIT 1",
                    (sid_, center_ms - GAP_BRIDGE_EPS_MS, center_ms + GAP_BRIDGE_EPS_MS),
                ).fetchone()
                is not None
            )

        # ---- 会话发现：session.time_updated 随每次消息/part 写入被 touch，
        #      覆盖「首步尚未落任何 usage 行」的全新会话
        sids = {r[0] for r in conn.execute(
            "SELECT id FROM session WHERE time_updated >= ?", (cut,)
        )}
        sids |= {r[0] for r in turns}
        sids |= {r[0] for r in settle_by_mid.values()}

        marker_stmt = """
            SELECT m.id AS mid, m.time_created AS t0, json_extract(m.data, '$.parentID') AS parent
            FROM message m
            WHERE m.session_id = ? AND m.time_created >= ?
              AND json_extract(m.data, '$.role') = 'assistant'
              AND EXISTS (
                SELECT 1 FROM part p
                WHERE p.message_id = m.id AND json_extract(p.data, '$.type') = 'step-start'
              )
            ORDER BY m.time_created, m.id
        """

        # 段内已结算 marker 的 turn_id 集合（回并证据①；同轮所有步共用 turn_id）
        def _tids(part: list) -> set:
            out: set = set()
            for mk in part:
                s = settle_by_mid.get(mk["mid"])
                if s is not None and s["turn_id"]:
                    out.add(s["turn_id"])
            return out

        for sid in sids:
            markers = conn.execute(marker_stmt, (sid, cut)).fetchall()
            if not markers:
                continue
            # 切段：跨过任一已结算轮终点、或相邻 marker 空洞 > SEGMENT_GAP_MS 都开新段，
            # 尾段即当前 live 轮（轮内 steering 只追加 marker 不换段）。reasons[i] 记
            # 「起始 segs[i] 的切点成因」：'boundary' 跨过已结算轮终点（禁止回并）、
            # 'gap' 大空洞（有证据才回并）；两者同时满足按 boundary 保守归类；首段 None。
            boundaries = [
                r["bend"]
                for r in conn.execute(
                    # 边界 = 轮终点（completed_at，缺失退 started_at）：live 轮自己没有
                    # turn_usage 行，只有上一轮的「终点」能把它的 markers 从上一轮切开
                    "SELECT COALESCE(completed_at, started_at) AS bend FROM turn_usage"
                    " WHERE session_id = ? AND started_at >= ? ORDER BY started_at",
                    (sid, cut),
                )
            ]
            segs: list = []
            reasons: list = [None]
            cur = [markers[0]]
            bi = 0
            for mk in markers[1:]:
                prev = cur[-1]
                while bi < len(boundaries) and boundaries[bi] <= prev["t0"]:
                    bi += 1
                crossed = bi < len(boundaries) and boundaries[bi] <= mk["t0"]
                if mk["t0"] - prev["t0"] > SEGMENT_GAP_MS or crossed:
                    segs.append(cur)
                    cur = [mk]
                    reasons.append("boundary" if crossed else "gap")
                else:
                    cur.append(mk)
            segs.append(cur)

            # ---- gap 切点回并：marker 只在步开始落库，一次超长流式请求（实测单步 54.6min /
            #      128k 输出）或一次超长工具在 marker 流上就是个大空洞，会被上面当成崩溃残留
            #      误切，把「用时」腰斩（故障轮 104min 只显示 20min）。取尾段后从后往前逐段
            #      试并，满足任一证据才并（宁可不并、不可错并）：
            #        ① turn_id 一致：前段任一已结算 marker 与尾段已见 turn_id 有交集（最硬）；
            #        ② 结算桥接：前段尾 marker 已结算，且其结束时刻距尾段首 marker ≤ EPS
            #          （步流水线串行，下一步请求紧跟前步结算；实测故障轮仅 71ms）；
            #        ③ 工具桥接：切点附近存在已完成工具（长工具跑完立即发下一步请求）。
            #      崩溃残留天生无证据（尾步未结算、洞内只有永远 running 的残留行、无 turn_id
            #      可比）→ 保持隔离；异常结束的旧轮被 ②③ 的「紧贴」条件挡掉（残留完成痕迹
            #      与新轮首 marker 之间必然隔着死亡时间 + 用户思考时间 > 60s）。
            seg = segs[-1]
            tids = _tids(seg)
            while len(segs) > 1 and reasons[-1] == "gap":
                prev_seg = segs[-2]
                prev_tids = _tids(prev_seg)
                s = settle_by_mid.get(prev_seg[-1]["mid"])
                end = None
                if s is not None:
                    end = (
                        s["completed_at"]
                        if s["completed_at"] is not None
                        else (s["started_at"] or 0) + (s["duration_ms"] or 0)
                    )
                bridge_settle = end is not None and abs(seg[0]["t0"] - end) <= GAP_BRIDGE_EPS_MS
                hit = bool(tids) and bool(prev_tids & tids)
                if not hit and not bridge_settle:
                    hit = has_done_tool_near(sid, seg[0]["t0"])
                if hit or bridge_settle:
                    seg = prev_seg + seg  # 并回后 seg[0] 即下一个待检查切点，循环继续往前试
                    tids |= prev_tids
                    segs.pop()
                    reasons.pop()
                else:
                    break
            seg_start = seg[0]["t0"]

            # settle 关联：每个 marker 至多一条结算行（model_usage id 按 assistant_message 幂等 upsert）
            settled_count = 0
            last_end = 0
            seg_tid = None
            ctx_row = None  # 段内最后已结算步 = 当前上下文水位（与官方 ModelComplete 更新粒度一致）
            settled_tids: set = set()
            for mk in seg:
                s = settle_by_mid.get(mk["mid"])
                if not s:
                    continue
                settled_count += 1
                if s["turn_id"]:
                    settled_tids.add(s["turn_id"])
                    seg_tid = s["turn_id"]
                c = ctx_by_mid.get(mk["mid"])
                if c is not None and (ctx_row is None or (s["started_at"] or 0) >= (ctx_row[3] or 0)):
                    ctx_row = (c[0], c[1], c[2], s["started_at"] or 0)  # (水位, 模型, provider, started_at)
                end = (
                    s["completed_at"]
                    if s["completed_at"] is not None
                    else (s["started_at"] or 0) + (s["duration_ms"] or 0)
                )
                if end > last_end:
                    last_end = end
            # 步流水线是串行的，真正在飞的只可能是段尾 marker；段中无 settle 的 marker
            # 是被 reactive compact / Start Plan 重试抛弃的步（永不结算），不能据此判定
            # 「进行中」，否则轮末 turn_usage 已接管后 live 条目会滞留到 freshness 过期
            seg_tail = seg[-1]
            in_flight = seg_tail if seg_tail["mid"] not in settle_by_mid else None
            # 整段结算且轮已关闭 → turn_usage 已接管，live 退出（最终值原地覆盖）
            if not in_flight and settled_tids and settled_tids.issubset(done_ids):
                continue
            # 活性：最近 10 分钟有 marker/结算落库，或尾步起点仍在 LIVE_MAX_MS 内（长流式期间
            # DB 完全安静，唯一可用的活性信号就是「步已开始多久」），或窗口内仍有 running
            # 工具（长工具同样不落库）。三者皆无 = 崩溃残留的半截轮，不合成。
            fresh_step = max(last_end, seg[-1]["t0"]) >= now_ms - LIVE_STALE_MS
            in_flight_fresh = in_flight is not None and now_ms - in_flight["t0"] <= LIVE_MAX_MS
            tools = [t for t in running_by_sid.get(sid, []) if t[2] >= seg_start]
            if not fresh_step and not in_flight_fresh and not tools:
                continue

            # 操作态：tool > generating > gap；等待确认优先展示；计时取本批最早 running（批起点）
            phase = "generating" if in_flight else "gap"
            tool = None
            tool_start = None
            tool_pending = False
            if tools:
                phase = "tool"
                tool_start = tools[0][2]
                pend = next((t for t in reversed(tools) if t[3] == "requested"), None)
                tool = (pend or tools[-1])[1]
                tool_pending = pend is not None

            dec_ms, dec_tok = dec.get(seg_tid, (0, 0))
            tps = (dec_tok * 1000.0 / dec_ms) if dec_ms > 0 else None
            prior = conn.execute(
                "SELECT COUNT(*) FROM turn_usage WHERE session_id = ? AND started_at < ?",
                (sid, seg_start),
            ).fetchone()[0]
            live.append(
                {
                    "turn_id": seg_tid,
                    "msg_id": next((mk["parent"] for mk in seg if mk["parent"]), None),
                    "session_id": sid,
                    "status": "running",
                    "live": True,
                    "phase": phase,
                    "start_ms": seg_start,
                    "end_ms": now_ms,
                    "run_ms": max(0, now_ms - seg_start),
                    "gen_start_ms": in_flight["t0"] if in_flight else None,
                    "gap_start_ms": last_end or None if phase == "gap" else None,
                    "ttft_ms": ttft_by_tid.get(seg_tid),
                    "tps": round(tps, 2) if tps else None,
                    "out_tokens": dec_tok or 0,
                    "turn_no": prior + 1,
                    "steps": len(seg) + compact_by_tid.get(seg_tid, 0),
                    "steps_settled": settled_count,
                    "models": models_by_turn.get(seg_tid, []),
                    "query_source": qsrc_by_sid.get(sid),  # 子会话判定（注入端只对 subagent/workflow_child 追加上下文段）
                    "ctx_tokens": ctx_row[0] if ctx_row else None,  # 当前水位；首步结算前为 None（不显示）
                    "ctx_model": ctx_row[1] if ctx_row else None,  # 水位行精确模型，本地解析分母用
                    "ctx_provider": ctx_row[2] if ctx_row else None,  # 水位行 provider，分母的作用域键
                    "tool": tool,
                    "tool_start_ms": tool_start,
                    "tool_pending": tool_pending,
                }
            )
    finally:
        conn.rollback()  # 只读事务，rollback = 结束快照；无活动事务时是 no-op
        conn.close()

    out = []
    # 水位在全量口径上计算：遍历窗内全部已结算轮（含被增量过滤掉的），保证不因过滤
    # 而水位回退；窗内没有任何已结算轮时退当前时刻（下次增量只多带 5 分钟内的旧轮）
    watermark = 0
    for sid, tid, msg_id, status, started, completed, ttft, out_tok, req_count in turns:
        completed = completed or started
        if completed > watermark:
            watermark = completed
        # 增量过滤：已结算轮不可变，早于「水位-边距」的条目调用方必然已有
        if since_ms and completed < since_ms - INC_MARGIN_MS:
            continue
        decode_ms, decode_tok = dec.get(tid, (0, 0))
        tps = (decode_tok * 1000.0 / decode_ms) if decode_ms > 0 else None
        ctx_info = ctx_by_tid.get(tid)
        out.append(
            {
                "turn_id": tid,
                "msg_id": msg_id,  # 桥：界面 section[data-turn-id] 的取值随版本可能是用户消息 ID
                "session_id": sid,
                "status": status,
                "start_ms": started,
                "end_ms": completed,
                "run_ms": max(0, completed - started),
                "ttft_ms": ttft,
                "tps": round(tps, 2) if tps else None,
                "out_tokens": out_tok or 0,
                "turn_no": turn_no.get((sid, tid)),
                "steps": req_count or 0,
                "models": models_by_turn.get(tid, []),
                "query_source": (ctx_info[1] if ctx_info else qsrc_by_sid.get(sid)),
                "ctx_tokens": ctx_info[0] if ctx_info else None,  # 轮末真实上下文水位
                "ctx_model": ctx_info[2] if ctx_info else None,
                "ctx_provider": ctx_info[3] if ctx_info else None,  # 本地侧解析分母的作用域键
                # ctx_window（分母）由本地侧解析模型目录后回填，见 server.mjs mergedTurns
            }
        )
    out.extend(live)
    out.sort(key=lambda t: t["end_ms"], reverse=True)
    return {
        "turns": out,
        "since": watermark or int(time.time() * 1000),
        "full": not since_ms,
    }


if __name__ == "__main__":
    try:
        since_ms = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1].isdigit() else 0
        print(json.dumps(fold(since_ms), separators=(",", ":"), ensure_ascii=False))
    except Exception as e:  # 任何异常都以 JSON 回告，方便本地侧记录
        try:
            print(json.dumps({"turns": [], "error": repr(e)[:300]}))
        except Exception:
            sys.exit(1)
