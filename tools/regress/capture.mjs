// 等待 healthz 就绪后抓取 /turns?limit=2000 存盘（node 裸 http，无视代理环境变量）
// 端口与 server.mjs 一致可用 TPS_FOOTER_PORT 覆盖，改动前后对拍时各抓一份
import fs from "node:fs";
import http from "node:http";

const out = process.argv[2];
const port = Number(process.env.TPS_FOOTER_PORT) || 3117;
const waitMaxMs = 15000;

function get(path) {
  return new Promise((resolve, reject) => {
    const req = http.get({ host: "127.0.0.1", port, path, timeout: 2000 }, (res) => {
      let b = "";
      res.on("data", (c) => (b += c));
      res.on("end", () => resolve({ code: res.statusCode, body: b }));
    });
    req.on("timeout", () => { req.destroy(); reject(new Error("timeout")); });
    req.on("error", reject);
  });
}

const t0 = Date.now();
for (;;) {
  try {
    const h = await get("/healthz");
    if (h.code === 200 && h.body === "ok") break;
  } catch {}
  if (Date.now() - t0 > waitMaxMs) { console.error("service not ready"); process.exit(1); }
  await new Promise((r) => setTimeout(r, 300));
}
const r = await get("/turns?limit=2000");
fs.writeFileSync(out, r.body);
const turns = JSON.parse(r.body).turns;
console.log(`saved ${out}: ${turns.length} turns (${r.body.length} bytes), live=${turns.filter(t => t.live).length}`);
