/**
 * zeus-worker
 * Notion → Zeus 同期処理専用 Cloudflare Worker
 *
 * エンドポイント:
 *   GET  /diag          → 環境変数・疎通チェック（認証不要）
 *   POST /sync-db       → 1DB同期（body: { source, user_id }）
 *   POST /sync-all      → 全5DB同期（body: { user_id?, force_full? }）
 *
 * 認証: Authorization: Bearer {ZEUS_WORKER_SECRET}
 * Cron: 毎日 JST 03:00（UTC 18:00）→ sync-all を自動実行
 *
 * 設計:
 *   - Cloudflare Workers に subrequest 上限なし（Pages Functions の 50回制限を回避）
 *   - waitUntil で長時間処理に対応
 *   - DB単位で独立して処理 → 1DBが失敗しても他は継続
 */

// ─── DB定義 ────────────────────────────────────────────────────────────────────

const NOTION_DBS = [
  { source: "notion-inbox",   dbId: "31c9c6c1c439800f8093dd4e9dca241c", label: "inbox",         skipBlocks: false },
  { source: "notion-input",   dbId: "31b9c6c1c43980b48b91d7128950f794", label: "インプットDB",   skipBlocks: false },
  { source: "notion-output",  dbId: "31b9c6c1c43980c5b8ccdf3b7fea572a", label: "アウトプットDB", skipBlocks: false },
  { source: "notion-asset",   dbId: "31b9c6c1c43980bd963fc2ca909feacb", label: "アセットDB",     skipBlocks: false },
  { source: "notion-project", dbId: "31b9c6c1c4398069b884f0916da9e795", label: "プロジェクトDB", skipBlocks: false },
];

const VOYAGE_BATCH   = 20;
const SUPABASE_BATCH = 50;
const BLOCK_CONCUR   = 5; // Workerはsubrequest制限なしのため並列数を増やせる

// ─── エントリポイント ──────────────────────────────────────────────────────────

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    // CORS preflight
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: corsHeaders() });
    }

    // 診断（認証不要）
    if (url.pathname === "/diag" && request.method === "GET") {
      return handleDiag(env);
    }

    // 認証
    if (!isAuthorized(request, env)) {
      return json({ error: "unauthorized" }, 401);
    }

    // ルーティング
    if (url.pathname === "/sync-db" && request.method === "POST") {
      return handleSyncDb(request, env);
    }

    if (url.pathname === "/sync-all" && request.method === "POST") {
      return handleSyncAll(request, env, ctx);
    }

    return json({ error: "not_found" }, 404);
  },

  async scheduled(event, env, ctx) {
    // JST 03:00 = UTC 18:00
    ctx.waitUntil(runSyncAll(env, false));
  },
};

// ─── 認証 ─────────────────────────────────────────────────────────────────────

function isAuthorized(request, env) {
  const auth = request.headers.get("Authorization") || "";
  if (!env.ZEUS_WORKER_SECRET) return false;
  if (!auth.startsWith("Bearer ")) return false;
  const token = auth.slice(7);
  // タイミング攻撃対策
  if (token.length !== env.ZEUS_WORKER_SECRET.length) return false;
  let result = 0;
  for (let i = 0; i < token.length; i++) {
    result |= token.charCodeAt(i) ^ env.ZEUS_WORKER_SECRET.charCodeAt(i);
  }
  return result === 0;
}

// ─── ハンドラ ──────────────────────────────────────────────────────────────────

async function handleDiag(env) {
  const checks = {
    ZEUS_WORKER_SECRET: !!env.ZEUS_WORKER_SECRET,
    NOTION_API_KEY:     !!env.NOTION_API_KEY,
    VOYAGE_API_KEY:     !!env.VOYAGE_API_KEY,
    VITE_SUPABASE_URL:  !!env.VITE_SUPABASE_URL,
    VITE_SUPABASE_ANON_KEY: !!env.VITE_SUPABASE_ANON_KEY,
    MCP_DEFAULT_USER_ID: !!env.MCP_DEFAULT_USER_ID,
  };
  const missing = Object.entries(checks).filter(([, v]) => !v).map(([k]) => k);
  return json({ ok: missing.length === 0, checks, missing });
}

// 1DB同期（Settings画面から呼ばれる）
async function handleSyncDb(request, env) {
  let body = {};
  try { body = await request.json(); } catch { /* 省略OK */ }

  const { source, user_id } = body;
  const uid = user_id || env.MCP_DEFAULT_USER_ID;
  if (!uid) return json({ error: "user_id required" }, 400);

  const db = NOTION_DBS.find(d => d.source === source);
  if (!db) return json({ error: `unknown source: ${source}` }, 400);

  try {
    const result = await syncOneDb(env, uid, db);
    return json({ ok: true, ...result });
  } catch (e) {
    console.error(`[zeus-worker] sync-db ${source}:`, e.message);
    return json({ error: e.message }, 502);
  }
}

// 全DB同期（Cron・shia2n-mcpから呼ばれる）
async function handleSyncAll(request, env, ctx) {
  let body = {};
  try { body = await request.json(); } catch { /* 省略OK */ }

  const uid        = body.user_id || env.MCP_DEFAULT_USER_ID;
  const forceFull  = body.force_full === true;
  if (!uid) return json({ error: "user_id required" }, 400);

  // waitUntil で長時間処理を継続（レスポンスは即返す）
  ctx.waitUntil(runSyncAll(env, forceFull, uid));
  return json({ ok: true, message: "sync started", user_id: uid, force_full: forceFull });
}

// ─── コア同期処理 ──────────────────────────────────────────────────────────────

async function runSyncAll(env, forceFull = false, uid = null) {
  const userId = uid || env.MCP_DEFAULT_USER_ID;
  if (!userId) { console.error("[zeus-worker] MCP_DEFAULT_USER_ID not set"); return; }

  const bySource = {};
  let   total    = 0;

  for (const db of NOTION_DBS) {
    try {
      const result   = await syncOneDb(env, userId, db, forceFull);
      bySource[db.source] = result.imported;
      total              += result.imported;
    } catch (e) {
      console.error(`[zeus-worker] sync-all ${db.source}:`, e.message);
      bySource[db.source] = { error: e.message };
    }
  }

  console.log(`[zeus-worker] sync-all done. total=${total}`, JSON.stringify(bySource));
}

async function syncOneDb(env, userId, db, forceFull = false) {
  const { source, dbId, label, skipBlocks } = db;

  // 1. 既存エントリ削除
  await supaDelete(env, "zeus_items",
    `source_app=eq.${source}&user_id=eq.${encodeURIComponent(userId)}`);

  // 2. Notionページ全件取得
  const pages = await notionAllPages(env.NOTION_API_KEY, dbId);
  if (pages.length === 0) return { source, imported: 0 };

  // 3. ブロック本文取得（skipBlocks=falseのみ）
  const blockMap = skipBlocks
    ? new Map()
    : await notionBlockMap(env.NOTION_API_KEY, pages.map(p => p.id));

  // 4. zeus_items 行構築
  const rows = pages.map(p => buildRow(source, userId, p, blockMap.get(p.id) || ""));

  // 5. Voyage AI バッチ Embedding
  for (let i = 0; i < rows.length; i += VOYAGE_BATCH) {
    const batch = rows.slice(i, i + VOYAGE_BATCH);
    let   embs;
    try {
      embs = await voyageEmbed(env.VOYAGE_API_KEY, batch.map(r => `${r.title}\n\n${r.content}`));
    } catch (e) {
      console.error(`[zeus-worker] embed ${source} offset ${i}:`, e.message);
      embs = batch.map(() => null);
    }
    batch.forEach((r, idx) => { r.embedding = embs[idx]; });
  }

  // 6. zeus_items 一括INSERT
  const projectId   = await upsertProject(env, userId, source, `Notionナレッジ: ${label}`);
  const insertedItems = await supaBulkInsert(env, "zeus_items", rows);

  // 7. zeus_item_projects 一括INSERT
  await supaBulkInsert(env, "zeus_item_projects",
    insertedItems.map(r => ({ item_id: r.id, project_id: projectId })));

  return { source, imported: pages.length };
}

// ─── Notion API ────────────────────────────────────────────────────────────────

function notionHeaders(key) {
  return {
    "Authorization":  `Bearer ${key}`,
    "Notion-Version": "2022-06-28",
    "Content-Type":   "application/json",
  };
}

async function notionAllPages(notionKey, dbId) {
  const pages = []; let cursor;
  do {
    const body = { page_size: 100 };
    if (cursor) body.start_cursor = cursor;
    const res = await fetch(`https://api.notion.com/v1/databases/${dbId}/query`, {
      method: "POST", headers: notionHeaders(notionKey), body: JSON.stringify(body),
    });
    if (!res.ok) { console.error(`[notion] DB ${dbId}: ${res.status}`); break; }
    const d = await res.json();
    pages.push(...(d.results || []));
    cursor = d.has_more ? d.next_cursor : undefined;
  } while (cursor);
  return pages;
}

async function notionPageBlockText(notionKey, pageId) {
  const lines = []; let cursor;
  do {
    const url = `https://api.notion.com/v1/blocks/${pageId}/children?page_size=100${cursor ? `&start_cursor=${cursor}` : ""}`;
    const res = await fetch(url, { headers: notionHeaders(notionKey) });
    if (!res.ok) break;
    const d = await res.json();
    for (const b of (d.results || [])) {
      const tx = extractBlockText(b);
      if (tx) lines.push(tx);
    }
    cursor = d.has_more ? d.next_cursor : undefined;
  } while (cursor);
  return lines.join("\n").trim();
}

function extractBlockText(block) {
  const t = block.type, b = block[t];
  if (!b) return "";
  const richTextTypes = [
    "paragraph", "heading_1", "heading_2", "heading_3",
    "bulleted_list_item", "numbered_list_item", "toggle", "quote", "callout",
  ];
  if (richTextTypes.includes(t)) return (b.rich_text || []).map(x => x.plain_text).join("").trim();
  if (t === "code") {
    const tx = (b.rich_text || []).map(x => x.plain_text).join("").trim();
    return tx ? `\`\`\`\n${tx}\n\`\`\`` : "";
  }
  if (t === "divider") return "---";
  return "";
}

async function notionBlockMap(notionKey, pageIds) {
  const map = new Map();
  for (let i = 0; i < pageIds.length; i += BLOCK_CONCUR) {
    const chunk = pageIds.slice(i, i + BLOCK_CONCUR);
    const texts = await Promise.all(
      chunk.map(id => notionPageBlockText(notionKey, id).catch(() => ""))
    );
    chunk.forEach((id, idx) => map.set(id, texts[idx]));
  }
  return map;
}

// ─── Notion プロパティ抽出 ──────────────────────────────────────────────────────

function extractTitle(props) {
  for (const v of Object.values(props)) {
    if (v?.type === "title") return (v.title || []).map(t => t.plain_text).join("").trim();
  }
  return "";
}
function extractRichText(f) { return (f?.rich_text || []).map(t => t.plain_text).join("").trim(); }
function extractSelect(f)    { return f?.select?.name || ""; }
function extractMultiSelect(f) { return (f?.multi_select || []).map(o => o.name); }

function buildRow(source, userId, page, blockText) {
  const props = page.properties || {};
  const title = extractTitle(props) || "（無題）";
  const meta  = [];

  if (source === "notion-inbox") {
    const g = extractMultiSelect(props["ジャンル"]), tp = extractMultiSelect(props["タイプ"]);
    if (g.length)  meta.push(`ジャンル: ${g.join(", ")}`);
    if (tp.length) meta.push(`タイプ: ${tp.join(", ")}`);
  } else if (source === "notion-input") {
    const st = extractSelect(props["source_type"]), tg = extractMultiSelect(props["topic_tag"]);
    if (st)        meta.push(`種別: ${st}`);
    if (tg.length) meta.push(`タグ: ${tg.join(", ")}`);
  } else if (source === "notion-output") {
    const md = extractMultiSelect(props["media"]), st = extractSelect(props["status"]);
    const tg = extractMultiSelect(props["topic_tag"]), hb = extractRichText(props["本文"]);
    if (st)        meta.push(`ステータス: ${st}`);
    if (md.length) meta.push(`メディア: ${md.join(", ")}`);
    if (tg.length) meta.push(`タグ: ${tg.join(", ")}`);
    if (hb)        meta.push(hb);
  } else if (source === "notion-asset") {
    const at = extractSelect(props["asset_type"]), tg = extractMultiSelect(props["topic_tag"]);
    if (at)        meta.push(`種別: ${at}`);
    if (tg.length) meta.push(`タグ: ${tg.join(", ")}`);
  } else if (source === "notion-project") {
    const st = extractSelect(props["status"]), ar = extractSelect(props["事業領域"]);
    const gl = extractRichText(props["goal"]);
    if (st) meta.push(`ステータス: ${st}`);
    if (ar) meta.push(`事業領域: ${ar}`);
    if (gl) meta.push(`ゴール: ${gl}`);
  }

  const parts = [];
  if (blockText)    parts.push(blockText);
  if (meta.length)  parts.push(meta.join("\n"));

  return {
    user_id:    userId,
    item_type:  "text",
    title,
    content:    parts.join("\n\n") || title,
    source_app: source,
    source_url: null,
    file_url:   null,
    metadata:   { notion_page_id: page.id },
    embedding:  null,
    folder_id:  null,
  };
}

// ─── Voyage AI ─────────────────────────────────────────────────────────────────

async function voyageEmbed(apiKey, texts) {
  const res = await fetch("https://api.voyageai.com/v1/embeddings", {
    method: "POST",
    headers: { "Authorization": `Bearer ${apiKey}`, "Content-Type": "application/json" },
    body: JSON.stringify({
      model: "voyage-3.5",
      input: texts.map(t => (t || "").slice(0, 120000)),
      input_type: "document",
      output_dimension: 1024,
    }),
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`Voyage ${res.status}: ${body.slice(0, 200)}`);
  }
  return (await res.json()).data.map(d => d.embedding);
}

// ─── Supabase ──────────────────────────────────────────────────────────────────

function supaConfig(env) {
  return {
    url: (env.VITE_SUPABASE_URL || "").replace(/\/$/, ""),
    key: env.VITE_SUPABASE_ANON_KEY,
  };
}

function supaAuthHeaders(key) {
  return { "Content-Type": "application/json", "apikey": key, "Authorization": `Bearer ${key}` };
}

async function supaDelete(env, table, filter) {
  const { url, key } = supaConfig(env);
  const res = await fetch(`${url}/rest/v1/${table}?${filter}`, {
    method: "DELETE", headers: supaAuthHeaders(key),
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`DELETE ${table}: ${res.status} ${body.slice(0, 200)}`);
  }
}

async function supaBulkInsert(env, table, rows) {
  if (!rows.length) return [];
  const { url, key } = supaConfig(env);
  const out = [];
  for (let i = 0; i < rows.length; i += SUPABASE_BATCH) {
    const batch = rows.slice(i, i + SUPABASE_BATCH);
    const res = await fetch(`${url}/rest/v1/${table}`, {
      method: "POST",
      headers: { ...supaAuthHeaders(key), "Prefer": "return=representation" },
      body: JSON.stringify(batch),
    });
    if (!res.ok) {
      const body = await res.text().catch(() => "");
      throw new Error(`INSERT ${table}: ${res.status} ${body.slice(0, 200)}`);
    }
    const d = await res.json();
    out.push(...(Array.isArray(d) ? d : [d]));
  }
  return out;
}

async function upsertProject(env, userId, name, description) {
  const { url, key } = supaConfig(env);
  // 既存チェック
  const res = await fetch(
    `${url}/rest/v1/zeus_projects?user_id=eq.${encodeURIComponent(userId)}&name=eq.${encodeURIComponent(name)}&select=id`,
    { headers: { ...supaAuthHeaders(key), "Prefer": "" } }
  );
  if (res.ok) {
    const existing = await res.json();
    if (existing.length > 0) return existing[0].id;
  }
  // 新規作成
  const ins = await fetch(`${url}/rest/v1/zeus_projects`, {
    method: "POST",
    headers: { ...supaAuthHeaders(key), "Prefer": "return=representation" },
    body: JSON.stringify({ user_id: userId, name, description }),
  });
  if (!ins.ok) {
    const body = await ins.text().catch(() => "");
    throw new Error(`INSERT zeus_projects: ${ins.status} ${body.slice(0, 200)}`);
  }
  const d = await ins.json();
  return (Array.isArray(d) ? d[0] : d).id;
}

// ─── ユーティリティ ────────────────────────────────────────────────────────────

function corsHeaders() {
  return {
    "Access-Control-Allow-Origin":  "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization",
  };
}

function json(obj, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "Content-Type": "application/json", ...corsHeaders() },
  });
}
