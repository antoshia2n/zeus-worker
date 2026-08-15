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
 *   - 呼び出しの上限（subrequest）は「無し」ではない。2026-08-15 に公式の一覧で確認した
 *     実物は、1 回の呼び出しにつき有料 10,000 回・無料 50 回（設定で 10,000 は上げ下げ可）。
 *     Cloudflare の内側（KV など）への呼び出しは、プランによらず 1 回の呼び出しにつき 1,000 回。
 *     出どころ：https://developers.cloudflare.com/workers/platform/limits
 *     ここを Pages Functions ではなく Worker に置いているのは、上限が消えるからではなく、
 *     50 回より桁が大きいため。取り込み元 1 本が使うのは、いちばん多いアウトプットDB
 *     （126 件）でも 300 回台で、有料の 10,000 には届かない。
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
const BLOCK_CONCUR   = 5; // 同時に開いておける外への接続が 1 回の呼び出しにつき 6 本まで
                          // （有料・無料とも同じ）なので、その手前の 5 で止めている
const DELETE_BATCH   = 50; // 旧行の削除をURL長の安全な範囲に分割する

// ─── 実行記録 ─────────────────────────────────────────────────────────────────
// 2026-08-10 追加：取り込みが何件入ったかを、shia2n-mcp と同じ形の記録として残す。
//   置き場：shia2n-mcp と同じ KV。wrangler.jsonc で RUN_LOG_KV として同じ id を指す。
//   鍵の形：cronlog:{名前}（shia2n-mcp の cron-log.ts と同一）。
//   名前  ：zeus_import。shia2n-mcp 側の zeus_sync は「起動できた」の記録なので
//           置き換えず、別の欄として並べる（同じ鍵に2つの意味を入れない）。
// 記録の書き込みに失敗しても取り込みは止めない（記録は補助であって目的ではない）。

const RUN_LOG_JOB = "zeus_import";
// 2026-08-14 変更：5 → 15。
// 取り込み元ごとに 1 回ずつ実行を分けたため、1 日に 5 行が入るようになった。
// 5 件のままだと 1 日ぶんしか残らず、前の日と比べられない。15 件で 3 日ぶん残る。
// 読む側（shia2n-mcp の readAllRuns）は件数を切っていないので、こちらだけで決まる。
const RUN_LOG_MAX = 15;

async function appendRun(env, record) {
  try {
    if (!env.RUN_LOG_KV) {
      console.error("[zeus-worker] RUN_LOG_KV not bound; run log skipped");
      return;
    }
    const key  = `cronlog:${RUN_LOG_JOB}`;
    const raw  = await env.RUN_LOG_KV.get(key);
    const prev = raw ? JSON.parse(raw) : [];
    const next = [record, ...(Array.isArray(prev) ? prev : [])].slice(0, RUN_LOG_MAX);
    await env.RUN_LOG_KV.put(key, JSON.stringify(next));
  } catch (e) {
    console.error("[zeus-worker] failed to write run log:", e.message);
  }
}

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
      return handleSyncDb(request, env, ctx);
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
    SUPABASE_URL:  !!(env.SUPABASE_URL || env.VITE_SUPABASE_URL),
    SUPABASE_SERVICE_ROLE_KEY: !!env.SUPABASE_SERVICE_ROLE_KEY,
    MCP_DEFAULT_USER_ID: !!env.MCP_DEFAULT_USER_ID,
    // 2026-08-10 追加：実行記録の置き場がつながっているか
    RUN_LOG_KV:         !!env.RUN_LOG_KV,
  };
  const missing = Object.entries(checks).filter(([, v]) => !v).map(([k]) => k);
  return json({ ok: missing.length === 0, checks, missing });
}

// 1DB同期（Settings画面から呼ばれる。2026-08-14 から毎朝の取り込みもここを使う）
//
// 2026-08-14 追加：body に async: true を付けると、すぐ返事を返して後ろで処理する。
//   付けない呼び出し（Settings画面）はこれまでどおり、終わるまで待って件数を返す。
//   画面の動きを変えないために、既存の形は残したまま枝分かれさせている。
//
// 毎朝の取り込みを 1 本ずつに分けた理由（2026-08-14）：
//   5 本を 1 回の実行でまとめて処理していたため、1 回あたりの上限に収まらず、
//   アウトプットDB から先が入らないまま 2026-08-09 以降ずっと止まっていた。
//   1 本ずつ別の実行にすれば、1 本ぶんの重さで済み、どの本がどの理由で
//   落ちたかも 1 本ごとに記録へ残る。
async function handleSyncDb(request, env, ctx) {
  let body = {};
  try { body = await request.json(); } catch { /* 省略OK */ }

  const { source, user_id } = body;
  const uid = user_id || env.MCP_DEFAULT_USER_ID;
  if (!uid) return json({ error: "user_id required" }, 400);

  const db = NOTION_DBS.find(d => d.source === source);
  if (!db) return json({ error: `unknown source: ${source}` }, 400);

  const forceFull = body.force_full === true;

  // すぐ返す形（毎朝の取り込み）。記録はこちらの経路でだけ残す。
  // 画面からの手動実行まで記録に混ぜると、1 日 1 回の並びが読めなくなるため。
  if (body.async === true) {
    if (!ctx) return json({ error: "async not available here" }, 500);
    ctx.waitUntil(runOneDb(env, uid, db, forceFull));
    return json({ ok: true, message: "sync started", source, user_id: uid, force_full: forceFull });
  }

  // これまでどおり、終わるまで待って件数を返す形（Settings画面）
  try {
    const result = await syncOneDb(env, uid, db, forceFull);
    return json({ ok: true, ...result });
  } catch (e) {
    console.error(`[zeus-worker] sync-db ${source}:`, e.message);
    return json({ error: e.message }, 502);
  }
}

// 取り込み元 1 本ぶんを処理し、その 1 本の実行記録を残す（2026-08-14 追加）。
// 記録の書き込みに失敗しても取り込み自体は止めない（appendRun の中で受け止める）。
async function runOneDb(env, userId, db, forceFull = false) {
  const startedAt = Date.now();

  try {
    const result = await syncOneDb(env, userId, db, forceFull);
    const detail = result.skipped === "notion_empty"
      ? `${db.label}：Notion 側が 0 件のため入れ替えを中止（既存は保持）`
      : `${db.label} ${result.imported} 件（入れ替え前 ${result.replaced} 件）`;

    await appendRun(env, {
      at:          new Date().toISOString(),
      status:      "success",
      count:       result.imported,
      detail:      detail.slice(0, 400),
      duration_ms: Date.now() - startedAt,
    });
  } catch (e) {
    const reason = e instanceof Error ? e.message : String(e);
    console.error(`[zeus-worker] sync-one ${db.source}:`, reason);

    await appendRun(env, {
      at:          new Date().toISOString(),
      status:      "failure",
      count:       null,
      detail:      `${db.label}：${reason}`.slice(0, 400),
      duration_ms: Date.now() - startedAt,
    });
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
  const startedAt = Date.now();
  const userId    = uid || env.MCP_DEFAULT_USER_ID;

  if (!userId) {
    console.error("[zeus-worker] MCP_DEFAULT_USER_ID not set");
    await appendRun(env, {
      at:          new Date().toISOString(),
      status:      "failure",
      count:       null,
      detail:      "利用者の指定が無いため取り込みを行いませんでした（MCP_DEFAULT_USER_ID 未設定）",
      duration_ms: Date.now() - startedAt,
    });
    return;
  }

  const bySource = {};
  const failed   = [];
  let   total    = 0;

  for (const db of NOTION_DBS) {
    try {
      const result   = await syncOneDb(env, userId, db, forceFull);
      bySource[db.source] = result.imported;
      total              += result.imported;
    } catch (e) {
      console.error(`[zeus-worker] sync-all ${db.source}:`, e.message);
      bySource[db.source] = { error: e.message };
      failed.push(`${db.label}：${String(e.message).slice(0, 120)}`);
    }
  }

  console.log(`[zeus-worker] sync-all done. total=${total}`, JSON.stringify(bySource));

  // 5本それぞれの件数を並べる。1本でも失敗があれば failure にする
  // （成功と表示されると、欠けたまま気づかないため）。
  const breakdown = NOTION_DBS
    .map((d) => {
      const v = bySource[d.source];
      return `${d.label} ${typeof v === "number" ? `${v} 件` : "失敗"}`;
    })
    .join(" / ");

  const detail = failed.length === 0
    ? `取り込み ${total} 件（${breakdown}）`
    : `取り込み ${total} 件（${breakdown}）。失敗 ${failed.length} 本：${failed.join(" ／ ")}`;

  await appendRun(env, {
    at:          new Date().toISOString(),
    status:      failed.length === 0 ? "success" : "failure",
    count:       total,
    detail:      detail.slice(0, 400),
    duration_ms: Date.now() - startedAt,
  });
}

async function syncOneDb(env, userId, db, forceFull = false) {
  const { source, dbId, label, skipBlocks } = db;
  const filter = `source_app=eq.${source}&user_id=eq.${encodeURIComponent(userId)}`;

  // 1. Notionページ全件取得（削除より先に行う）
  //    取得に失敗すると上位の catch へ抜けるため、既存データは触られないまま残る。
  const pages = await notionAllPages(env.NOTION_API_KEY, dbId);

  // 2. 0件なら入れ替えを中止する（既存を保持）
  //    Notion側の一時的な不調と「本当に0件」を区別できないため、消さない側に倒す。
  if (pages.length === 0) {
    console.warn(`[zeus-worker] ${source}: Notion 0件のため入れ替えを中止（既存は保持）`);
    return { source, imported: 0, skipped: "notion_empty" };
  }

  // 3. ブロック本文取得（skipBlocks=falseのみ）
  const blockMap = skipBlocks
    ? new Map()
    : await notionBlockMap(env.NOTION_API_KEY, pages.map(p => p.id));

  // 4. zeus_items 行構築
  const rows = pages.map(p => buildRow(source, userId, p, blockMap.get(p.id) || ""));

  // 5. Voyage AI バッチ Embedding
  //
  // 2026-08-15 変更：ベクトルを作る呼び出しが失敗したときに、そのまま先へ進むのをやめた。
  //   これまでは失敗した分を null のままにして 6 以降へ進み、空の行を新しく入れてから
  //   ベクトルの入っていた古い行を消していた。呼び出し元には件数が返るため、
  //   記録には「成功」と残り、中身だけが空に置き換わっていた。
  //   2026-08-15 03:00 の inbox 18 件がこの経路で空になっている。
  //   ここで throw すると 6 以降の書き込みと削除に一切入らないため、既存の行は
  //   そのまま残る。呼び出し元（runOneDb）が失敗として記録し、下の理由の文字列も
  //   その記録に入る（これまで理由は画面に出ないところにしか残っていなかった）。
  //   2 の「Notion 側が 0 件なら入れ替えを中止」と同じ考え方で、消さない側に倒している。
  const embedErrors = [];
  for (let i = 0; i < rows.length; i += VOYAGE_BATCH) {
    const batch = rows.slice(i, i + VOYAGE_BATCH);
    try {
      const embs = await voyageEmbed(env.VOYAGE_API_KEY, batch.map(r => `${r.title}\n\n${r.content}`));
      batch.forEach((r, idx) => { r.embedding = embs[idx]; });
    } catch (e) {
      const reason = e instanceof Error ? e.message : String(e);
      console.error(`[zeus-worker] embed ${source} offset ${i}:`, reason);
      embedErrors.push(`${i + 1}件目から${batch.length}件：${reason}`);
    }
  }

  if (embedErrors.length > 0) {
    throw new Error(
      `ベクトルを作れなかったため入れ替えを中止（既存は保持）。${embedErrors.length}組：${embedErrors.join(" ／ ")}`
    );
  }

  // 例外は出ていないが中身が欠けている場合も、入れ替えに進まない。
  // 返ってきた数が足りないと、末尾の行のベクトルが undefined のまま残るため。
  const embedMissing = rows.filter(r => !Array.isArray(r.embedding) || r.embedding.length === 0).length;
  if (embedMissing > 0) {
    throw new Error(
      `ベクトルの入っていない行が ${embedMissing} 件あるため入れ替えを中止（既存は保持）`
    );
  }

  // 6. 入れ替え対象の既存IDを控える（削除はINSERT成功後）
  const oldIds = await supaSelectIds(env, "zeus_items", filter);

  // 7. zeus_items 一括INSERT
  const projectId   = await upsertProject(env, userId, source, `Notionナレッジ: ${label}`);
  const insertedItems = await supaBulkInsert(env, "zeus_items", rows);

  // 8. zeus_item_projects 一括INSERT
  await supaBulkInsert(env, "zeus_item_projects",
    insertedItems.map(r => ({ item_id: r.id, project_id: projectId })));

  // 9. 旧行を削除（ここまで来た＝新しい行は入っている）
  //    zeus_item_projects は ON DELETE CASCADE のため紐付けも同時に消える。
  await supaDeleteByIds(env, "zeus_items", oldIds);

  return { source, imported: pages.length, replaced: oldIds.length };
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
    if (!res.ok) {
      const body = await res.text().catch(() => "");
      throw new Error(`Notion DB ${dbId}: ${res.status} ${body.slice(0, 200)}`);
    }
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

// HTMLタグ・エンティティを除去してプレーンテキストに変換
function stripHtml(str) {
  return str
    .replace(/<br\s*\/?>/gi, "\n")        // <br> → 改行
    .replace(/<\/p>/gi, "\n")             // </p> → 改行
    .replace(/<[^>]+>/g, "")             // 残りのタグを除去
    .replace(/&nbsp;/g, " ")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/\n{3,}/g, "\n\n")          // 連続改行を最大2つに
    .trim();
}

function extractTitle(props) {
  for (const v of Object.values(props)) {
    if (v?.type === "title") return (v.title || []).map(t => t.plain_text).join("").trim();
  }
  return "";
}
function extractRichText(f) { return stripHtml((f?.rich_text || []).map(t => t.plain_text).join("").trim()); }
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
  if (blockText)    parts.push(stripHtml(blockText));
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

// 2026-07-31 変更：公開キーから管理者キーへ切り替え。
// 正本：2026-07-30 決定「画面は公開キーでデータベースに直接触らない」
// ここはブラウザではなくサーバー側（毎晩の取り込み処理）なので管理者キーを使う場所。
// 公開キーへの自動フォールバックは入れない（権限を外した瞬間に黙って止まるため）。
function supaConfig(env) {
  const url = (env.SUPABASE_URL || env.VITE_SUPABASE_URL || "").replace(/\/$/, "");
  const key = env.SUPABASE_SERVICE_ROLE_KEY;
  if (!url) throw new Error("Supabase URL not configured (SUPABASE_URL / VITE_SUPABASE_URL)");
  if (!key) throw new Error("SUPABASE_SERVICE_ROLE_KEY not configured");
  return { url, key };
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

async function supaSelectIds(env, table, filter) {
  const { url, key } = supaConfig(env);
  const res = await fetch(`${url}/rest/v1/${table}?${filter}&select=id`, {
    headers: supaAuthHeaders(key),
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`SELECT ${table}: ${res.status} ${body.slice(0, 200)}`);
  }
  const d = await res.json();
  return (Array.isArray(d) ? d : []).map(r => r.id);
}

async function supaDeleteByIds(env, table, ids) {
  if (!ids.length) return;
  for (let i = 0; i < ids.length; i += DELETE_BATCH) {
    const chunk = ids.slice(i, i + DELETE_BATCH);
    await supaDelete(env, table, `id=in.(${chunk.join(",")})`);
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
