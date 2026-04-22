const { S3Client, PutObjectCommand } = require("@aws-sdk/client-s3");
const { v5: uuidv5 } = require("uuid");

const DATABRICKS_HOST = process.env.DATABRICKS_HOST || "";
const DATABRICKS_HTTP_PATH = process.env.DATABRICKS_HTTP_PATH || "";
const DATABRICKS_CLIENT_ID = process.env.DATABRICKS_CLIENT_ID || "";
const DATABRICKS_CLIENT_SECRET = process.env.DATABRICKS_CLIENT_SECRET || "";

const SUPABASE_URL = (process.env.SUPABASE_URL || "").replace(/\/$/, "");
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || "";

const DBX_TABLE =
  process.env.DBX_TABLE || "prod.gldlogistica.db_trade_dim_app_transportistas";
const SUPABASE_TABLE = process.env.SUPABASE_TABLE || "transportistas";
const WATERMARK_TABLE = "etl_watermarks";
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || "500", 10);
const ETL_LOGS_BUCKET = process.env.ETL_LOGS_BUCKET || "";
const UUID_NAMESPACE = "b8f9e3a1-7c2d-4f5e-9a1b-3c4d5e6f7a8b";

const s3Client = new S3Client({});

function logInfo(message, extra) {
  if (extra !== undefined) {
    console.log(`${message}: ${JSON.stringify(extra)}`);
  } else {
    console.log(message);
  }
}

function getSupabaseHeaders(preferHeader) {
  const headers = {
    apikey: SUPABASE_KEY,
    Authorization: `Bearer ${SUPABASE_KEY}`,
    "Content-Type": "application/json",
  };
  if (preferHeader) headers.Prefer = preferHeader;
  return headers;
}

function generateTransportistaId(codigoTransportista) {
  return uuidv5(String(codigoTransportista).trim(), UUID_NAMESPACE);
}

function cleanUniqueField(value) {
  if (value === null || value === undefined) return null;
  const s = String(value).trim();
  return s || null;
}

function deduplicateRows(rows) {
  const seen = new Map();
  for (const row of rows) {
    const codigo = row[0] ? String(row[0]).trim() : null;
    if (codigo) {
      seen.set(codigo, row);
    }
  }
  const dupes = rows.length - seen.size;
  if (dupes > 0) {
    console.warn(`Removed ${dupes} duplicate codigo_transportista from Databricks data.`);
  }
  return [...seen.values()];
}

async function getDatabricksOAuthToken() {
  const tokenUrl = `https://${DATABRICKS_HOST}/oidc/v1/token`;
  const auth = Buffer.from(`${DATABRICKS_CLIENT_ID}:${DATABRICKS_CLIENT_SECRET}`).toString(
    "base64"
  );
  const body = new URLSearchParams({
    grant_type: "client_credentials",
    scope: "all-apis",
  });

  const res = await fetch(tokenUrl, {
    method: "POST",
    headers: {
      Authorization: `Basic ${auth}`,
      "Content-Type": "application/x-www-form-urlencoded",
    },
    body: body.toString(),
  });

  if (!res.ok) {
    const txt = await res.text();
    throw new Error(`Databricks OAuth failed (${res.status}): ${txt.slice(0, 300)}`);
  }

  const data = await res.json();
  return data.access_token;
}

async function fetchDatabricks() {
  const statement = `
    SELECT
      codigo_transportista, ruc, nombre_transportista,
      telefono, email, estado_transportista
    FROM ${DBX_TABLE}
  `;

  logInfo(`Querying Databricks PRD`, { table: DBX_TABLE });
  const token = await getDatabricksOAuthToken();
  const res = await fetch(`https://${DATABRICKS_HOST}/api/2.0/sql/statements`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      warehouse_id: DATABRICKS_HTTP_PATH.split("/").pop(),
      statement,
      wait_timeout: "50s",
      disposition: "INLINE",
    }),
  });

  if (!res.ok) {
    const txt = await res.text();
    throw new Error(`Databricks query failed (${res.status}): ${txt.slice(0, 300)}`);
  }

  const payload = await res.json();
  if (payload.status?.state !== "SUCCEEDED") {
    throw new Error(`Databricks statement status: ${payload.status?.state || "UNKNOWN"}`);
  }

  const rows = payload.result?.data_array || [];
  logInfo(`Fetched ${rows.length} row(s) from Databricks.`);
  return rows;
}

async function supabaseGet(table, params = {}, headers = {}) {
  const url = new URL(`${SUPABASE_URL}/rest/v1/${table}`);
  Object.entries(params).forEach(([k, v]) => url.searchParams.set(k, v));
  return fetch(url, {
    headers: { ...getSupabaseHeaders(), ...headers },
  });
}

async function supabasePost(table, rows) {
  return fetch(`${SUPABASE_URL}/rest/v1/${table}`, {
    method: "POST",
    headers: getSupabaseHeaders("resolution=ignore-duplicates,return=minimal"),
    body: JSON.stringify(rows),
  });
}

async function supabaseUpsert(table, rows) {
  return fetch(`${SUPABASE_URL}/rest/v1/${table}`, {
    method: "POST",
    headers: getSupabaseHeaders("resolution=merge-duplicates"),
    body: JSON.stringify(rows),
  });
}

async function getExistingCodigos() {
  const codigos = new Set();
  let offset = 0;
  const batch = 1000;

  while (true) {
    const res = await supabaseGet(
      SUPABASE_TABLE,
      { select: "codigo_transportista" },
      { Range: `${offset}-${offset + batch - 1}` }
    );

    if (![200, 206].includes(res.status)) {
      throw new Error(`Failed to read existing codes: ${res.status}`);
    }

    const data = await res.json();
    if (!data.length) break;
    for (const row of data) codigos.add(row.codigo_transportista);
    if (data.length < batch) break;
    offset += batch;
  }

  return codigos;
}

function splitNewAndExisting(rows, existingCodigos, now) {
  const newRows = [];
  const seenEmails = new Set();
  const seenPhones = new Set();

  for (const row of rows) {
    const codigo = row[0] ? String(row[0]).trim() : null;
    const ruc = row[1] ? String(row[1]).trim() : null;
    if (!codigo || !ruc) continue;

    let email = cleanUniqueField(row[4]);
    let telefono = cleanUniqueField(row[3]);

    if (email && seenEmails.has(email)) email = null;
    if (telefono && seenPhones.has(telefono)) telefono = null;

    if (email) seenEmails.add(email);
    if (telefono) seenPhones.add(telefono);

    if (!existingCodigos.has(codigo)) {
      newRows.push({
        transportista_id: generateTransportistaId(codigo),
        codigo_transportista: codigo,
        ruc,
        nombre_transportista: row[2],
        telefono,
        email,
        estado_transportista: "pendiente",
        _ingested_at: now,
      });
    }
  }

  return newRows;
}

async function saveEtlLog(logData) {
  if (!ETL_LOGS_BUCKET) {
    console.warn("ETL_LOGS_BUCKET not set, skipping S3 log.");
    return null;
  }

  const ts = logData.sync_timestamp || new Date().toISOString();
  const datePrefix = ts.slice(0, 10);
  const key = `transportistas/${datePrefix}/${ts.replace(/:/g, "-")}.json`;

  try {
    await s3Client.send(
      new PutObjectCommand({
        Bucket: ETL_LOGS_BUCKET,
        Key: key,
        Body: JSON.stringify(logData, null, 2),
        ContentType: "application/json",
      })
    );
    const uri = `s3://${ETL_LOGS_BUCKET}/${key}`;
    logInfo("ETL log saved", { uri });
    return uri;
  } catch (err) {
    console.error("Failed to save ETL log to S3", err);
    return null;
  }
}

async function runSync() {
  const syncStart = new Date();
  const now = syncStart.toISOString();

  const tableRes = await supabaseGet(SUPABASE_TABLE, {
    select: "codigo_transportista",
    limit: "1",
  });
  if (tableRes.status !== 200) {
    throw new Error(`Cannot access '${SUPABASE_TABLE}': ${tableRes.status}`);
  }

  const wmRes = await supabaseGet(WATERMARK_TABLE, {
    select: "table_name",
    limit: "1",
  });
  if (wmRes.status !== 200) {
    throw new Error(`Cannot access '${WATERMARK_TABLE}': ${wmRes.status}`);
  }

  const existing = await getExistingCodigos();
  const countBefore = existing.size;
  logInfo(`Found ${countBefore} existing records in Supabase.`);

  let dbxRows = await fetchDatabricks();
  if (!dbxRows.length) {
    return { status: "no_data", inserted: 0, total: countBefore };
  }
  dbxRows = deduplicateRows(dbxRows);

  const newRows = splitNewAndExisting(dbxRows, existing, now);
  logInfo("Sync split", {
    new: newRows.length,
    skipped: dbxRows.length - newRows.length,
  });

  let inserted = 0;
  for (let i = 0; i < newRows.length; i += BATCH_SIZE) {
    const batch = newRows.slice(i, i + BATCH_SIZE);
    const res = await supabasePost(SUPABASE_TABLE, batch);
    if (![200, 201].includes(res.status)) {
      const txt = await res.text();
      throw new Error(`Insert failed: ${res.status} ${txt.slice(0, 300)}`);
    }
    inserted += batch.length;
    logInfo(`Inserted batch ${Math.floor(i / BATCH_SIZE) + 1}`, { size: batch.length });
  }

  const postCodigos = await getExistingCodigos();
  const countAfter = postCodigos.size;
  const syncTime = new Date().toISOString();

  const upsertRes = await supabaseUpsert(WATERMARK_TABLE, [
    { table_name: SUPABASE_TABLE, last_timestamp: syncTime },
  ]);
  if (![200, 201].includes(upsertRes.status)) {
    console.warn(`Watermark update failed: ${upsertRes.status}`);
  }

  const detail = {
    status: "success",
    inserted,
    total: countAfter,
    source_table: DBX_TABLE,
    dest_table: SUPABASE_TABLE,
    source_count: dbxRows.length,
    dest_count_before: countBefore,
    dest_count_after: countAfter,
    skipped: dbxRows.length - newRows.length,
    sync_timestamp: syncTime,
    inserted_records: newRows.map((r) => ({
      codigo_transportista: r.codigo_transportista,
      ruc: r.ruc,
      nombre_transportista: r.nombre_transportista,
    })),
  };
  await saveEtlLog(detail);

  return {
    status: "success",
    inserted,
    total: countAfter,
    sync_timestamp: syncTime,
  };
}

exports.lambdaHandler = async (event) => {
  logInfo("Event received", event);
  try {
    const result = await runSync();
    logInfo("Sync complete", result);
    return {
      statusCode: 200,
      body: JSON.stringify(result),
    };
  } catch (error) {
    console.error("Sync failed", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ status: "error", message: String(error.message || error) }),
    };
  }
};
