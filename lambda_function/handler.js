const fs = require("fs");
const path = require("path");
const { S3Client, PutObjectCommand } = require("@aws-sdk/client-s3");
const { v5: uuidv5 } = require("uuid");

const DATABRICKS_HOST = process.env.DATABRICKS_HOST || "";
const DATABRICKS_HTTP_PATH = process.env.DATABRICKS_HTTP_PATH || "";
const DATABRICKS_CLIENT_ID = process.env.DATABRICKS_CLIENT_ID || "";
const DATABRICKS_CLIENT_SECRET = process.env.DATABRICKS_CLIENT_SECRET || "";

const SUPABASE_URL = (process.env.SUPABASE_URL || "").replace(/\/$/, "");
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || "";

const WATERMARK_TABLE = "etl_watermarks";
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || "500", 10);
const ETL_LOGS_BUCKET = process.env.ETL_LOGS_BUCKET || "";
const UUID_NAMESPACE = "b8f9e3a1-7c2d-4f5e-9a1b-3c4d5e6f7a8b";
const PIPELINES_CONFIG_PATH =
  process.env.PIPELINES_CONFIG_PATH || path.join(__dirname, "pipelines.json");

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

function loadPipelinesConfig() {
  const raw = fs.readFileSync(PIPELINES_CONFIG_PATH, "utf8");
  return JSON.parse(raw);
}

function getPipelineConfig(event) {
  const pipelineName = event?.pipeline_name || process.env.DEFAULT_PIPELINE || "transportistas";
  const all = loadPipelinesConfig();
  const cfg = all[pipelineName];
  if (!cfg) {
    throw new Error(`Unknown pipeline_name '${pipelineName}'`);
  }
  return cfg;
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

async function fetchDatabricks(pipelineConfig) {
  const sourceColumns = pipelineConfig.column_mapping.map((m) => m.source).join(", ");
  const statement = `SELECT ${sourceColumns} FROM ${pipelineConfig.source_table}`;

  logInfo(`Querying Databricks PRD`, { table: pipelineConfig.source_table });
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

async function supabaseUpsert(table, rows, conflictKey = null) {
  const url = new URL(`${SUPABASE_URL}/rest/v1/${table}`);
  if (conflictKey) {
    url.searchParams.set("on_conflict", conflictKey);
  }
  return fetch(url, {
    method: "POST",
    headers: getSupabaseHeaders("resolution=merge-duplicates"),
    body: JSON.stringify(rows),
  });
}

async function getExistingKeys(targetTable, conflictKey) {
  const codigos = new Set();
  let offset = 0;
  const batch = 1000;

  while (true) {
    const res = await supabaseGet(
      targetTable,
      { select: conflictKey },
      { Range: `${offset}-${offset + batch - 1}` }
    );

    if (![200, 206].includes(res.status)) {
      throw new Error(`Failed to read existing codes: ${res.status}`);
    }

    const data = await res.json();
    if (!data.length) break;
    for (const row of data) codigos.add(row[conflictKey]);
    if (data.length < batch) break;
    offset += batch;
  }

  return codigos;
}

function splitRowsByMode(rows, pipelineConfig, existingKeys, now) {
  const newRows = [];
  const allRows = [];
  const seenEmails = new Set();
  const seenPhones = new Set();
  const sourceIndex = Object.fromEntries(
    pipelineConfig.column_mapping.map((m, idx) => [m.source, idx])
  );
  const conflictKey = pipelineConfig.conflict_key;

  for (const row of rows) {
    const codigo = row[sourceIndex.codigo_transportista]
      ? String(row[sourceIndex.codigo_transportista]).trim()
      : null;
    const ruc = row[sourceIndex.ruc] ? String(row[sourceIndex.ruc]).trim() : null;
    if (!codigo || !ruc) continue;

    let email =
      sourceIndex.email !== undefined ? cleanUniqueField(row[sourceIndex.email]) : null;
    let telefono =
      sourceIndex.telefono !== undefined
        ? cleanUniqueField(row[sourceIndex.telefono])
        : null;

    if (email && seenEmails.has(email)) email = null;
    if (telefono && seenPhones.has(telefono)) telefono = null;

    if (email) seenEmails.add(email);
    if (telefono) seenPhones.add(telefono);

    const record = {};
    for (const mapping of pipelineConfig.column_mapping) {
      const idx = sourceIndex[mapping.source];
      let value = idx !== undefined ? row[idx] : null;
      if (mapping.target === "email") value = email;
      if (mapping.target === "telefono") value = telefono;
      record[mapping.target] = value;
    }
    if (pipelineConfig.defaults) {
      Object.assign(record, pipelineConfig.defaults);
    }
    if (pipelineConfig.id_strategy?.type === "uuid5_codigo_transportista") {
      record[pipelineConfig.id_strategy.column] = generateTransportistaId(codigo);
    }
    record._ingested_at = now;

    allRows.push(record);
    if (!existingKeys.has(record[conflictKey])) {
      newRows.push(record);
    }
  }

  return { newRows, allRows };
}

async function saveEtlLog(logData) {
  if (!ETL_LOGS_BUCKET) {
    console.warn("ETL_LOGS_BUCKET not set, skipping S3 log.");
    return null;
  }

  const ts = logData.sync_timestamp || new Date().toISOString();
  const datePrefix = ts.slice(0, 10);
  const pipelineName = logData.pipeline_name || "unknown";
  const key = `${pipelineName}/${datePrefix}/${ts.replace(/:/g, "-")}.json`;

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
  throw new Error("runSync(event) must be called with pipeline config");
}

async function createRunRecord(pipelineName, status, payload = {}) {
  const row = {
    pipeline_name: pipelineName,
    status,
    source_table: payload.source_table || null,
    target_table: payload.dest_table || null,
    inserted_count: payload.inserted || 0,
    updated_count: payload.updated || 0,
    skipped_count: payload.skipped || 0,
    error_message: payload.error_message || null,
    started_at: payload.started_at || new Date().toISOString(),
    finished_at: payload.finished_at || new Date().toISOString(),
    duration_ms: payload.duration_ms || null,
    s3_log_uri: payload.s3_log_uri || null
  };
  try {
    const res = await supabasePost("etl_runs", [row]);
    if (![200, 201].includes(res.status)) {
      const txt = await res.text();
      console.warn(`etl_runs insert skipped (${res.status}): ${txt.slice(0, 200)}`);
    }
  } catch (err) {
    console.warn(`etl_runs insert skipped: ${String(err.message || err)}`);
  }
}

async function runSyncForPipeline(event) {
  const startedAt = new Date();
  const pipelineConfig = getPipelineConfig(event);
  const pipelineName = pipelineConfig.pipeline_name;
  const syncStart = new Date();
  const now = syncStart.toISOString();
  const sourceTable = pipelineConfig.source_table;
  const targetTable = pipelineConfig.target_table;
  const conflictKey = pipelineConfig.conflict_key;
  const batchSize = pipelineConfig.batch_size || BATCH_SIZE;
  const writeMode = pipelineConfig.write_mode || "insert_only";

  const tableRes = await supabaseGet(targetTable, {
    select: conflictKey,
    limit: "1",
  });
  if (tableRes.status !== 200) {
    throw new Error(`Cannot access '${targetTable}': ${tableRes.status}`);
  }

  const wmRes = await supabaseGet(WATERMARK_TABLE, {
    select: "table_name",
    limit: "1",
  });
  if (wmRes.status !== 200) {
    throw new Error(`Cannot access '${WATERMARK_TABLE}': ${wmRes.status}`);
  }

  const existing = await getExistingKeys(targetTable, conflictKey);
  const countBefore = existing.size;
  logInfo(`Found ${countBefore} existing records in Supabase.`, { targetTable, pipelineName });

  let dbxRows = await fetchDatabricks(pipelineConfig);
  if (!dbxRows.length) {
    return { status: "no_data", inserted: 0, total: countBefore, pipeline_name: pipelineName };
  }
  dbxRows = deduplicateRows(dbxRows);

  const { newRows, allRows } = splitRowsByMode(dbxRows, pipelineConfig, existing, now);
  logInfo("Sync split", {
    pipeline: pipelineName,
    new: newRows.length,
    skipped: dbxRows.length - newRows.length,
  });

  let inserted = 0;
  let updated = 0;
  const rowsToWrite = writeMode === "upsert" ? allRows : newRows;
  for (let i = 0; i < rowsToWrite.length; i += batchSize) {
    const batch = rowsToWrite.slice(i, i + batchSize);
    const res =
      writeMode === "upsert"
        ? await supabaseUpsert(targetTable, batch, conflictKey)
        : await supabasePost(targetTable, batch);
    if (![200, 201].includes(res.status)) {
      const txt = await res.text();
      throw new Error(`Insert failed: ${res.status} ${txt.slice(0, 300)}`);
    }
    if (writeMode === "upsert") {
      const insertedInBatch = batch.filter((r) => !existing.has(r[conflictKey])).length;
      inserted += insertedInBatch;
      updated += batch.length - insertedInBatch;
    } else {
      inserted += batch.length;
    }
    logInfo(`Written batch ${Math.floor(i / batchSize) + 1}`, {
      mode: writeMode,
      size: batch.length
    });
  }

  const postCodigos = await getExistingKeys(targetTable, conflictKey);
  const countAfter = postCodigos.size;
  const syncTime = new Date().toISOString();

  const upsertRes = await supabaseUpsert(WATERMARK_TABLE, [
    { table_name: pipelineName, last_timestamp: syncTime },
  ]);
  if (![200, 201].includes(upsertRes.status)) {
    console.warn(`Watermark update failed: ${upsertRes.status}`);
  }

  const detail = {
    pipeline_name: pipelineName,
    status: "success",
    inserted,
    updated,
    total: countAfter,
    source_table: sourceTable,
    dest_table: targetTable,
    write_mode: writeMode,
    conflict_key: conflictKey,
    source_count: dbxRows.length,
    dest_count_before: countBefore,
    dest_count_after: countAfter,
    skipped: dbxRows.length - rowsToWrite.length,
    sync_timestamp: syncTime,
    inserted_records: newRows.slice(0, 200).map((r) => ({
      [conflictKey]: r[conflictKey]
    })),
  };
  const s3LogUri = await saveEtlLog(detail);
  const finishedAt = new Date();
  await createRunRecord(pipelineName, "success", {
    ...detail,
    started_at: startedAt.toISOString(),
    finished_at: finishedAt.toISOString(),
    duration_ms: finishedAt.getTime() - startedAt.getTime(),
    s3_log_uri: s3LogUri
  });

  return {
    pipeline_name: pipelineName,
    status: "success",
    inserted,
    updated,
    total: countAfter,
    sync_timestamp: syncTime,
  };
}

exports.lambdaHandler = async (event) => {
  logInfo("Event received", event);
  try {
    const result = await runSyncForPipeline(event || {});
    logInfo("Sync complete", result);
    return {
      statusCode: 200,
      body: JSON.stringify(result),
    };
  } catch (error) {
    console.error("Sync failed", error);
    try {
      const pipelineName = event?.pipeline_name || process.env.DEFAULT_PIPELINE || "transportistas";
      await createRunRecord(pipelineName, "error", {
        error_message: String(error.message || error)
      });
    } catch (inner) {
      console.error("Failed writing etl_runs error row", inner);
    }
    return {
      statusCode: 500,
      body: JSON.stringify({ status: "error", message: String(error.message || error) }),
    };
  }
};
