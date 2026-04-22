const { executeDatabricksSql } = require("../lib/databricks");
const { generateTransportistaId } = require("../lib/idGenerator");
const {
  supabaseGet,
  supabasePost,
  supabaseUpsert,
  getExistingCodigos
} = require("../lib/supabase");

function cleanUniqueField(value) {
  if (value === null || value === undefined) return null;
  const s = String(value).trim();
  return s || null;
}

function deduplicateRows(rows) {
  const seen = new Map();
  rows.forEach((row) => {
    const codigo = row[0] ? String(row[0]).trim() : null;
    if (codigo) seen.set(codigo, row);
  });
  return [...seen.values()];
}

function splitNewRows(rows, existingCodigos, now) {
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
        _ingested_at: now
      });
    }
  }

  return newRows;
}

async function runSync(config) {
  const now = new Date().toISOString();

  const tableCheck = await supabaseGet(config, config.supabaseTable, {
    select: "codigo_transportista",
    limit: "1"
  });
  if (tableCheck.status !== 200) {
    throw new Error(`Cannot access table ${config.supabaseTable}: ${tableCheck.status}`);
  }

  const watermarkCheck = await supabaseGet(config, "etl_watermarks", {
    select: "table_name",
    limit: "1"
  });
  if (watermarkCheck.status !== 200) {
    throw new Error(`Cannot access table etl_watermarks: ${watermarkCheck.status}`);
  }

  const existing = await getExistingCodigos(config);
  const countBefore = existing.size;

  const statement = `
    SELECT
      codigo_transportista, ruc, nombre_transportista,
      telefono, email, estado_transportista
    FROM ${config.dbxTable}
  `;
  let dbxRows = await executeDatabricksSql(config, statement);
  dbxRows = deduplicateRows(dbxRows);

  const newRows = splitNewRows(dbxRows, existing, now);
  let inserted = 0;

  for (let i = 0; i < newRows.length; i += config.batchSize) {
    const batch = newRows.slice(i, i + config.batchSize);
    const res = await supabasePost(config, config.supabaseTable, batch);
    if (![200, 201].includes(res.status)) {
      throw new Error(`Insert failed: ${res.status} ${(await res.text()).slice(0, 300)}`);
    }
    inserted += batch.length;
  }

  const postCodigos = await getExistingCodigos(config);
  const countAfter = postCodigos.size;
  const syncTimestamp = new Date().toISOString();

  await supabaseUpsert(config, "etl_watermarks", [
    { table_name: config.supabaseTable, last_timestamp: syncTimestamp }
  ]);

  return {
    status: "success",
    inserted,
    total: countAfter,
    source_table: config.dbxTable,
    dest_table: config.supabaseTable,
    source_count: dbxRows.length,
    dest_count_before: countBefore,
    dest_count_after: countAfter,
    skipped: dbxRows.length - newRows.length,
    sync_timestamp: syncTimestamp,
    inserted_records: newRows.map((r) => ({
      codigo_transportista: r.codigo_transportista,
      ruc: r.ruc,
      nombre_transportista: r.nombre_transportista
    }))
  };
}

module.exports = { runSync };
