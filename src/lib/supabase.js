function baseHeaders(supabaseKey) {
  return {
    apikey: supabaseKey,
    Authorization: `Bearer ${supabaseKey}`,
    "Content-Type": "application/json"
  };
}

async function supabaseGet(config, table, params = {}, headers = {}) {
  const url = new URL(`${config.supabaseUrl}/rest/v1/${table}`);
  Object.entries(params).forEach(([k, v]) => url.searchParams.set(k, v));
  return fetch(url, { headers: { ...baseHeaders(config.supabaseKey), ...headers } });
}

async function supabasePost(config, table, rows) {
  return fetch(`${config.supabaseUrl}/rest/v1/${table}`, {
    method: "POST",
    headers: {
      ...baseHeaders(config.supabaseKey),
      Prefer: "resolution=ignore-duplicates,return=minimal"
    },
    body: JSON.stringify(rows)
  });
}

async function supabaseUpsert(config, table, rows) {
  return fetch(`${config.supabaseUrl}/rest/v1/${table}`, {
    method: "POST",
    headers: {
      ...baseHeaders(config.supabaseKey),
      Prefer: "resolution=merge-duplicates"
    },
    body: JSON.stringify(rows)
  });
}

async function getExistingCodigos(config) {
  const codigos = new Set();
  let offset = 0;
  const batch = 1000;

  while (true) {
    const res = await supabaseGet(
      config,
      config.supabaseTable,
      { select: "codigo_transportista" },
      { Range: `${offset}-${offset + batch - 1}` }
    );
    if (![200, 206].includes(res.status)) {
      throw new Error(`Failed to read existing codigos: ${res.status}`);
    }
    const data = await res.json();
    if (!data.length) break;
    data.forEach((row) => codigos.add(row.codigo_transportista));
    if (data.length < batch) break;
    offset += batch;
  }

  return codigos;
}

module.exports = {
  supabaseGet,
  supabasePost,
  supabaseUpsert,
  getExistingCodigos
};
