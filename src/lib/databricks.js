async function getOAuthToken({
  databricksHost,
  databricksClientId,
  databricksClientSecret
}) {
  const auth = Buffer.from(
    `${databricksClientId}:${databricksClientSecret}`
  ).toString("base64");

  const body = new URLSearchParams({
    grant_type: "client_credentials",
    scope: "all-apis"
  });

  const res = await fetch(`https://${databricksHost}/oidc/v1/token`, {
    method: "POST",
    headers: {
      Authorization: `Basic ${auth}`,
      "Content-Type": "application/x-www-form-urlencoded"
    },
    body: body.toString()
  });

  if (!res.ok) {
    throw new Error(`Databricks OAuth failed (${res.status}): ${await res.text()}`);
  }

  const payload = await res.json();
  return payload.access_token;
}

async function executeDatabricksSql(config, statement) {
  const token = await getOAuthToken(config);
  const warehouseId = config.databricksHttpPath.split("/").pop();

  const res = await fetch(`https://${config.databricksHost}/api/2.0/sql/statements`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      warehouse_id: warehouseId,
      statement,
      wait_timeout: "50s",
      disposition: "INLINE"
    })
  });

  if (!res.ok) {
    throw new Error(`Databricks SQL failed (${res.status}): ${await res.text()}`);
  }

  const payload = await res.json();
  if (payload.status?.state !== "SUCCEEDED") {
    throw new Error(`Databricks statement status: ${payload.status?.state || "UNKNOWN"}`);
  }

  return payload.result?.data_array || [];
}

module.exports = { executeDatabricksSql };
