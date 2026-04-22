import { appConfig } from "./config";

async function getToken() {
  const auth = Buffer.from(
    `${appConfig.databricksClientId}:${appConfig.databricksClientSecret}`
  ).toString("base64");
  const body = new URLSearchParams({
    grant_type: "client_credentials",
    scope: "all-apis",
  });

  const res = await fetch(`https://${appConfig.databricksHost}/oidc/v1/token`, {
    method: "POST",
    headers: {
      Authorization: `Basic ${auth}`,
      "Content-Type": "application/x-www-form-urlencoded",
    },
    body: body.toString(),
    cache: "no-store",
  });
  if (!res.ok) throw new Error(`Databricks OAuth failed (${res.status})`);
  return (await res.json()).access_token as string;
}

export async function countSourceRows(sourceTable: string): Promise<number> {
  const token = await getToken();
  const warehouseId = appConfig.databricksHttpPath.split("/").pop();

  const res = await fetch(`https://${appConfig.databricksHost}/api/2.0/sql/statements`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      warehouse_id: warehouseId,
      statement: `SELECT COUNT(*) FROM ${sourceTable}`,
      wait_timeout: "50s",
      disposition: "INLINE",
    }),
    cache: "no-store",
  });
  if (!res.ok) throw new Error(`Databricks SQL failed (${res.status})`);
  const payload = await res.json();
  if (payload.status?.state !== "SUCCEEDED") {
    throw new Error(`Databricks status: ${payload.status?.state || "UNKNOWN"}`);
  }
  return Number(payload.result?.data_array?.[0]?.[0] || 0);
}

export async function maxSourceAuditCreatedAt(sourceTable: string): Promise<string | null> {
  const token = await getToken();
  const warehouseId = appConfig.databricksHttpPath.split("/").pop();

  const res = await fetch(`https://${appConfig.databricksHost}/api/2.0/sql/statements`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      warehouse_id: warehouseId,
      statement: `SELECT MAX(audit_created_at) FROM ${sourceTable}`,
      wait_timeout: "50s",
      disposition: "INLINE",
    }),
    cache: "no-store",
  });
  if (!res.ok) return null;
  const payload = await res.json();
  if (payload.status?.state !== "SUCCEEDED") return null;
  return payload.result?.data_array?.[0]?.[0] || null;
}

