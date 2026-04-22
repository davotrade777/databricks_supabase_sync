import { appConfig } from "./config";

function headers(extra?: Record<string, string>) {
  return {
    apikey: appConfig.supabaseKey,
    Authorization: `Bearer ${appConfig.supabaseKey}`,
    "Content-Type": "application/json",
    ...extra,
  };
}

export async function supabaseCount(table: string): Promise<number> {
  const res = await fetch(
    `${appConfig.supabaseUrl}/rest/v1/${table}?select=${encodeURIComponent("*")}`,
    {
      headers: headers({ Prefer: "count=exact", Range: "0-0" }),
      cache: "no-store",
    }
  );
  const range = res.headers.get("content-range") || "*/0";
  return Number(range.split("/")[1] || 0);
}

export async function supabaseLastWatermark(pipelineName: string) {
  const res = await fetch(
    `${appConfig.supabaseUrl}/rest/v1/etl_watermarks?select=last_timestamp&table_name=eq.${encodeURIComponent(pipelineName)}&limit=1`,
    { headers: headers(), cache: "no-store" }
  );
  if (!res.ok) return null;
  const data = (await res.json()) as Array<{ last_timestamp: string }>;
  return data[0]?.last_timestamp || null;
}

export async function supabaseLastRun(pipelineName: string) {
  const res = await fetch(
    `${appConfig.supabaseUrl}/rest/v1/etl_runs?select=pipeline_name,status,inserted_count,updated_count,skipped_count,error_message,started_at,finished_at,duration_ms,s3_log_uri&pipeline_name=eq.${encodeURIComponent(pipelineName)}&order=created_at.desc&limit=1`,
    { headers: headers(), cache: "no-store" }
  );
  if (!res.ok) return null;
  const data = await res.json();
  return data[0] || null;
}

export async function supabaseDatamartWatermark(targetTable: string): Promise<string | null> {
  const res = await fetch(
    `${appConfig.supabaseUrl}/rest/v1/${targetTable}?select=_ingested_at&order=_ingested_at.desc&limit=1`,
    { headers: headers(), cache: "no-store" }
  );
  if (!res.ok) return null;
  const data = (await res.json()) as Array<{ _ingested_at?: string }>;
  return data[0]?._ingested_at || null;
}

