import type { SupabaseProfile } from "./supabase";
import { supabaseLastRun } from "./supabase";
import { fetchCheckpointFromS3, s3CheckpointToLastRun } from "./s3-checkpoint";

/**
 * Prefer `etl_runs` in Supabase; if missing or empty, use Lambda S3 checkpoint
 * `s3://{bucket}/{etl-success}/{pipeline}/latest.json`.
 */
export async function resolveLastRun(pipelineName: string, profile: SupabaseProfile) {
  const fromDb = await supabaseLastRun(pipelineName, profile);
  if (fromDb) {
    return { ...fromDb, run_source: "supabase" as const };
  }
  const cp = await fetchCheckpointFromS3(pipelineName);
  if (cp) {
    return s3CheckpointToLastRun(cp);
  }
  return null;
}
