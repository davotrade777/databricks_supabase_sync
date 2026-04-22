import fs from "fs";
import path from "path";

export type PipelineConfig = {
  pipeline_name: string;
  source_table: string;
  target_table: string;
  write_mode: "insert_only" | "upsert";
  conflict_key: string;
  batch_size: number;
  schedule: string;
};

type PipelinesMap = Record<string, PipelineConfig>;

export function readPipelines(): PipelineConfig[] {
  const filePath = path.resolve(process.cwd(), "pipelines.json");
  const parsed = JSON.parse(fs.readFileSync(filePath, "utf8")) as PipelinesMap;
  return Object.values(parsed);
}

