const express = require("express");
const fs = require("fs");
const path = require("path");
const { LambdaClient, InvokeCommand, GetFunctionConfigurationCommand } = require("@aws-sdk/client-lambda");
const { CloudWatchLogsClient, DescribeLogStreamsCommand } = require("@aws-sdk/client-cloudwatch-logs");
const { getConfig } = require("../lib/config");
const { executeDatabricksSql } = require("../lib/databricks");
const { supabaseGet } = require("../lib/supabase");

const app = express();
app.use(express.json());

const config = getConfig();
const lambdaClient = new LambdaClient({ region: "us-east-1" });
const logsClient = new CloudWatchLogsClient({ region: "us-east-1" });

async function supabaseCount(table) {
  const res = await supabaseGet(
    config,
    table,
    { select: "codigo_transportista" },
    { Prefer: "count=exact", Range: "0-0" }
  );
  return parseInt((res.headers.get("content-range") || "*/0").split("/")[1], 10);
}

function loadPipelines() {
  const filePath = path.resolve(process.cwd(), "lambda_function/pipelines.json");
  const parsed = JSON.parse(fs.readFileSync(filePath, "utf8"));
  return Object.values(parsed);
}

async function getLastRun(pipelineName) {
  const res = await supabaseGet(config, "etl_runs", {
    select: "pipeline_name,status,inserted_count,updated_count,skipped_count,error_message,started_at,finished_at,duration_ms,s3_log_uri",
    pipeline_name: `eq.${pipelineName}`,
    order: "created_at.desc",
    limit: "1"
  });
  const data = await res.json();
  return data[0] || null;
}

app.get("/api/data", async (req, res) => {
  try {
    const pipelineName = req.query.pipeline || "transportistas";
    const pipelines = loadPipelines();
    const pipeline = pipelines.find((p) => p.pipeline_name === pipelineName);
    if (!pipeline) {
      return res.status(404).json({ error: `Pipeline '${pipelineName}' not found` });
    }

    const sourceCountRows = await executeDatabricksSql(
      config,
      `SELECT COUNT(*) FROM ${pipeline.source_table}`
    );
    const sourceCount = parseInt(sourceCountRows[0][0], 10);
    const destCount = await supabaseCount(pipeline.target_table);

    const wmRes = await supabaseGet(config, "etl_watermarks", {
      select: "last_timestamp",
      table_name: `eq.${pipelineName}`
    });
    const wmData = await wmRes.json();
    const lastRun = await getLastRun(pipelineName);

    const lambdaCfg = await lambdaClient.send(
      new GetFunctionConfigurationCommand({ FunctionName: config.lambdaName })
    );
    const streams = await logsClient.send(
      new DescribeLogStreamsCommand({
        logGroupName: `/aws/lambda/${config.lambdaName}`,
        orderBy: "LastEventTime",
        descending: true,
        limit: 5
      })
    );

    res.json({
      timestamp: new Date().toISOString(),
      pipeline_name: pipelineName,
      source_table: pipeline.source_table,
      dest_table: pipeline.target_table,
      write_mode: pipeline.write_mode,
      conflict_key: pipeline.conflict_key,
      source_count: sourceCount,
      dest_count: destCount,
      pending: Math.max(0, sourceCount - destCount),
      watermark: wmData[0]?.last_timestamp || null,
      last_run: lastRun,
      pipelines: pipelines.map((p) => ({
        pipeline_name: p.pipeline_name,
        source_table: p.source_table,
        target_table: p.target_table,
        write_mode: p.write_mode,
        schedule: p.schedule
      })),
      lambda_info: {
        runtime: lambdaCfg.Runtime,
        memory: lambdaCfg.MemorySize,
        timeout: lambdaCfg.Timeout,
        state: lambdaCfg.State
      },
      executions:
        streams.logStreams?.map((s) => ({
          stream: s.logStreamName,
          last_event: s.lastEventTimestamp
        })) || []
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.post("/api/trigger", async (req, res) => {
  try {
    const pipelineName = req.body?.pipeline_name || "transportistas";
    const result = await lambdaClient.send(
      new InvokeCommand({
        FunctionName: config.lambdaName,
        InvocationType: "RequestResponse",
        Payload: Buffer.from(JSON.stringify({ pipeline_name: pipelineName }))
      })
    );
    const payload = JSON.parse(Buffer.from(result.Payload || []).toString("utf8"));
    res.json({ status: "ok", lambda_response: payload });
  } catch (error) {
    res.status(500).json({ status: "error", message: error.message });
  }
});

app.get("/", (req, res) => {
  res.send(`
    <!doctype html>
    <html>
      <head><meta charset="utf-8"><title>Sync Dashboard (Node)</title></head>
      <body style="font-family: Arial; background:#111; color:#eee; padding:24px;">
        <h2>Databricks → Supabase Sync (Node, Multi-Pipeline)</h2>
        <label>Pipeline:</label>
        <select id="pipeline" onchange="load()"></select>
        <button onclick="trigger()">Run Sync</button>
        <pre id="out">Loading...</pre>
        <script>
          let pipelinesLoaded = false;
          async function load() {
            const current = document.getElementById('pipeline').value || 'transportistas';
            const data = await fetch('/api/data?pipeline=' + encodeURIComponent(current)).then(r => r.json());
            if (!pipelinesLoaded && data.pipelines) {
              const sel = document.getElementById('pipeline');
              sel.innerHTML = data.pipelines.map(p => '<option value=\"'+p.pipeline_name+'\">'+p.pipeline_name+'</option>').join('');
              sel.value = data.pipeline_name;
              pipelinesLoaded = true;
            }
            document.getElementById('out').textContent = JSON.stringify(data, null, 2);
          }
          async function trigger() {
            const pipeline_name = document.getElementById('pipeline').value || 'transportistas';
            await fetch('/api/trigger', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({pipeline_name}) });
            await load();
          }
          load();
          setInterval(load, 30000);
        </script>
      </body>
    </html>
  `);
});

app.listen(5050, "0.0.0.0", () => {
  console.log("Dashboard: http://localhost:5050");
});
