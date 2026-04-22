const express = require("express");
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

app.get("/api/data", async (req, res) => {
  try {
    const sourceCountRows = await executeDatabricksSql(
      config,
      `SELECT COUNT(*) FROM ${config.dbxTable}`
    );
    const sourceCount = parseInt(sourceCountRows[0][0], 10);
    const destCount = await supabaseCount(config.supabaseTable);

    const wmRes = await supabaseGet(config, "etl_watermarks", {
      select: "last_timestamp",
      table_name: `eq.${config.supabaseTable}`
    });
    const wmData = await wmRes.json();

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
      source_table: config.dbxTable,
      dest_table: config.supabaseTable,
      source_count: sourceCount,
      dest_count: destCount,
      pending: Math.max(0, sourceCount - destCount),
      watermark: wmData[0]?.last_timestamp || null,
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
    const result = await lambdaClient.send(
      new InvokeCommand({ FunctionName: config.lambdaName, InvocationType: "RequestResponse" })
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
        <h2>Databricks → Supabase Sync (Node)</h2>
        <button onclick="trigger()">Run Sync</button>
        <pre id="out">Loading...</pre>
        <script>
          async function load() {
            const data = await fetch('/api/data').then(r => r.json());
            document.getElementById('out').textContent = JSON.stringify(data, null, 2);
          }
          async function trigger() {
            await fetch('/api/trigger', { method: 'POST' });
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
