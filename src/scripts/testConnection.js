const { getConfig } = require("../lib/config");
const { executeDatabricksSql } = require("../lib/databricks");

async function main() {
  try {
    const config = getConfig();
    const rows = await executeDatabricksSql(config, "SELECT 1 AS ping");
    console.log(`Connection OK. Ping: ${rows?.[0]?.[0]}`);
  } catch (error) {
    console.error(`Connection failed: ${error.message}`);
    process.exit(1);
  }
}

main();
