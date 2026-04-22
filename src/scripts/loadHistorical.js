const { getConfig } = require("../lib/config");
const { runSync } = require("../sync/runSync");

async function main() {
  try {
    const config = getConfig();
    const result = await runSync(config);
    console.log(
      JSON.stringify(
        {
          mode: "historical-merge",
          ...result
        },
        null,
        2
      )
    );
  } catch (error) {
    console.error(`Historical load failed: ${error.message}`);
    process.exit(1);
  }
}

main();
