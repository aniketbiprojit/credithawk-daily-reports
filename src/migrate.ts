#!/usr/bin/env tsx

import { runMigrations, showMigrationStatus } from "@/bigquery/migration";
import * as path from "path";

const MIGRATIONS_DIR = path.join(__dirname, "../migrations");

async function main() {
  const command = process.argv[2];
  //   const args = process.argv.slice(3);

  try {
    switch (command) {
      case "up":
      case "migrate":
        console.log("Running migrations...");
        await runMigrations(MIGRATIONS_DIR);
        break;

      case "status":
        await showMigrationStatus(MIGRATIONS_DIR);
        break;

      default:
        console.error("Unknown command:", command);
        process.exit(1);
    }
  } catch (error) {
    console.error("Migration failed:", error);
    process.exit(1);
  }
}

if (require.main === module) {
  main();
}
