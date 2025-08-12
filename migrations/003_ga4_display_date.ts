import { TABLE_IDS } from "@/bigquery";
import { Migration } from "@/bigquery/migration";
import { BigQueryUtils } from "@/bigquery/utils";

const migration: Migration = {
  id: "003_ga4_display_date",
  name: "GA4 display date",
  description: "Add display date to GA4 tables",
  up: async () => {
    const datasetId = process.env.DATASET_ID;
    if (!datasetId) throw new Error("DATASET_ID env var is required");
    const bq = new BigQueryUtils(datasetId);

    const tables = [
      TABLE_IDS.GA4_REPORT_SUMMARY_TABLE_ID,
      TABLE_IDS.GA4_SOURCE_METRICS_TABLE_ID,
      TABLE_IDS.GA4_MEDIUM_METRICS_TABLE_ID,
    ];

    for (const table of tables) {
      try {
        console.log(`Running migration for table: ${table}`);
        await bq.addColumn(table, "display_date", "TIMESTAMP", "");
      } catch (error) {
        console.error(`Error adding column to table ${table}:`, error);
      }
    }
  },
  down: async () => {
    // pass
  },
};

export default migration;
