import { TABLE_IDS } from "@/bigquery";
import { Migration } from "@/bigquery/migration";
import { BigQueryUtils } from "@/bigquery/utils";

const migration: Migration = {
  id: "004_ga4_total_ad_revenue",
  name: "GA4 total ad revenue",
  description: "Add total ad revenue to GA4 tables",
  up: async () => {
    console.log("Running migration for GA4 total ad revenue");

    const datasetId = process.env.DATASET_ID;
    if (!datasetId) throw new Error("DATASET_ID env var is required");
    const bq = new BigQueryUtils(datasetId);
    try {
      await bq.addColumn(
        TABLE_IDS.GA4_REPORT_SUMMARY_TABLE_ID,
        "total_ad_revenue",
        "FLOAT64",
        ""
      );
    } catch (error) {
      console.error("Error adding column to report summary table:", error);
    }

    try {
      await bq.addColumn(
        TABLE_IDS.GA4_SOURCE_METRICS_TABLE_ID,
        "ad_revenue",
        "FLOAT64",
        ""
      );
    } catch (error) {
      console.error("Error adding column to source metrics table:", error);
    }

    try {
      await bq.addColumn(
        TABLE_IDS.GA4_MEDIUM_METRICS_TABLE_ID,
        "ad_revenue",
        "FLOAT64",
        ""
      );
    } catch (error) {
      console.error("Error adding column to medium metrics table:", error);
    }

    try {
      await bq.addColumn(
        TABLE_IDS.GA4_SOURCE_AND_MEDIUM_METRICS_TABLE_ID,
        "ad_revenue",
        "FLOAT64",
        ""
      );
    } catch (error) {
      console.error("Error adding column to source metrics table:", error);
    }

    console.log("Migration completed");
  },
  down: async () => {
    // pass
  },
};

export default migration;
