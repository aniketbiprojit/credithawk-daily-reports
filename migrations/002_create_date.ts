import { Migration } from "@/bigquery/migration";
import { TABLE_IDS } from "@/bigquery";
import { BigQueryUtils } from "@/bigquery/utils";

const migration: Migration = {
  id: "002_initial",
  name: "Secondary migration",
  description: "Create the initial tables for ADX and Anura reporting",
  up: async () => {
    const bq = new BigQueryUtils(process.env.DATASET_ID!);
    await bq.addColumn(
      TABLE_IDS.ADX_REVENUE_BY_SOURCE_TABLE_ID,
      "display_date",
      "TIMESTAMP",
      ""
    );
    await bq.addColumn(
      TABLE_IDS.ADX_SUMMARY_TABLE_ID,
      "display_date",
      "TIMESTAMP",
      ""
    );
    await bq.addColumn(
      TABLE_IDS.ANURA_SOURCE_METRICS_TABLE_ID,
      "display_date",
      "TIMESTAMP",
      ""
    );
    await bq.addColumn(
      TABLE_IDS.ANURA_SUMMARY_TABLE_ID,
      "display_date",
      "TIMESTAMP",
      ""
    );
  },
  down: async () => {
    // pass
  },
};

export default migration;
