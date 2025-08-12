import { Migration } from "@/bigquery/migration";
import { initializeBigQuery } from "@/bigquery";

const migration: Migration = {
  id: "001_initial",
  name: "Initial migration",
  description: "Create the initial tables for ADX and Anura reporting",
  up: async () => {
    console.log("Initial migration");
    await initializeBigQuery();
  },
  down: async () => {
    // pass
  },
};

export default migration;
