import { BigQuery } from "@google-cloud/bigquery";
import * as fs from "fs";
import * as path from "path";
import { config } from "dotenv";

config();

const bigquery = new BigQuery();
const MIGRATIONS_TABLE_ID = "migrations";
const DATASET_ID = process.env.DATASET_ID || "";

if (DATASET_ID === "") {
  throw new Error("DATASET_ID is not set");
}

export interface Migration {
  id: string;
  name: string;
  description?: string;
  up: () => Promise<void>;
  down: () => Promise<void>;
}

export interface MigrationRecord {
  id: string;
  name: string;
  applied_at: string;
  checksum: string;
}

export class MigrationRunner {
  private dataset: any;
  private migrationsTable: any;

  constructor() {
    this.dataset = bigquery.dataset(DATASET_ID);
    this.migrationsTable = this.dataset.table(MIGRATIONS_TABLE_ID);
  }

  async initialize(): Promise<void> {
    const [datasetExists] = await this.dataset.exists();
    if (!datasetExists) {
      throw new Error("Dataset doesn't exist");
    }

    // Ensure migrations table exists
    const [tableExists] = await this.migrationsTable.exists();
    if (!tableExists) {
      await this.migrationsTable.create({
        schema: {
          fields: [
            { name: "id", type: "STRING", mode: "REQUIRED" },
            { name: "name", type: "STRING", mode: "REQUIRED" },
            { name: "applied_at", type: "TIMESTAMP", mode: "REQUIRED" },
            { name: "checksum", type: "STRING", mode: "REQUIRED" },
          ],
        },
      });
      console.log(`Migrations table created successfully`);
    }
  }

  async getAppliedMigrations(): Promise<MigrationRecord[]> {
    try {
      const [rows] = await this.migrationsTable.query(`
        SELECT id, name, applied_at, checksum
        FROM \`${DATASET_ID}.${MIGRATIONS_TABLE_ID}\`
        ORDER BY applied_at ASC
      `);
      return rows || [];
    } catch (error) {
      console.error("Error fetching applied migrations:", error);
      throw "Invalid permissions to perform migratory functions";
    }
  }

  async recordMigration(migration: Migration, checksum: string): Promise<void> {
    const record = {
      id: migration.id,
      name: migration.name,
      applied_at: new Date().toISOString(),
      checksum,
    };

    await this.migrationsTable.insert([record]);
  }

  async removeMigration(migrationId: string): Promise<void> {
    const query = `
      DELETE FROM \`${DATASET_ID}.${MIGRATIONS_TABLE_ID}\`
      WHERE id = @migrationId
    `;

    await this.migrationsTable.query({
      query,
      params: { migrationId },
    });
  }

  async migrate(migrations: Migration[]): Promise<void> {
    await this.initialize();

    const appliedMigrations = await this.getAppliedMigrations();
    const appliedIds = new Set(appliedMigrations.map((m) => m.id));

    console.log("Total Migrations:", migrations.length);

    for (const migration of migrations) {
      if (appliedIds.has(migration.id)) {
        console.log(`Migration ${migration.id} already applied, skipping`);
        continue;
      }

      console.log(`Applying migration: ${migration.id} - ${migration.name}`);

      try {
        await migration.up();
        const checksum = this.generateChecksum(migration);
        await this.recordMigration(migration, checksum);
        console.log(`✓ Migration ${migration.id} applied successfully`);
      } catch (error) {
        console.error(`✗ Failed to apply migration ${migration.id}:`, error);
        throw error;
      }
    }
  }

  async rollback(migrations: Migration[], steps: number = 1): Promise<void> {
    await this.initialize();

    const appliedMigrations = await this.getAppliedMigrations();
    const appliedIds = new Set(appliedMigrations.map((m) => m.id));

    const migrationsToRollback = migrations
      .filter((m) => appliedIds.has(m.id))
      .slice(-steps);

    for (const migration of migrationsToRollback.reverse()) {
      console.log(
        `Rolling back migration: ${migration.id} - ${migration.name}`
      );

      try {
        await migration.down();
        await this.removeMigration(migration.id);
        console.log(`✓ Migration ${migration.id} rolled back successfully`);
      } catch (error) {
        console.error(`✗ Failed to rollback migration ${migration.id}:`, error);
        throw error;
      }
    }
  }

  async status(migrations: Migration[]): Promise<void> {
    await this.initialize();

    const appliedMigrations = await this.getAppliedMigrations();
    const appliedIds = new Set(appliedMigrations.map((m) => m.id));

    console.log("\nMigration Status:");
    console.log("==================");

    for (const migration of migrations) {
      const status = appliedIds.has(migration.id) ? "✓ Applied" : "○ Pending";
      console.log(`${status} ${migration.id} - ${migration.name}`);
    }

    console.log(
      `\nTotal: ${migrations.length} migrations, ${appliedMigrations.length} applied`
    );
  }

  private generateChecksum(migration: Migration): string {
    // Simple checksum based on migration content
    const content = `${migration.id}${migration.name}${
      migration.description || ""
    }`;
    return Buffer.from(content).toString("base64").substring(0, 16);
  }
}

export async function loadMigrations(
  migrationsDir: string
): Promise<Migration[]> {
  const migrations: Migration[] = [];

  if (!fs.existsSync(migrationsDir)) {
    return migrations;
  }

  const files = fs
    .readdirSync(migrationsDir)
    .filter((file) => file.endsWith(".ts") || file.endsWith(".js"))
    .sort();

  for (const file of files) {
    try {
      const migrationPath = path.join(migrationsDir, file);
      const migrationModule = await import(migrationPath);

      if (
        migrationModule.default &&
        typeof migrationModule.default === "object"
      ) {
        migrations.push(migrationModule.default);
      }
    } catch (error) {
      console.error(`Error loading migration from ${file}:`, error);
    }
  }

  return migrations;
}

export async function runMigrations(migrationsDir: string): Promise<void> {
  const runner = new MigrationRunner();
  const migrations = await loadMigrations(migrationsDir);

  if (migrations.length === 0) {
    console.log("No migrations found");
    return;
  }

  await runner.migrate(migrations);
}

export async function rollbackMigrations(
  migrationsDir: string,
  steps: number = 1
): Promise<void> {
  const runner = new MigrationRunner();
  const migrations = await loadMigrations(migrationsDir);

  if (migrations.length === 0) {
    console.log("No migrations found");
    return;
  }

  await runner.rollback(migrations, steps);
}

export async function showMigrationStatus(
  migrationsDir: string
): Promise<void> {
  const runner = new MigrationRunner();
  const migrations = await loadMigrations(migrationsDir);

  await runner.status(migrations);
}
