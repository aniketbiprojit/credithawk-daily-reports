import { BigQuery } from "@google-cloud/bigquery";

export class BigQueryUtils {
  private bigquery: BigQuery;
  private datasetId: string;

  constructor(datasetId: string) {
    this.bigquery = new BigQuery({
      projectId: process.env.BIGQUERY_PROJECT_ID,
    });
    this.datasetId = datasetId;
  }

  async createTable(tableId: string, schema: any[]): Promise<void> {
    const table = this.bigquery.dataset(this.datasetId).table(tableId);
    const [tableExists] = await table.exists();

    if (!tableExists) {
      await table.create({
        schema: {
          fields: schema,
        },
      });
      console.log(`Table ${tableId} created successfully`);
    } else {
      console.log(`Table ${tableId} already exists`);
    }
  }

  async dropTable(tableId: string): Promise<void> {
    const table = this.bigquery.dataset(this.datasetId).table(tableId);
    const [tableExists] = await table.exists();

    if (tableExists) {
      await table.delete();
      console.log(`Table ${tableId} dropped successfully`);
    } else {
      console.log(`Table ${tableId} does not exist`);
    }
  }

  async addColumn(
    tableId: string,
    columnName: string,
    columnType: string,
    mode: "" | "REQUIRED"
  ): Promise<void> {
    const query = `
      ALTER TABLE \`${this.bigquery.projectId}.${this.datasetId}.${tableId}\`
      ADD COLUMN ${columnName} ${columnType} ${mode};
    `;
    console.log(query);

    await this.bigquery.query({ query });
    console.log(`Column ${columnName} added to table ${tableId}`);
  }

  async dropColumn(tableId: string, columnName: string): Promise<void> {
    const query = `
      ALTER TABLE \`${this.bigquery.projectId}.${this.datasetId}.${tableId}\`
      DROP COLUMN ${columnName}
    `;

    await this.bigquery.query({ query });
    console.log(`Column ${columnName} dropped from table ${tableId}`);
  }

  async renameColumn(
    tableId: string,
    oldColumnName: string,
    newColumnName: string
  ): Promise<void> {
    const query = `
      ALTER TABLE \`${this.bigquery.projectId}.${this.datasetId}.${tableId}\`
      RENAME COLUMN ${oldColumnName} TO ${newColumnName}
    `;

    await this.bigquery.query({ query });
    console.log(
      `Column ${oldColumnName} renamed to ${newColumnName} in table ${tableId}`
    );
  }

  async changeColumnType(
    tableId: string,
    columnName: string,
    newType: string
  ): Promise<void> {
    const query = `
      ALTER TABLE \`${this.bigquery.projectId}.${this.datasetId}.${tableId}\`
      ALTER COLUMN ${columnName} SET DATA TYPE ${newType}
    `;

    await this.bigquery.query({ query });
    console.log(
      `Column ${columnName} type changed to ${newType} in table ${tableId}`
    );
  }

  async createView(viewId: string, query: string): Promise<void> {
    const view = this.bigquery.dataset(this.datasetId).table(viewId);
    const [viewExists] = await view.exists();

    if (!viewExists) {
      await view.create({
        view: {
          query: query,
          useLegacySql: false,
        },
      });
      console.log(`View ${viewId} created successfully`);
    } else {
      console.log(`View ${viewId} already exists`);
    }
  }

  async dropView(viewId: string): Promise<void> {
    const view = this.bigquery.dataset(this.datasetId).table(viewId);
    const [viewExists] = await view.exists();

    if (viewExists) {
      await view.delete();
      console.log(`View ${viewId} dropped successfully`);
    } else {
      console.log(`View ${viewId} does not exist`);
    }
  }

  async executeQuery(query: string): Promise<any[]> {
    const [rows] = await this.bigquery.query({ query });
    return rows;
  }

  async tableExists(tableId: string): Promise<boolean> {
    const table = this.bigquery.dataset(this.datasetId).table(tableId);
    const [exists] = await table.exists();
    return exists;
  }

  async getTableSchema(tableId: string): Promise<any[]> {
    const table = this.bigquery.dataset(this.datasetId).table(tableId);
    const [metadata] = await table.getMetadata();
    return metadata.schema.fields;
  }

  async copyTable(
    sourceTableId: string,
    destinationTableId: string
  ): Promise<void> {
    const sourceTable = this.bigquery
      .dataset(this.datasetId)
      .table(sourceTableId);
    const destinationTable = this.bigquery
      .dataset(this.datasetId)
      .table(destinationTableId);

    const [job] = await sourceTable.copy(destinationTable);

    console.log(
      `Table ${sourceTableId} copied to ${destinationTableId}`,
      job.id
    );
  }

  async createPartitionedTable(
    tableId: string,
    schema: any[],
    partitionField: string,
    partitionType: string = "DAY"
  ): Promise<void> {
    const table = this.bigquery.dataset(this.datasetId).table(tableId);
    const [tableExists] = await table.exists();

    if (!tableExists) {
      await table.create({
        schema: {
          fields: schema,
        },
        timePartitioning: {
          type: partitionType,
          field: partitionField,
        },
      });
      console.log(`Partitioned table ${tableId} created successfully`);
    } else {
      console.log(`Table ${tableId} already exists`);
    }
  }
}
