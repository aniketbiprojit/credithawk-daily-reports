import { BigQuery } from "@google-cloud/bigquery";

const bigquery = new BigQuery();

const DATASET_ID = process.env.DATASET_ID || "";

if (DATASET_ID === "") {
  throw new Error("DATASET_ID is not set");
}

const ADX_SUMMARY_TABLE_ID = "adx_report_summaries";
const ADX_REVENUE_BY_SOURCE_TABLE_ID = "adx_revenue_by_source";

const ANURA_SUMMARY_TABLE_ID = "anura_report_summaries";
const ANURA_SOURCE_METRICS_TABLE_ID = "anura_source_metrics";

const GA4_REPORT_SUMMARY_TABLE_ID = "ga4_report_summaries";
const GA4_SOURCE_METRICS_TABLE_ID = "ga4_source_metrics";
const GA4_MEDIUM_METRICS_TABLE_ID = "ga4_medium_metrics";
const GA4_SOURCE_AND_MEDIUM_METRICS_TABLE_ID = "ga4_source_and_medium_metrics";

export const TABLE_IDS = {
  ADX_SUMMARY_TABLE_ID,
  ADX_REVENUE_BY_SOURCE_TABLE_ID,
  ANURA_SUMMARY_TABLE_ID,
  ANURA_SOURCE_METRICS_TABLE_ID,
  GA4_REPORT_SUMMARY_TABLE_ID,
  GA4_SOURCE_METRICS_TABLE_ID,
  GA4_MEDIUM_METRICS_TABLE_ID,
  GA4_SOURCE_AND_MEDIUM_METRICS_TABLE_ID,
};

const ADX_SUMMARY_SCHEMA = [
  { name: "report_date", type: "DATE", mode: "REQUIRED" },
  { name: "report_id", type: "STRING", mode: "REQUIRED" },
  { name: "total_revenue", type: "FLOAT64", mode: "NULLABLE" },
  { name: "total_ad_x_revenue", type: "FLOAT64", mode: "NULLABLE" },
  { name: "total_ad_server_revenue", type: "FLOAT64", mode: "NULLABLE" },
  { name: "total_clicks", type: "INT64", mode: "NULLABLE" },
  { name: "total_impressions", type: "INT64", mode: "NULLABLE" },
  { name: "average_ecpm", type: "FLOAT64", mode: "NULLABLE" },
  { name: "average_ctr", type: "FLOAT64", mode: "NULLABLE" },
  { name: "total_ad_requests", type: "INT64", mode: "NULLABLE" },
  { name: "status", type: "STRING", mode: "REQUIRED" },
  { name: "created_at", type: "TIMESTAMP", mode: "REQUIRED" },
  { name: "updated_at", type: "TIMESTAMP", mode: "REQUIRED" },
  { name: "display_date", type: "TIMESTAMP", mode: "NULLABLE" },
];

const ANURA_SUMMARY_SCHEMA = [
  { name: "start_date", type: "INT64", mode: "REQUIRED" },
  { name: "end_date", type: "INT64", mode: "REQUIRED" },
  { name: "report_hours", type: "INT64", mode: "REQUIRED" },
  { name: "request_report_id", type: "STRING", mode: "REQUIRED" },
  { name: "response_report_id", type: "STRING", mode: "REQUIRED" },
  { name: "request_report_name", type: "STRING", mode: "REQUIRED" },
  { name: "response_report_name", type: "STRING", mode: "REQUIRED" },
  { name: "total_requests", type: "INT64", mode: "NULLABLE" },
  { name: "total_responses", type: "INT64", mode: "NULLABLE" },
  { name: "drop_rate", type: "FLOAT64", mode: "NULLABLE" },
  { name: "good", type: "INT64", mode: "NULLABLE" },
  { name: "good_rate", type: "FLOAT64", mode: "NULLABLE" },
  { name: "bad", type: "INT64", mode: "NULLABLE" },
  { name: "bad_rate", type: "FLOAT64", mode: "NULLABLE" },
  { name: "warn", type: "INT64", mode: "NULLABLE" },
  { name: "warn_rate", type: "FLOAT64", mode: "NULLABLE" },
  { name: "status", type: "STRING", mode: "REQUIRED" },
  { name: "created_at", type: "TIMESTAMP", mode: "REQUIRED" },
  { name: "updated_at", type: "TIMESTAMP", mode: "REQUIRED" },
  { name: "display_date", type: "TIMESTAMP", mode: "NULLABLE" },
];

const ADX_REVENUE_BY_SOURCE_SCHEMA = [
  { name: "report_date", type: "DATE", mode: "REQUIRED" },
  { name: "report_id", type: "STRING", mode: "REQUIRED" },
  { name: "source", type: "STRING", mode: "REQUIRED" },
  { name: "revenue", type: "FLOAT64", mode: "NULLABLE" },
  { name: "ad_server_revenue", type: "FLOAT64", mode: "NULLABLE" },
  { name: "ad_x_revenue", type: "FLOAT64", mode: "NULLABLE" },
  { name: "created_at", type: "TIMESTAMP", mode: "REQUIRED" },
  { name: "display_date", type: "TIMESTAMP", mode: "NULLABLE" },
];

const ANURA_SOURCE_METRICS_SCHEMA = [
  { name: "start_date", type: "INT64", mode: "REQUIRED" },
  { name: "end_date", type: "INT64", mode: "REQUIRED" },
  { name: "report_hours", type: "INT64", mode: "REQUIRED" },
  { name: "request_report_id", type: "STRING", mode: "REQUIRED" },
  { name: "response_report_id", type: "STRING", mode: "REQUIRED" },
  { name: "source", type: "STRING", mode: "REQUIRED" },
  { name: "total_requests", type: "INT64", mode: "NULLABLE" },
  { name: "total_responses", type: "INT64", mode: "NULLABLE" },
  { name: "drop_rate", type: "FLOAT64", mode: "NULLABLE" },
  { name: "good", type: "INT64", mode: "NULLABLE" },
  { name: "good_rate", type: "FLOAT64", mode: "NULLABLE" },
  { name: "bad", type: "INT64", mode: "NULLABLE" },
  { name: "bad_rate", type: "FLOAT64", mode: "NULLABLE" },
  { name: "warn", type: "INT64", mode: "NULLABLE" },
  { name: "warn_rate", type: "FLOAT64", mode: "NULLABLE" },
  { name: "created_at", type: "TIMESTAMP", mode: "REQUIRED" },
  { name: "display_date", type: "TIMESTAMP", mode: "NULLABLE" },
];

const GA4_REPORT_SUMMARY_SCHEMA = [
  { name: "report_date", type: "DATE", mode: "REQUIRED" },
  { name: "total_sessions", type: "INT64", mode: "NULLABLE" },
  { name: "total_revenue", type: "FLOAT64", mode: "NULLABLE" },
  { name: "total_ad_revenue", type: "FLOAT64", mode: "NULLABLE" },
  { name: "total_events", type: "INT64", mode: "NULLABLE" },
  { name: "total_page_views", type: "INT64", mode: "NULLABLE" },
  { name: "total_engaged_sessions", type: "INT64", mode: "NULLABLE" },
  { name: "total_users", type: "INT64", mode: "NULLABLE" },
  { name: "status", type: "STRING", mode: "REQUIRED" },
  { name: "created_at", type: "TIMESTAMP", mode: "REQUIRED" },
  { name: "updated_at", type: "TIMESTAMP", mode: "REQUIRED" },
  { name: "display_date", type: "TIMESTAMP", mode: "NULLABLE" },
];

const GA4_BY_SOURCE_SCHEMA = [
  { name: "report_date", type: "DATE", mode: "REQUIRED" },
  { name: "source", type: "STRING", mode: "REQUIRED" },
  { name: "sessions", type: "INT64", mode: "NULLABLE" },
  { name: "revenue", type: "FLOAT64", mode: "NULLABLE" },
  { name: "ad_revenue", type: "FLOAT64", mode: "NULLABLE" },
  { name: "created_at", type: "TIMESTAMP", mode: "REQUIRED" },
  { name: "display_date", type: "TIMESTAMP", mode: "NULLABLE" },
];

const GA4_BY_MEDIUM_SCHEMA = [
  { name: "report_date", type: "DATE", mode: "REQUIRED" },
  { name: "medium", type: "STRING", mode: "REQUIRED" },
  { name: "sessions", type: "INT64", mode: "NULLABLE" },
  { name: "revenue", type: "FLOAT64", mode: "NULLABLE" },
  { name: "ad_revenue", type: "FLOAT64", mode: "NULLABLE" },
  { name: "created_at", type: "TIMESTAMP", mode: "REQUIRED" },
  { name: "display_date", type: "TIMESTAMP", mode: "NULLABLE" },
];

const GA4_BY_SOURCE_AND_MEDIUM_SCHEMA = [
  { name: "report_date", type: "DATE", mode: "REQUIRED" },
  { name: "source", type: "STRING", mode: "REQUIRED" },
  { name: "medium", type: "STRING", mode: "REQUIRED" },
  { name: "sessions", type: "INT64", mode: "NULLABLE" },
  { name: "revenue", type: "FLOAT64", mode: "NULLABLE" },
  { name: "ad_revenue", type: "FLOAT64", mode: "NULLABLE" },
  { name: "created_at", type: "TIMESTAMP", mode: "REQUIRED" },
  { name: "display_date", type: "TIMESTAMP", mode: "NULLABLE" },
];

export const TABLE_SCHEMAS = {
  ADX_SUMMARY_SCHEMA,
  ADX_REVENUE_BY_SOURCE_SCHEMA,
  ANURA_SUMMARY_SCHEMA,
  ANURA_SOURCE_METRICS_SCHEMA,
  GA4_REPORT_SUMMARY_SCHEMA,
  GA4_BY_SOURCE_SCHEMA,
  GA4_BY_MEDIUM_SCHEMA,
};

export async function initializeBigQuery(): Promise<void> {
  try {
    const dataset = bigquery.dataset(DATASET_ID);
    const [datasetExists] = await dataset.exists();

    if (!datasetExists) {
      await dataset.create();
      console.log(`Dataset ${DATASET_ID} created successfully`);
    }

    await createTableIfNotExists(ADX_SUMMARY_TABLE_ID, ADX_SUMMARY_SCHEMA);
    await createTableIfNotExists(
      ADX_REVENUE_BY_SOURCE_TABLE_ID,
      ADX_REVENUE_BY_SOURCE_SCHEMA
    );
    await createTableIfNotExists(ANURA_SUMMARY_TABLE_ID, ANURA_SUMMARY_SCHEMA);
    await createTableIfNotExists(
      ANURA_SOURCE_METRICS_TABLE_ID,
      ANURA_SOURCE_METRICS_SCHEMA
    );
    await createTableIfNotExists(
      GA4_REPORT_SUMMARY_TABLE_ID,
      GA4_REPORT_SUMMARY_SCHEMA
    );
    await createTableIfNotExists(
      GA4_SOURCE_METRICS_TABLE_ID,
      GA4_BY_SOURCE_SCHEMA
    );
    await createTableIfNotExists(
      GA4_MEDIUM_METRICS_TABLE_ID,
      GA4_BY_MEDIUM_SCHEMA
    );
    await createTableIfNotExists(
      GA4_SOURCE_AND_MEDIUM_METRICS_TABLE_ID,
      GA4_BY_SOURCE_AND_MEDIUM_SCHEMA
    );

    console.log("BigQuery tables initialized successfully");
  } catch (error) {
    console.error("Error initializing BigQuery:", error);
    throw error;
  }
}

async function createTableIfNotExists(
  tableId: string,
  schema: any[]
): Promise<void> {
  const table = bigquery.dataset(DATASET_ID).table(tableId);
  const [tableExists] = await table.exists();

  if (!tableExists) {
    await table.create({
      schema: {
        fields: schema,
      },
    });
    console.log(`Table ${tableId} created successfully`);
  }
}

export interface AdxReportRow {
  date: string;
  countryName: string;
  adUnitName: string;
  browserName: string;
  sessionSource: string;
  campaign: string;
  adXRevenue: number;
  adServerRevenue: number;
  clicks: number;
  impressions: number;
  averageEcpm: number;
  ctr: number;
  viewableImpressions: number;
  adRequests: number;
  reportId: string;
}

export interface AdxReportSummary {
  reportDate: string;
  reportId: string;
  totalRevenue: number;
  totalAdXRevenue: number;
  totalAdServerRevenue: number;
  totalClicks: number;
  totalImpressions: number;
  averageEcpm: number;
  averageCtr: number;
  totalAdRequests: number;
  status: string;
  displayDate: Date;
}

export interface AdxRevenueBySource {
  reportDate: string;
  reportId: string;
  source: string;
  revenue: number;
  adServerRevenue: number;
  adXRevenue: number;
  displayDate: Date;
}

export async function insertAdxReportSummary(
  summary: AdxReportSummary
): Promise<void> {
  const table = bigquery.dataset(DATASET_ID).table(ADX_SUMMARY_TABLE_ID);

  const bigQueryRow = {
    report_date: summary.reportDate,
    report_id: summary.reportId,
    total_revenue: summary.totalRevenue,
    total_ad_x_revenue: summary.totalAdXRevenue,
    total_ad_server_revenue: summary.totalAdServerRevenue,
    total_clicks: summary.totalClicks,
    total_impressions: summary.totalImpressions,
    average_ecpm: summary.averageEcpm,
    average_ctr: summary.averageCtr,
    total_ad_requests: summary.totalAdRequests,
    status: summary.status,
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
    display_date: summary.displayDate,
  };

  try {
    await table.insert([bigQueryRow]);
    console.log("ADX report summary inserted into BigQuery");
  } catch (error) {
    console.error("Error inserting ADX report summary:", error);

    if (error?.response || error?.errors) {
      console.error(error.response);
      console.error(error.errors);
    }
    throw error;
  }
}

export async function insertAdxRevenueBySource(
  revenueBySource: AdxRevenueBySource[]
): Promise<void> {
  if (revenueBySource.length === 0) return;

  const table = bigquery
    .dataset(DATASET_ID)
    .table(ADX_REVENUE_BY_SOURCE_TABLE_ID);

  const bigQueryRows = revenueBySource.map((item) => ({
    report_date: item.reportDate,
    report_id: item.reportId,
    source: item.source,
    revenue: item.revenue,
    ad_server_revenue: item.adServerRevenue,
    ad_x_revenue: item.adXRevenue,
    created_at: new Date().toISOString(),
    display_date: item.displayDate,
  }));

  try {
    await table.insert(bigQueryRows);
    console.log(
      `Inserted ${revenueBySource.length} ADX revenue by source rows into BigQuery`
    );
  } catch (error) {
    console.error("Error inserting ADX revenue by source:", error);

    if (error?.response || error?.errors) {
      console.error(error.response);
      console.error(error.errors);
    }
    throw error;
  }
}

// Anura Report functions
export interface AnuraReportRow {
  source: string;
  campaign: string;
  totalRequests: number;
  totalResponses: number;
  dropRate: number;
  good: number;
  goodRate: number;
  bad: number;
  badRate: number;
  warn: number;
  warnRate: number;
  startDate: number;
  endDate: number;
  reportHours: number;
}

export interface AnuraReportSummary {
  startDate: number;
  endDate: number;
  reportHours: number;
  requestReportId: string;
  responseReportId: string;
  requestReportName: string;
  responseReportName: string;
  totalRequests: number;
  totalResponses: number;
  dropRate: number;
  good: number;
  goodRate: number;
  bad: number;
  badRate: number;
  warn: number;
  warnRate: number;
  status: string;
  displayDate: Date;
}

export interface AnuraSourceMetrics {
  startDate: number;
  endDate: number;
  reportHours: number;
  requestReportId: string;
  responseReportId: string;
  source: string;
  totalRequests: number;
  totalResponses: number;
  dropRate: number;
  good: number;
  goodRate: number;
  bad: number;
  badRate: number;
  warn: number;
  warnRate: number;
  displayDate: Date;
}

export async function insertAnuraReportSummary(
  summary: AnuraReportSummary
): Promise<void> {
  const table = bigquery.dataset(DATASET_ID).table(ANURA_SUMMARY_TABLE_ID);

  const bigQueryRow = {
    start_date: summary.startDate,
    end_date: summary.endDate,
    report_hours: summary.reportHours,
    request_report_id: summary.requestReportId,
    response_report_id: summary.responseReportId,
    request_report_name: summary.requestReportName,
    response_report_name: summary.responseReportName,
    total_requests: summary.totalRequests,
    total_responses: summary.totalResponses,
    drop_rate: summary.dropRate,
    good: summary.good,
    good_rate: summary.goodRate,
    bad: summary.bad,
    bad_rate: summary.badRate,
    warn: summary.warn,
    warn_rate: summary.warnRate,
    status: summary.status,
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
    display_date: summary.displayDate,
  };

  try {
    await table.insert([bigQueryRow]);
    console.log("Anura report summary inserted into BigQuery");
  } catch (error) {
    console.error("Error inserting Anura report summary:", error);

    if (error?.response || error?.errors) {
      console.error(error.response);
      console.error(error.errors);
    }
    throw error;
  }
}

export async function insertAnuraSourceMetrics(
  sourceMetrics: AnuraSourceMetrics[]
): Promise<void> {
  if (sourceMetrics.length === 0) return;

  const table = bigquery
    .dataset(DATASET_ID)
    .table(ANURA_SOURCE_METRICS_TABLE_ID);

  const bigQueryRows = sourceMetrics.map((metric) => ({
    start_date: metric.startDate,
    end_date: metric.endDate,
    report_hours: metric.reportHours,
    request_report_id: metric.requestReportId,
    response_report_id: metric.responseReportId,
    source: metric.source,
    total_requests: metric.totalRequests,
    total_responses: metric.totalResponses,
    drop_rate: metric.dropRate,
    good: metric.good,
    good_rate: metric.goodRate,
    bad: metric.bad,
    bad_rate: metric.badRate,
    warn: metric.warn,
    warn_rate: metric.warnRate,
    created_at: new Date().toISOString(),
    display_date: metric.displayDate,
  }));

  try {
    await table.insert(bigQueryRows);
    console.log(
      `Inserted ${sourceMetrics.length} Anura source metrics rows into BigQuery`
    );
  } catch (error) {
    console.error("Error inserting Anura source metrics:", error);
    throw error;
  }
}

export interface Ga4ReportSummaryBQ {
  reportDate: string;
  totalSessions: number;
  totalRevenue: number;
  totalAdRevenue: number;
  totalEvents: number;
  totalPageViews: number;
  totalEngagedSessions: number;
  totalUsers: number;
  status: string;
  displayDate?: Date;
}

export async function insertGa4ReportSummary(
  summary: Ga4ReportSummaryBQ
): Promise<void> {
  const table = bigquery.dataset(DATASET_ID).table(GA4_REPORT_SUMMARY_TABLE_ID);
  const bigQueryRow = {
    report_date: summary.reportDate,
    total_sessions: summary.totalSessions,
    total_revenue: summary.totalRevenue,
    total_ad_revenue: summary.totalAdRevenue,
    total_events: summary.totalEvents,
    total_page_views: summary.totalPageViews,
    total_engaged_sessions: summary.totalEngagedSessions,
    total_users: summary.totalUsers,
    status: summary.status,
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
    display_date: summary.displayDate || null,
  };
  try {
    await table.insert([bigQueryRow]);
    console.log("GA4 report summary inserted into BigQuery");
  } catch (error) {
    console.error("Error inserting GA4 report summary:", error);
    if (error?.response || error?.errors) {
      console.error(error.response);
      console.error(error.errors);
    }
    throw error;
  }
}

export interface Ga4BySourceRow {
  reportDate: string;
  source: string;
  sessions: number;
  revenue: number;
  adRevenue: number;
  displayDate?: Date;
}

export interface Ga4ByMediumRow {
  reportDate: string;
  medium: string;
  sessions: number;
  revenue: number;
  adRevenue: number;
  displayDate?: Date;
}

export async function insertGa4BySource(rows: Ga4BySourceRow[]): Promise<void> {
  if (!rows.length) return;
  const table = bigquery.dataset(DATASET_ID).table(GA4_SOURCE_METRICS_TABLE_ID);
  const now = new Date().toISOString();
  const bigQueryRows = rows.map((row) => ({
    report_date: row.reportDate,
    source: row.source,
    sessions: row.sessions,
    revenue: row.revenue,
    ad_revenue: row.adRevenue,
    created_at: now,
    display_date: row.displayDate || null,
  }));
  try {
    await table.insert(bigQueryRows);
    console.log(`Inserted ${rows.length} GA4 by source rows into BigQuery`);
  } catch (error) {
    console.error("Error inserting GA4 by source rows:", error);
    throw error;
  }
}

export interface Ga4BySourceAndMediumRow {
  reportDate: string;
  source: string;
  medium: string;
  sessions: number;
  revenue: number;
  adRevenue: number;
  displayDate?: Date;
}

export async function insertGa4BySourceAndMedium(
  rows: Ga4BySourceAndMediumRow[]
): Promise<void> {
  if (!rows.length) return;
  const table = bigquery
    .dataset(DATASET_ID)
    .table(GA4_SOURCE_AND_MEDIUM_METRICS_TABLE_ID);
  const now = new Date().toISOString();
  const bigQueryRows = rows.map((row) => ({
    report_date: row.reportDate,
    source: row.source,
    medium: row.medium,
    sessions: row.sessions,
    revenue: row.revenue,
    ad_revenue: row.adRevenue,
    created_at: now,
    display_date: row.displayDate || null,
  }));

  try {
    await table.insert(bigQueryRows);
    console.log(
      `Inserted ${rows.length} GA4 by source and medium rows into BigQuery`
    );
  } catch (error) {
    console.error("Error inserting GA4 by source and medium rows:", error);
    throw error;
  }
}

export async function insertGa4ByMedium(rows: Ga4ByMediumRow[]): Promise<void> {
  if (!rows.length) return;
  const table = bigquery.dataset(DATASET_ID).table(GA4_MEDIUM_METRICS_TABLE_ID);
  const now = new Date().toISOString();
  const bigQueryRows = rows.map((row) => ({
    report_date: row.reportDate,
    medium: row.medium,
    sessions: row.sessions,
    revenue: row.revenue,
    ad_revenue: row.adRevenue,
    created_at: now,
    display_date: row.displayDate || null,
  }));
  try {
    await table.insert(bigQueryRows);
    console.log(`Inserted ${rows.length} GA4 by medium rows into BigQuery`);
  } catch (error) {
    console.error("Error inserting GA4 by medium rows:", error);
    throw error;
  }
}

// Query functions
export async function queryAdxReports(
  startDate?: string,
  limit?: number
): Promise<any[]> {
  let query = `SELECT * FROM \`${bigquery.projectId}.${DATASET_ID}.${ADX_SUMMARY_TABLE_ID}\``;
  const conditions = [];

  if (startDate) {
    conditions.push(`report_date = '${startDate}'`);
  }

  if (conditions.length > 0) {
    query += ` WHERE ${conditions.join(" AND ")}`;
  }

  query += ` ORDER BY report_date DESC`;

  if (limit) {
    query += ` LIMIT ${limit}`;
  }

  try {
    const [rows] = await bigquery.query({ query });
    return rows;
  } catch (error) {
    console.error("Error querying ADX reports:", error);
    throw error;
  }
}

export async function queryAnuraReports(
  startDate?: number,
  limit?: number
): Promise<any[]> {
  let query = `SELECT * FROM \`${bigquery.projectId}.${DATASET_ID}.${ANURA_SUMMARY_TABLE_ID}\``;
  const conditions = [];

  if (startDate) {
    conditions.push(`start_date = ${startDate}`);
  }

  if (conditions.length > 0) {
    query += ` WHERE ${conditions.join(" AND ")}`;
  }

  query += ` ORDER BY start_date DESC`;

  if (limit) {
    query += ` LIMIT ${limit}`;
  }

  console.log(query);

  try {
    const [rows] = await bigquery.query({ query });
    return rows;
  } catch (error) {
    console.error("Error querying Anura reports:", error);
    throw error;
  }
}

export async function queryGa4Reports(
  reportDate?: string,
  limit?: number
): Promise<any[]> {
  let query = `SELECT * FROM \`${bigquery.projectId}.${DATASET_ID}.${GA4_REPORT_SUMMARY_TABLE_ID}\``;
  const conditions = [];
  if (reportDate) {
    conditions.push(`report_date = '${reportDate}'`);
  }
  if (conditions.length > 0) {
    query += ` WHERE ${conditions.join(" AND ")}`;
  }
  query += ` ORDER BY report_date DESC`;
  if (limit) {
    query += ` LIMIT ${limit}`;
  }
  try {
    const [rows] = await bigquery.query({ query });
    return rows;
  } catch (error) {
    console.error("Error querying GA4 reports:", error);
    throw error;
  }
}
