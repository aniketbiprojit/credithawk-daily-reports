import { GoogleAuth } from "google-auth-library";
import { tmpdir } from "os";
import * as path from "path";
import * as fs from "fs";
import { uploadToGCP } from "./utils";
import {
  initializeBigQuery,
  insertAdxReportSummary,
  insertAdxRevenueBySource,
  queryAdxReports,
  AdxReportSummary,
  AdxRevenueBySource,
} from "./bigquery";
import * as dotenv from "dotenv";

dotenv.config();

// Configuration
const NETWORK_CODE = 21805304712;

// ADX dimensions and metrics for revenue reporting
// Note: This implementation now supports pagination to handle reports with more than 1000 rows.
// The fetchAllReportRows function will automatically fetch all available data using the
// Google Ad Manager API's nextPageToken mechanism.
const adxDimensions = [
  "DATE", // 0
  "COUNTRY_NAME", // 1
  "AD_UNIT_NAME_ALL_LEVEL", // 2
  "BROWSER_NAME", // 3
  "CUSTOM_DIMENSION_0_VALUE", // 4
  "CUSTOM_DIMENSION_1_VALUE", // 5
  "AD_UNIT_ID", // 6
  "AD_UNIT_NAME", // 7
];

const adxDimensionsMap = {
  DATE: 0,
  COUNTRY_NAME: 1,
  AD_UNIT_NAME_ALL_LEVEL: 2,
  BROWSER_NAME: 3,
  SOURCE: 4,
  CAMPAIGN: 5,
  AD_UNIT_ID: 6,
  AD_UNIT_NAME: 7,
} as const;

const adxMetrics = [
  "AD_EXCHANGE_REVENUE", // 0
  "AD_EXCHANGE_CLICKS", // 1
  "AD_EXCHANGE_IMPRESSIONS", // 2
  "AD_EXCHANGE_AVERAGE_ECPM", // 3
  "AD_EXCHANGE_CTR", // 4
  "AD_EXCHANGE_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS", // 5
  "AD_REQUESTS", // 6
  "AD_SERVER_REVENUE", // 7
];

const adxMetricsMap = {
  AD_EXCHANGE_REVENUE: 0,
  AD_EXCHANGE_CLICKS: 1,
  AD_EXCHANGE_IMPRESSIONS: 2,
  AD_EXCHANGE_AVERAGE_ECPM: 3,
  AD_EXCHANGE_CTR: 4,
  AD_EXCHANGE_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS: 5,
  AD_REQUESTS: 6,
  AD_SERVER_REVENUE: 7,
} as const;

// Type for parsed row data before database insertion
interface ParsedAdxRow {
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
}

// Type for summary data before database insertion
interface ParsedAdxSummary {
  totalRevenue: number;
  totalAdXRevenue: number;
  totalAdServerRevenue: number;
  totalClicks: number;
  totalImpressions: number;
  averageEcpm: number;
  averageCtr: number;
  totalAdRequests: number;
  revenueByAdUnit: Record<
    string,
    {
      adXRevenue: number;
      adServerRevenue: number;
    }
  >;
  revenueBySource: Record<
    string,
    {
      adXRevenue: number;
      adServerRevenue: number;
    }
  >;
  rows: ParsedAdxRow[];
}

// Helper function to get yesterday's date
const getReportDate = (): Date => {
  const today = new Date(process.env.REPORT_DATE || Date.now());
  today.setDate(today.getDate());
  return today;
};

const getOneDayBefore = (date: Date): Date => {
  const oneDayBefore = new Date(date);
  oneDayBefore.setDate(oneDayBefore.getDate() - 1);
  return oneDayBefore;
};

// Helper function to format date as YYYY-MM-DD
const formatDateToString = (date: Date): string => {
  return date.toISOString().split("T")[0];
};

// Checkpoint interface for BigQuery
interface AdxReportCheckpoint {
  reportDate: Date;
  reportId?: string;
  reportResultId?: string;
  status?: string;
  createdAt?: Date;
  updatedAt?: Date;
}

// Load checkpoint from JSON file
const loadCheckpoint = async (
  reportDate: Date
): Promise<AdxReportCheckpoint | null> => {
  try {
    const fs = require("fs");
    const path = require("path");
    const checkpointFile = path.join(process.cwd(), "adx-checkpoint.json");

    if (fs.existsSync(checkpointFile)) {
      const data = fs.readFileSync(checkpointFile, "utf8");
      const checkpoints = JSON.parse(data);
      const dateStr = formatDateToString(reportDate);
      const checkpoint = checkpoints[dateStr];

      if (checkpoint) {
        console.log("Loaded checkpoint from JSON file");
        return {
          ...checkpoint,
          reportDate: new Date(checkpoint.reportDate),
        };
      }
    }
    return null;
  } catch (error) {
    console.error("Error loading checkpoint:", error);
    return null;
  }
};

// Save checkpoint to JSON file
const saveCheckpoint = async (
  checkpoint: Partial<AdxReportCheckpoint> & { reportDate: Date }
): Promise<void> => {
  try {
    const fs = require("fs");
    const path = require("path");
    const checkpointFile = path.join(process.cwd(), "adx-checkpoint.json");

    let checkpoints: Record<string, any> = {};
    if (fs.existsSync(checkpointFile)) {
      const data = fs.readFileSync(checkpointFile, "utf8");
      checkpoints = JSON.parse(data);
    }

    const dateStr = formatDateToString(checkpoint.reportDate);

    const existingCheckpoint = checkpoints[dateStr] || {};

    checkpoints[dateStr] = {
      ...existingCheckpoint,
      ...checkpoint,
      reportDate: checkpoint.reportDate.toISOString(),
      updatedAt: new Date().toISOString(),
    };

    fs.writeFileSync(checkpointFile, JSON.stringify(checkpoints, null, 2));
    console.log("Checkpoint saved to JSON file");
  } catch (error) {
    console.error("Error saving checkpoint:", error);
  }
};

// Clear checkpoint from JSON file
// const clearCheckpoint = async (reportDate: Date): Promise<void> => {
//   try {
//     const fs = require("fs");
//     const path = require("path");
//     const checkpointFile = path.join(process.cwd(), "adx-checkpoint.json");

//     if (fs.existsSync(checkpointFile)) {
//       const data = fs.readFileSync(checkpointFile, "utf8");
//       const checkpoints = JSON.parse(data);
//       const dateStr = formatDateToString(reportDate);

//       if (checkpoints[dateStr]) {
//         delete checkpoints[dateStr];
//         fs.writeFileSync(checkpointFile, JSON.stringify(checkpoints, null, 2));
//         console.log("Checkpoint cleared from JSON file");
//       }
//     }
//   } catch (error) {
//     console.error("Error clearing checkpoint:", error);
//   }
// };

async function getAccessToken(): Promise<string> {
  const auth = new GoogleAuth({
    scopes: ["https://www.googleapis.com/auth/dfp"],
  });

  const client = await auth.getClient();
  const token = await client.getAccessToken();

  if (!token.token) {
    throw new Error("Failed to get access token");
  }

  return token.token;
}

async function createReport(token: string, reportDate: string): Promise<any> {
  const dateRange = {
    fixed: {
      startDate: {
        year: Number(reportDate.split("-")[0]),
        month: Number(reportDate.split("-")[1]),
        day: Number(reportDate.split("-")[2]) - 1,
      },
      endDate: {
        year: Number(reportDate.split("-")[0]),
        month: Number(reportDate.split("-")[1]),
        day: Number(reportDate.split("-")[2]) - 1,
      },
    },
  };
  console.log(dateRange);
  const reportDefinition = {
    name: `networks/${NETWORK_CODE}/reports/adx-yesterday-revenue-${Date.now()}`,
    visibility: "HIDDEN",
    reportDefinition: {
      customDimensionKeyIds: [14045082, 15320277, 16495849],
      dimensions: adxDimensions,
      metrics: adxMetrics,
      dateRange: process.env.REPORT_DATE
        ? dateRange
        : {
            relative: "YESTERDAY",
          },
      filters: [],
      reportType: "HISTORICAL",
      timePeriodColumn: "TIME_PERIOD_COLUMN_DATE",
    },
    displayName: `${reportDate} Adx Report`,
    createTime: new Date().toISOString(),
    updateTime: new Date().toISOString(),
  };

  console.log(`Creating report for ${reportDate}...`);

  const response = await fetch(
    `https://admanager.googleapis.com/v1/networks/${NETWORK_CODE}/reports`,
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(reportDefinition),
    }
  );

  if (!response.ok) {
    const errorData = await response.json();
    console.error("Report creation error:", JSON.stringify(errorData, null, 2));
    throw new Error(`Failed to create report: ${response.statusText}`);
  }

  return response.json();
}

async function runReport(token: string, reportId: string): Promise<any> {
  console.log("Running report...");

  const response = await fetch(
    `https://admanager.googleapis.com/v1/networks/${NETWORK_CODE}/reports/${reportId}:run`,
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
      },
    }
  );

  if (!response.ok) {
    const errorData = await response.json();
    throw new Error(`Failed to run report: ${JSON.stringify(errorData)}`);
  }

  return response.json();
}

async function pollOperation(
  token: string,
  operationName: string
): Promise<any> {
  let delay = 5000;
  const maxDelay = 60000;
  const maxAttempts = 60;
  let attempts = 0;

  console.log("Polling for report completion...");

  while (attempts < maxAttempts) {
    const response = await fetch(
      `https://admanager.googleapis.com/v1/${operationName}`,
      {
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
      }
    );

    const operation = await response.json();

    if (operation.done) {
      if (operation.error) {
        throw new Error(`Operation failed: ${JSON.stringify(operation.error)}`);
      }
      console.log("Report completed successfully!");
      return operation.response;
    }

    console.log(
      `Report still processing... (attempt ${attempts + 1}/${maxAttempts})`
    );

    await new Promise((resolve) => setTimeout(resolve, delay));
    delay = Math.min(delay * 1.5, maxDelay);
    attempts++;
  }

  throw new Error("Operation timed out after maximum attempts");
}

async function fetchReportRows(
  token: string,
  reportId: string,
  reportResultId: string,
  pageSize: number = 1000,
  nextPageToken?: string
): Promise<any> {
  console.log("Fetching report rows...");

  let url = `https://admanager.googleapis.com/v1/networks/${NETWORK_CODE}/reports/${reportId}/results/${reportResultId}:fetchRows?pageSize=${pageSize}`;

  if (nextPageToken) {
    url += `&pageToken=${nextPageToken}`;
  }

  const response = await fetch(url, {
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
  });

  const result = await response.json();

  if (result.error) {
    throw new Error(`Failed to fetch report rows: ${result.error.message}`);
  }

  return result;
}

async function fetchAllReportRows(
  token: string,
  reportId: string,
  reportResultId: string,
  pageSize: number = 1000
): Promise<any[]> {
  console.log("Fetching all report rows with pagination...");

  let allRows: any[] = [];
  let nextPageToken: string | undefined;
  let pageCount = 0;
  const maxPages = 100; // Safety limit to prevent infinite loops

  do {
    pageCount++;
    console.log(`Fetching page ${pageCount}...`);

    if (pageCount > maxPages) {
      console.warn(
        `Reached maximum page limit (${maxPages}). Stopping pagination.`
      );
      break;
    }

    const result = await fetchReportRows(
      token,
      reportId,
      reportResultId,
      pageSize,
      nextPageToken
    );

    if (result.rows) {
      allRows = allRows.concat(result.rows);
      console.log(
        `Page ${pageCount}: Fetched ${result.rows.length} rows (Total: ${allRows.length})`
      );
    } else {
      console.warn(`Page ${pageCount}: No rows returned`);
    }

    nextPageToken = result.nextPageToken;

    // Add a small delay between requests to be respectful to the API
    if (nextPageToken) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  } while (nextPageToken);

  console.log(
    `Completed fetching all rows. Total rows: ${allRows.length} across ${pageCount} pages`
  );
  return allRows;
}

function parseAdxRow(row: any): ParsedAdxRow {
  const dimensionValues = row.dimensionValues || [];
  const metricValues = row.metricValueGroups?.[0]?.primaryValues || [];

  return {
    date: dimensionValues[adxDimensionsMap.DATE]?.stringValue || "",
    countryName:
      dimensionValues[adxDimensionsMap.COUNTRY_NAME]?.stringValue || "",
    adUnitName:
      dimensionValues[adxDimensionsMap.AD_UNIT_NAME_ALL_LEVEL]?.stringValue ||
      "",
    browserName:
      dimensionValues[adxDimensionsMap.BROWSER_NAME]?.stringValue || "",
    sessionSource: dimensionValues[adxDimensionsMap.SOURCE]?.stringValue || "",
    campaign: dimensionValues[adxDimensionsMap.CAMPAIGN]?.stringValue || "",
    adXRevenue:
      metricValues[adxMetricsMap.AD_EXCHANGE_REVENUE]?.doubleValue || 0,
    adServerRevenue:
      metricValues[adxMetricsMap.AD_SERVER_REVENUE]?.doubleValue || 0,
    clicks:
      typeof metricValues[1]?.intValue === "string"
        ? parseInt(metricValues[1].intValue, 10)
        : metricValues[1]?.intValue || 0,
    impressions:
      typeof metricValues[2]?.intValue === "string"
        ? parseInt(metricValues[2].intValue, 10)
        : metricValues[2]?.intValue || 0,
    averageEcpm: metricValues[3]?.doubleValue || 0,
    ctr: metricValues[4]?.doubleValue || 0,
    viewableImpressions:
      typeof metricValues[5]?.intValue === "string"
        ? parseInt(metricValues[5].intValue, 10)
        : metricValues[5]?.intValue || 0,
    adRequests:
      typeof metricValues[6]?.intValue === "string"
        ? parseInt(metricValues[6].intValue, 10)
        : metricValues[6]?.intValue || 0,
  };
}

function generateReportSummary(rows: ParsedAdxRow[]): ParsedAdxSummary {
  const summary: ParsedAdxSummary = {
    totalRevenue: 0,
    totalAdXRevenue: 0,
    totalAdServerRevenue: 0,
    totalClicks: 0,
    totalImpressions: 0,
    averageEcpm: 0,
    averageCtr: 0,
    totalAdRequests: 0,
    revenueByAdUnit: {},
    revenueBySource: {},
    rows: rows,
  };

  let totalEcpm = 0;
  let totalCtr = 0;
  let rowCount = 0;

  rows.forEach((row) => {
    summary.totalClicks += row.clicks;
    summary.totalImpressions += row.impressions;
    summary.totalAdRequests += row.adRequests;
    summary.totalAdXRevenue += row.adXRevenue;
    summary.totalAdServerRevenue += row.adServerRevenue;
    summary.totalRevenue += row.adServerRevenue + row.adXRevenue;
    // Aggregate by ad unit
    summary.revenueByAdUnit[row.adUnitName] = {
      adXRevenue:
        (summary.revenueByAdUnit[row.adUnitName]?.adXRevenue || 0) +
        row.adXRevenue,
      adServerRevenue:
        (summary.revenueByAdUnit[row.adUnitName]?.adServerRevenue || 0) +
        row.adServerRevenue,
    };

    // Aggregate by source
    summary.revenueBySource[row.sessionSource] = {
      adXRevenue:
        (summary.revenueBySource[row.sessionSource]?.adXRevenue || 0) +
        row.adXRevenue,
      adServerRevenue:
        (summary.revenueBySource[row.sessionSource]?.adServerRevenue || 0) +
        row.adServerRevenue,
    };

    if (row.averageEcpm > 0) {
      totalEcpm += row.averageEcpm;
      rowCount++;
    }

    if (row.ctr > 0) {
      totalCtr += row.ctr;
    }
  });

  summary.averageEcpm = rowCount > 0 ? totalEcpm / rowCount : 0;
  summary.averageCtr = rows.length > 0 ? totalCtr / rows.length : 0;

  const totalRevenue = summary.totalRevenue;
  console.log(
    `Total Revenue: ${totalRevenue.toLocaleString()} (AdX: ${summary.totalAdXRevenue.toLocaleString()}, AdServer: ${summary.totalAdServerRevenue.toLocaleString()})`
  );

  return summary;
}

// Save report data to BigQuery
async function saveReportToBigQuery(
  reportDate: Date,
  reportId: string,
  summary: ParsedAdxSummary
): Promise<void> {
  try {
    // Create summary for BigQuery
    const bigQuerySummary: AdxReportSummary = {
      reportDate: formatDateToString(reportDate),
      reportId: reportId,
      totalRevenue: summary.totalRevenue,
      totalAdXRevenue: summary.totalAdXRevenue,
      totalAdServerRevenue: summary.totalAdServerRevenue,
      totalClicks: summary.totalClicks,
      totalImpressions: summary.totalImpressions,
      averageEcpm: summary.averageEcpm,
      averageCtr: summary.averageCtr,
      totalAdRequests: summary.totalAdRequests,
      status: "COMPLETED",
      displayDate: getOneDayBefore(reportDate),
    };

    // Insert report summary
    console.log("bigQuerySummary", bigQuerySummary);
    await insertAdxReportSummary(bigQuerySummary);

    // Insert revenue by source data
    const revenueBySourceData: AdxRevenueBySource[] = Object.entries(
      summary.revenueBySource
    ).map(([source, revenue]) => ({
      reportDate: formatDateToString(reportDate),
      reportId: reportId,
      source: source || "(not set)",
      revenue: revenue.adXRevenue + revenue.adServerRevenue,
      adServerRevenue: revenue.adServerRevenue,
      adXRevenue: revenue.adXRevenue,
      displayDate: getOneDayBefore(reportDate),
    }));

    console.log("revenueBySourceData", revenueBySourceData);
    await insertAdxRevenueBySource(revenueBySourceData);

    console.log("Report saved to BigQuery successfully");
  } catch (error) {
    console.error("Error saving report to BigQuery:", error);
    throw error;
  }
}

interface DumpDataOptions {
  reportDate: Date;
  reportId: string;
  data: any;
  filename?: string;
}

export async function dumpRawDataToFile(
  options: DumpDataOptions
): Promise<string> {
  const { reportDate, reportId, data, filename: customFilename } = options;

  try {
    const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
    const dateStr = reportDate.toISOString().split("T")[0];
    const filename = customFilename || `raw-data-${dateStr}-${timestamp}.json`;
    const filepath = path.join(tmpdir(), filename);

    const dumpData = {
      metadata: {
        reportDate: dateStr,
        reportId,
      },
      data: data,
    };

    fs.writeFileSync(filepath, JSON.stringify(dumpData, null, 2));

    console.log(`Raw data dumped to: ${filepath}`);

    // Check if GCP bucket is configured
    const gcpBucket = process.env.GCP_BUCKET;
    if (gcpBucket) {
      console.log("Uploading file to GCP");
      await uploadToGCP(filepath, filename, reportDate, gcpBucket);
    }

    return filepath;
  } catch (error) {
    console.error("Error dumping raw data to file:", error);
    throw error;
  }
}

function printReportSummary(summary: ParsedAdxSummary): void {
  console.log("\n" + "=".repeat(60));
  console.log("ADX YESTERDAY REVENUE REPORT");
  console.log("=".repeat(60));

  console.log(`\nOVERALL SUMMARY:`);
  console.log(`Total Revenue: $${summary.totalRevenue.toFixed(4)}`);
  console.log(`Total AdX Revenue: $${summary.totalAdXRevenue.toFixed(4)}`);
  console.log(
    `Total AdServer Revenue: $${summary.totalAdServerRevenue.toFixed(4)}`
  );
  console.log(`Total Clicks: ${summary.totalClicks.toLocaleString()}`);
  console.log(
    `Total Impressions: ${summary.totalImpressions.toLocaleString()}`
  );
  console.log(`Average eCPM: $${summary.averageEcpm.toFixed(4)}`);
  console.log(`Average CTR: ${(summary.averageCtr * 100).toFixed(4)}%`);
  console.log(`Total Ad Requests: ${summary.totalAdRequests.toLocaleString()}`);

  console.log(`\nREVENUE BY AD UNIT:`);
  Object.entries(summary.revenueByAdUnit)
    .sort(
      ([, a], [, b]) =>
        b.adXRevenue + b.adServerRevenue - (a.adXRevenue + a.adServerRevenue)
    )
    .forEach(([adUnit, revenue]) => {
      console.log(
        `  ${adUnit}: $${(revenue.adXRevenue + revenue.adServerRevenue).toFixed(
          4
        )}`
      );
    });

  console.log(`\nREVENUE BY SOURCE:`);
  Object.entries(summary.revenueBySource)
    .sort(
      ([, a], [, b]) =>
        b.adXRevenue + b.adServerRevenue - (a.adXRevenue + a.adServerRevenue)
    )
    .slice(0, 10) // Show top 10 sources
    .forEach(([source, revenue]) => {
      console.log(
        `  ${source || "(not set)"}: $${(
          revenue.adXRevenue + revenue.adServerRevenue
        ).toFixed(4)}`
      );
    });

  console.log(`\nDETAILED ROWS (${summary.rows.length} total):`);
  summary.rows.slice(0, 5).forEach((row, index) => {
    console.log(
      `  ${index + 1}. ${row.date} | ${row.countryName} | ${
        row.adUnitName
      } | $${(row.adXRevenue + row.adServerRevenue).toFixed(4)}`
    );
  });

  if (summary.rows.length > 5) {
    console.log(`  ... and ${summary.rows.length - 5} more rows`);
  }

  console.log("\n" + "=".repeat(60));
}

export async function generateAdxYesterdayReport(): Promise<AdxReportSummary> {
  const reportDate = getReportDate();
  const reportDateStr = formatDateToString(reportDate);

  // Initialize BigQuery
  await initializeBigQuery();

  // Check if force rerun is enabled
  const forceRerun = process.env.FORCE_RERUN === "true";

  if (forceRerun) {
    console.log(
      `FORCE_RERUN enabled - will generate new report for ${reportDateStr}`
    );
  }

  // Load checkpoint (skip if force rerun)
  let checkpoint = await loadCheckpoint(reportDate);

  if (!forceRerun) {
    console.log(
      `Report for ${reportDateStr} already completed according to checkpoint.`
    );

    // Check if data actually exists in BigQuery
    try {
      const existingData = await queryAdxReports(reportDateStr, 1);
      if (existingData.length > 0) {
        console.log(existingData);

        console.log("Data found in BigQuery, skipping report generation");
        return {
          reportDate: reportDateStr,
          reportId: existingData?.[0]?.["report_id"] || "",
          totalRevenue: 0, // Would need to query summary table for actual values
          totalAdXRevenue: 0,
          totalAdServerRevenue: 0,
          totalClicks: 0,
          totalImpressions: 0,
          averageEcpm: 0,
          averageCtr: 0,
          totalAdRequests: 0,
          status: "COMPLETED",
          displayDate: getOneDayBefore(reportDate),
        };
      }
    } catch (error) {
      console.log(
        "Error checking BigQuery for existing data, will regenerate report:",
        error
      );
      throw error;
    }
  }

  // Initialize checkpoint if not exists or if force rerun
  if (!checkpoint || forceRerun) {
    checkpoint = {
      reportDate,
      status: "PROCESSING",
    };
  }

  try {
    console.log("Starting ADX Yesterday Revenue Report Generation...");

    const token = await getAccessToken();
    console.log("Authentication successful");

    // Create report if not already created
    let reportId = checkpoint.reportId;
    if (!reportId) {
      const report = await createReport(token, reportDateStr);
      reportId = report.name.split("/").pop();
      await saveCheckpoint({ reportDate, reportId });
      console.log(`Report created with ID: ${reportId}`);
    }

    if (!reportId) {
      throw new Error("Report ID not found. Something went wrong.");
    }

    // Run report if not already run
    let reportResultId = checkpoint.reportResultId;
    if (!reportResultId) {
      const operation = await runReport(token, reportId);
      console.log(`Report execution started`);

      // Poll for completion
      const reportResult = await pollOperation(token, operation.name);
      reportResultId = reportResult.reportResult.split("results/")[1];
      await saveCheckpoint({ reportDate, reportId, reportResultId });
      console.log(`Report execution completed`);
    }

    if (!reportResultId) {
      throw new Error("Report Result ID not found. Something went wrong.");
    }

    // Fetch all rows with pagination (handles reports with >1000 rows)
    const rowsData = await fetchAllReportRows(
      token,
      reportId,
      reportResultId,
      1000
    );
    console.log(`Fetched ${rowsData.length} rows total`);

    // Parse and process data
    const parsedRows: ParsedAdxRow[] = [];
    rowsData.forEach((row: any) => {
      const parsedRow = parseAdxRow(row);
      parsedRows.push(parsedRow);
    });

    // Dump raw data to temporary file
    await dumpRawDataToFile({
      reportDate,
      reportId,
      data: { rows: rowsData },
    });

    // Generate summary
    const summary = generateReportSummary(parsedRows);

    printReportSummary(summary);

    await saveReportToBigQuery(reportDate, reportId, summary);

    await saveCheckpoint({
      reportDate,
      reportId,
      reportResultId,
      status: "COMPLETED",
    });

    // await clearCheckpoint(reportDate);

    console.log("ADX Yesterday Revenue Report completed successfully!");

    return {
      reportDate: formatDateToString(reportDate),
      reportId: reportId,
      totalRevenue: summary.totalRevenue,
      totalAdXRevenue: summary.totalAdXRevenue,
      totalAdServerRevenue: summary.totalAdServerRevenue,
      totalClicks: summary.totalClicks,
      totalImpressions: summary.totalImpressions,
      averageEcpm: summary.averageEcpm,
      averageCtr: summary.averageCtr,
      totalAdRequests: summary.totalAdRequests,
      status: "COMPLETED",
      displayDate: getOneDayBefore(reportDate),
    };
  } catch (error) {
    console.error("Error generating ADX report:", error);
    await saveCheckpoint({
      reportDate,
      reportId: checkpoint.reportId,
      reportResultId: checkpoint.reportResultId,
      status: "FAILED",
    });
    throw error;
  }
}

// Run the report if this file is executed directly
if (require.main === module) {
  generateAdxYesterdayReport()
    .then(() => {
      console.log("Report generation completed!");
      process.exit(0);
    })
    .catch((error) => {
      console.error("Report generation failed:", error);
      process.exit(1);
    });
}
