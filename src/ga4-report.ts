import { BetaAnalyticsDataClient } from "@google-analytics/data";
import * as dotenv from "dotenv";
import * as fs from "fs";
import * as path from "path";
import { tmpdir } from "os";
import { uploadToGCP, formatDateToString } from "./utils";
import {
  initializeBigQuery,
  insertGa4ReportSummary,
  insertGa4BySource,
  insertGa4ByMedium,
  queryGa4Reports,
  insertGa4BySourceAndMedium,
} from "./bigquery";

dotenv.config();
const PROPERTY_ID = 473915191;

const ga4Dimensions = [
  "date",
  "sessionSource",
  "sessionMedium",
  "sessionCampaignName",
  "sessionCampaignId",
];

const ga4Metrics = [
  "sessions",
  "totalRevenue",
  "eventCount",
  "screenPageViews",
  "engagedSessions",
  "totalUsers",
  "totalAdRevenue",
];

interface Ga4ReportRow {
  date: string;
  sessionSource: string;
  sessionMedium: string;
  sessionCampaignName: string;
  sessionCampaignId: string;
  sessions: number;
  totalRevenue: number;
  eventCount: number;
  screenPageViews: number;
  engagedSessions: number;
  totalUsers: number;
  totalAdRevenue: number;
}

interface Ga4ReportSummary {
  totalSessions: number;
  totalRevenue: number;
  totalAdRevenue: number;
  totalEvents: number;
  totalPageViews: number;
  totalEngagedSessions: number;
  totalUsers: number;
  sessionsBySource: Record<string, number>;
  sessionsByMedium: Record<string, number>;
  revenueBySource: Record<string, number>;
  revenueByMedium: Record<string, number>;
  adRevenueBySource: Record<string, number>;
  adRevenueByMedium: Record<string, number>;
  //
  adRevenueBySourceAndMedium: Record<string, number>;
  revenueBySourceAndMedium: Record<string, number>;
  sessionsBySourceAndMedium: Record<string, number>;

  rows: Ga4ReportRow[];
}

const separator = "__JOIN__SEPERATOR__";

const joinBySourceAndMedium = (source: string, medium: string) => {
  return `${source}${separator}${medium}`;
};

const splitBySourceAndMedium = (sourceAndMedium: string) => {
  const [source, medium] = sourceAndMedium.split(separator);
  return { source, medium };
};

// Checkpointing for GA4
interface Ga4ReportCheckpoint {
  reportDate: Date;
  status?: string;
  rowCount?: number;
  createdAt?: Date;
  updatedAt?: Date;
}

const loadCheckpoint = async (
  reportDate: Date
): Promise<Ga4ReportCheckpoint | null> => {
  try {
    const checkpointFile = path.join(process.cwd(), "ga4-checkpoint.json");
    if (fs.existsSync(checkpointFile)) {
      const data = fs.readFileSync(checkpointFile, "utf8");
      const checkpoints = JSON.parse(data);
      const dateStr = formatDateToString(reportDate);
      const checkpoint = checkpoints[dateStr];
      if (checkpoint) {
        console.log("Loaded GA4 checkpoint from JSON file");
        return {
          ...checkpoint,
          reportDate: new Date(checkpoint.reportDate),
        };
      }
    }
    return null;
  } catch (error) {
    throw error;
  }
};

const saveCheckpoint = async (
  checkpoint: Partial<Ga4ReportCheckpoint> & { reportDate: Date }
): Promise<void> => {
  try {
    const checkpointFile = path.join(process.cwd(), "ga4-checkpoint.json");
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
    console.log("GA4 checkpoint saved to JSON file");
  } catch (error) {
    throw error;
  }
};

async function createGa4Client(): Promise<BetaAnalyticsDataClient> {
  return new BetaAnalyticsDataClient({});
}

async function runGa4Report(
  client: BetaAnalyticsDataClient,
  startDate: string,
  endDate: string,
  limit: number = 250000,
  offset: number = 0
): Promise<any> {
  console.log(`Running GA4 report for ${startDate} to ${endDate}...`);

  const [response] = await client.runReport({
    property: `properties/${PROPERTY_ID}`,
    dateRanges: [{ startDate, endDate }],
    dimensions: ga4Dimensions.map((dim) => ({ name: dim })),
    metrics: ga4Metrics.map((metric) => ({ name: metric })),
    limit: limit,
    offset: offset,
  });

  console.log("response row count:", response.rowCount);

  if (!response.rows) {
    console.log("No data returned from GA4");
    return { rows: [] };
  }

  return response;
}

function parseGa4Row(row: any): Ga4ReportRow {
  const dimensionValues = row.dimensionValues || [];
  const metricValues = row.metricValues || [];

  return {
    date: dimensionValues[0]?.value || "",
    sessionSource: dimensionValues[1]?.value || "(not set)",
    sessionMedium: dimensionValues[2]?.value || "(not set)",
    sessionCampaignName: dimensionValues[3]?.value || "(not set)",
    sessionCampaignId: dimensionValues[4]?.value || "(not set)",
    sessions: parseInt(metricValues[0]?.value || "0", 10),
    totalRevenue: parseFloat(metricValues[1]?.value || "0"),
    eventCount: parseInt(metricValues[2]?.value || "0", 10),
    screenPageViews: parseInt(metricValues[3]?.value || "0", 10),
    engagedSessions: parseInt(metricValues[4]?.value || "0", 10),
    totalUsers: parseInt(metricValues[5]?.value || "0", 10),
    totalAdRevenue: parseFloat(metricValues[6]?.value || "0"),
  };
}

function generateReportSummary(rows: Ga4ReportRow[]): Ga4ReportSummary {
  const summary: Ga4ReportSummary = {
    totalSessions: 0,
    totalRevenue: 0,
    totalEvents: 0,
    totalPageViews: 0,
    totalAdRevenue: 0,
    totalEngagedSessions: 0,
    totalUsers: 0,
    sessionsBySource: {},
    sessionsByMedium: {},
    revenueBySource: {},
    revenueByMedium: {},
    adRevenueBySource: {},
    adRevenueByMedium: {},
    adRevenueBySourceAndMedium: {},
    revenueBySourceAndMedium: {},
    sessionsBySourceAndMedium: {},
    rows: rows,
  };

  rows.forEach((row) => {
    summary.totalSessions += row.sessions;
    summary.totalRevenue += row.totalRevenue;
    summary.totalAdRevenue += row.totalAdRevenue;
    summary.totalEvents += row.eventCount;
    summary.totalPageViews += row.screenPageViews;
    summary.totalEngagedSessions += row.engagedSessions;
    summary.totalUsers += row.totalUsers;

    summary.sessionsBySource[row.sessionSource] =
      (summary.sessionsBySource[row.sessionSource] || 0) + row.sessions;

    summary.sessionsByMedium[row.sessionMedium] =
      (summary.sessionsByMedium[row.sessionMedium] || 0) + row.sessions;

    summary.revenueBySource[row.sessionSource] =
      (summary.revenueBySource[row.sessionSource] || 0) + row.totalRevenue;

    summary.revenueByMedium[row.sessionMedium] =
      (summary.revenueByMedium[row.sessionMedium] || 0) + row.totalRevenue;

    summary.adRevenueBySource[row.sessionSource] =
      (summary.adRevenueBySource[row.sessionSource] || 0) + row.totalAdRevenue;

    summary.adRevenueByMedium[row.sessionMedium] =
      (summary.adRevenueByMedium[row.sessionMedium] || 0) + row.totalAdRevenue;

    const sourceAndMedium = joinBySourceAndMedium(
      row.sessionSource,
      row.sessionMedium
    );
    summary.adRevenueBySourceAndMedium[sourceAndMedium] =
      (summary.adRevenueBySourceAndMedium[sourceAndMedium] || 0) +
      row.totalAdRevenue;

    summary.sessionsBySourceAndMedium[sourceAndMedium] =
      (summary.sessionsBySourceAndMedium[sourceAndMedium] || 0) + row.sessions;

    summary.revenueBySourceAndMedium[sourceAndMedium] =
      (summary.revenueBySourceAndMedium[sourceAndMedium] || 0) +
      row.totalRevenue;
  });

  return summary;
}

function printReportSummary(summary: Ga4ReportSummary): void {
  console.log("\n" + "=".repeat(60));
  console.log("GA4 UTM & REVENUE REPORT");
  console.log("=".repeat(60));

  console.log(`\nOVERALL SUMMARY:`);
  console.log(`Total Sessions: ${summary.totalSessions.toLocaleString()}`);
  console.log(`Total Revenue: $${summary.totalRevenue.toFixed(4)}`);
  console.log(`Total Events: ${summary.totalEvents.toLocaleString()}`);
  console.log(`Total Page Views: ${summary.totalPageViews.toLocaleString()}`);
  console.log(
    `Total Engaged Sessions: ${summary.totalEngagedSessions.toLocaleString()}`
  );
  console.log(`Total Users: ${summary.totalUsers.toLocaleString()}`);

  console.log(`\nSESSIONS BY UTM SOURCE:`);
  Object.entries(summary.sessionsBySource)
    .sort(([, a], [, b]) => b - a)
    .forEach(([source, sessions]) => {
      console.log(`  ${source}: ${sessions.toLocaleString()}`);
    });

  console.log(`\nSESSIONS BY UTM MEDIUM:`);
  Object.entries(summary.sessionsByMedium)
    .sort(([, a], [, b]) => b - a)
    .forEach(([medium, sessions]) => {
      console.log(`  ${medium}: ${sessions.toLocaleString()}`);
    });

  console.log(`\nREVENUE BY UTM SOURCE:`);
  Object.entries(summary.revenueBySource)
    .sort(([, a], [, b]) => b - a)
    .forEach(([source, revenue]) => {
      console.log(`  ${source}: $${revenue.toFixed(4)}`);
    });

  console.log(`\nREVENUE BY UTM MEDIUM:`);
  Object.entries(summary.revenueByMedium)
    .sort(([, a], [, b]) => b - a)
    .forEach(([medium, revenue]) => {
      console.log(`  ${medium}: $${revenue.toFixed(4)}`);
    });

  console.log(`\nDETAILED ROWS (${summary.rows.length} total):`);
  summary.rows.slice(0, 5).forEach((row, index) => {
    console.log(
      `  ${index + 1}. ${row.date} | ${row.sessionSource} | ${
        row.sessionMedium
      } | ${row.sessions} sessions | $${row.totalRevenue.toFixed(4)} revenue`
    );
  });

  if (summary.rows.length > 5) {
    console.log(`  ... and ${summary.rows.length - 5} more rows`);
  }

  console.log("\n" + "=".repeat(60));
}

// Helper to fetch all rows with pagination and robust error handling
async function fetchAllGa4Rows(
  client: BetaAnalyticsDataClient,
  startDate: string,
  endDate: string,
  limit: number = 250000
): Promise<any[]> {
  let offset = 0;
  let allRows: any[] = [];
  let totalRows = 0;
  let page = 0;
  while (true) {
    page++;
    try {
      const response = await runGa4Report(
        client,
        startDate,
        endDate,
        limit,
        offset
      );
      const rows = response.rows || [];
      allRows = allRows.concat(rows);
      totalRows += rows.length;
      console.log(
        `Fetched page ${page}: ${rows.length} rows (total: ${totalRows})`
      );
      if (rows.length < limit) {
        break;
      }
      offset += limit;
    } catch (error) {
      console.error(`Error fetching GA4 rows at offset ${offset}:`, error);
      throw error;
    }
  }
  return allRows;
}

// Save raw GA4 data to file and optionally upload to GCP
interface DumpGa4DataOptions {
  reportDate: Date;
  data: any;
  filePrefix?: string;
}

async function dumpRawGa4DataToFile(
  options: DumpGa4DataOptions
): Promise<string> {
  const { reportDate, data, filePrefix } = options;
  try {
    const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
    const dateStr = formatDateToString(reportDate);
    const filename =
      (filePrefix ?? "") + `ga4-raw-data-${dateStr}-${timestamp}.json`;
    const filepath = path.join(tmpdir(), filename);
    const dumpData = {
      metadata: {
        reportDate: dateStr,
      },
      data: data,
    };
    fs.writeFileSync(filepath, JSON.stringify(dumpData, null, 2));
    console.log(`GA4 raw data dumped to: ${filepath}`);
    // Check if GCP bucket is configured
    const gcpBucket = process.env.GCP_BUCKET;
    if (gcpBucket) {
      try {
        console.log("Uploading GA4 file to GCP");
        await uploadToGCP(filepath, filename, reportDate, gcpBucket);
      } catch (gcpErr) {
        console.error("Error uploading GA4 file to GCP:", gcpErr);
        throw gcpErr;
      }
    }
    return filepath;
  } catch (error) {
    console.error("Error dumping GA4 raw data to file:", error);
    throw error;
  }
}

const saveReportToBigQuery = async (
  yesterdayStr: string,
  summary: Ga4ReportSummary,
  displayDate: Date
) => {
  try {
    await insertGa4ReportSummary({
      reportDate: yesterdayStr,
      totalSessions: summary.totalSessions,
      totalRevenue: summary.totalRevenue,
      totalAdRevenue: summary.totalAdRevenue,
      totalEvents: summary.totalEvents,
      totalPageViews: summary.totalPageViews,
      totalEngagedSessions: summary.totalEngagedSessions,
      totalUsers: summary.totalUsers,
      status: "COMPLETED",
      displayDate,
    });
    // Save by source
    const bySourceRows = Object.keys(summary.sessionsBySource).map(
      (source) => ({
        reportDate: yesterdayStr,
        source,
        sessions: summary.sessionsBySource[source] || 0,
        revenue: summary.revenueBySource[source] || 0,
        adRevenue: summary.adRevenueBySource[source] || 0,
        displayDate,
      })
    );

    await insertGa4BySource(bySourceRows);
    // Save by medium
    const byMediumRows = Object.keys(summary.sessionsByMedium).map(
      (medium) => ({
        reportDate: yesterdayStr,
        medium,
        sessions: summary.sessionsByMedium[medium] || 0,
        revenue: summary.revenueByMedium[medium] || 0,
        adRevenue: summary.adRevenueByMedium[medium] || 0,
        displayDate,
      })
    );

    await insertGa4ByMedium(byMediumRows);

    const bySourceAndMediumRows = Object.keys(
      summary.adRevenueBySourceAndMedium
    ).map((sourceAndMedium) => {
      const { source, medium } = splitBySourceAndMedium(sourceAndMedium);
      return {
        reportDate: yesterdayStr,
        source,
        medium,
        adRevenue: summary.adRevenueBySourceAndMedium[sourceAndMedium] || 0,
        sessions: summary.sessionsBySourceAndMedium[sourceAndMedium] || 0,
        revenue: summary.revenueBySourceAndMedium[sourceAndMedium] || 0,
        displayDate,
      };
    });
    await insertGa4BySourceAndMedium(bySourceAndMediumRows);
  } catch (bqErr) {
    console.error(
      "Error saving GA4 summary/by-source/by-medium to BigQuery:",
      bqErr
    );
    throw bqErr;
  }
};

export async function generateGa4YesterdayReport(): Promise<Ga4ReportSummary> {
  const yesterday = new Date(process.env.REPORT_DATE || new Date());
  yesterday.setDate(yesterday.getDate() - 1);
  const yesterdayStr = yesterday.toISOString().split("T")[0];

  // Initialize BigQuery
  await initializeBigQuery();

  // Check if force rerun is enabled
  const forceRerun = process.env.FORCE_RERUN === "true";

  // Check checkpoint before running
  let checkpoint = await loadCheckpoint(yesterday);

  if (!forceRerun) {
    // Check if data actually exists in BigQuery
    try {
      const existingData = await queryGa4Reports(yesterdayStr, 1);
      if (existingData.length > 0) {
        console.log(existingData);
        console.log("Data found in BigQuery, skipping report generation");
        return {
          totalSessions: existingData[0]?.totalSessions,
          totalRevenue: existingData[0]?.totalRevenue,
          totalEvents: 0,
          totalPageViews: 0,
          totalEngagedSessions: 0,
          totalUsers: 0,
          sessionsBySource: {},
          sessionsByMedium: {},
          revenueBySource: {},
          revenueByMedium: {},
          adRevenueBySource: {},
          adRevenueByMedium: {},
          rows: [],
          totalAdRevenue: existingData[0]?.totalAdRevenue,
          adRevenueBySourceAndMedium: {},
          revenueBySourceAndMedium: {},
          sessionsBySourceAndMedium: {},
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
      reportDate: yesterday,
      status: "PROCESSING",
    };
  }

  // Save checkpoint as processing
  await saveCheckpoint({ reportDate: yesterday, status: "PROCESSING" });

  try {
    console.log("Starting GA4 Yesterday UTM & Revenue Report Generation...");
    const client = await createGa4Client();
    console.log("GA4 client created successfully");
    console.log("Runnin report for date:", yesterdayStr);
    // Use pagination to fetch all rows
    const allRows = await fetchAllGa4Rows(client, yesterdayStr, yesterdayStr);
    console.log(`Fetched ${allRows.length} total rows (all pages)`);
    // Save raw data to file and optionally GCP
    try {
      await dumpRawGa4DataToFile({
        reportDate: yesterday,
        data: { rows: allRows },
      });
    } catch (err) {
      throw err;
    }
    const parsedRows: Ga4ReportRow[] = [];
    allRows.forEach((row: any) => {
      const parsedRow = parseGa4Row(row);
      parsedRows.push(parsedRow);
    });
    try {
      await dumpRawGa4DataToFile({
        reportDate: yesterday,
        data: { rows: allRows },
        filePrefix: "parsed-",
      });
    } catch (err) {
      throw err;
    }
    const summary = generateReportSummary(parsedRows);
    printReportSummary(summary);
    // Save summary to BigQuery
    await saveReportToBigQuery(yesterdayStr, summary, yesterday);
    await saveCheckpoint({
      reportDate: yesterday,
      status: "COMPLETED",
      rowCount: allRows.length,
    });
    console.log("GA4 Yesterday UTM & Revenue Report completed successfully!");
    return summary;
  } catch (error) {
    console.error("Error generating GA4 report:", error);

    if (error instanceof error?.errors) {
      console.log("PartialFailureError:", error?.errors?.[0]);
      console.log("PartialFailureError:", error?.response?.statusMessage);
    }

    await saveCheckpoint({ reportDate: yesterday, status: "FAILED" });
    throw error;
  }
}

if (require.main === module) {
  generateGa4YesterdayReport()
    .then(() => {
      console.log("Report generation completed!");
      process.exit(0);
    })
    .catch((error) => {
      console.error("Report generation failed:", error);
      process.exit(1);
    });
}
