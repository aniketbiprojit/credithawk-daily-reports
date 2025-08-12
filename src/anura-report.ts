import * as dotenv from "dotenv";
import * as fs from "fs";
import { tmpdir } from "os";
import * as path from "path";
import extract from "extract-zip";
import Papa from "papaparse";
import { uploadToGCP } from "./utils";
import {
  initializeBigQuery,
  insertAnuraReportSummary,
  insertAnuraSourceMetrics,
  queryAnuraReports,
  AnuraReportSummary,
  AnuraSourceMetrics,
} from "./bigquery";
import { DateTime } from "luxon";

dotenv.config();

const ANURA_BASE_URL = "https://api.anura.io/v1";
const ANURA_API_TOKEN =
  "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiIwMzM4MjU1OTkxODUiLCJpYXQiOjE3NDgwMTAxOTQuNzM2NzUzLCJuYmYiOjE3NDgwMTAxOTQuNzM2NzUzLCJqdGkiOiJLWVpSS3hCSEQ1NVduUFlldE9oUHd4dUU5dVo4Y0hCZCIsImF1ZCI6IlhxU25ZWFpkMjNhakhaTmQlMkZ4N1U4dyUzRCUzRCIsInRrbiI6ImFwaSIsImFjdCI6ODE0LCJ1c3IiOjI3MDMsImlucyI6IlsxMDczMDc0Mzg3XSJ9.K71uV2h_jGtmANX1sQbtMmABYVcLIR9Fd_oxFi1mxF4AqjLBTvHXEHfQziQMJN12dtVS65P2i9I6pc3BcDmKTA";

interface AnuraReportRequest {
  token: string;
  instance: number;
  type: "request" | "response";
  start: number;
  end: number;
  standard: string[];
  name: string;
  send_email?: boolean;
}

interface AnuraReportResponse {
  success: boolean;
  message: string;
  id: string;
}

interface AnuraReportData {
  id: string;
  type: string;
  name: string;
  start: number;
  end: number;
  instance_id: number;
  instance_name: string;
  columns: {
    standard: string[];
    additional: string[] | null;
  };
  filters: any | null;
  send_email: boolean;
  requested: string;
  file_name: string;
  file_size: number;
  rows: number;
  created: string;
  failed: string | null;
  timed_out: string | null;
  cancelled: string | null;
  alerted: string;
  status: string;
  processing_minutes: number;
  expires: string;
}

interface SourceMetrics {
  source?: string | null;
  totalRequests: number;
  totalResponses: number;
  dropRate: number;
  good: number;
  goodRate: number;
  bad: number;
  badRate: number;
  warn: number;
  warnRate: number;
}

interface ReportMetrics {
  totalRequests: number;
  totalResponses: number;
  dropRate: number;
  good: number;
  goodRate: number;
  bad: number;
  badRate: number;
  warn: number;
  warnRate: number;
  otherMetrics: SourceMetrics[];
}

// Configuration for report period
const REPORT_HOURS = parseInt(process.env.REPORT_HOURS ?? "") || 24;
const INSTANCE_ID = 1073074387;

// Helper function to format date as YYYYMMDD number
const formatDateToNumber = (date: Date): number => {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return Number(`${year}${month}${day}`);
};

// Helper to generate a fixed report name
const getReportName = (
  type: "request" | "response",
  start: number,
  end: number
) => {
  const prefix =
    type === "request" ? "Anura Request Report" : "Anura Response Report";
  return `${prefix} Last${REPORT_HOURS}Hours ${start}-${end} ${
    process.env.INDEX ?? ""
  }`;
};

// Checkpoint interface for BigQuery
interface AnuraReportCheckpoint {
  startDate: number;
  endDate?: number;
  reportHours?: number;
  requestReportId?: string;
  responseReportId?: string;
  requestReportName?: string;
  responseReportName?: string;
  requestReady?: boolean;
  responseReady?: boolean;
  requestMetadataId?: string;
  responseMetadataId?: string;
  status?: string;
  createdAt?: string;
  updatedAt?: string;
}

// Type for checkpoint storage in JSON file
type AnuraCheckpointStorage = Record<string, AnuraReportCheckpoint>;

// Load checkpoint from JSON file
const loadCheckpoint = async (
  startDate: number
): Promise<AnuraReportCheckpoint | null> => {
  try {
    const fs = require("fs");
    const path = require("path");
    const checkpointFile = path.join(process.cwd(), "anura-checkpoint.json");

    if (fs.existsSync(checkpointFile)) {
      const data = fs.readFileSync(checkpointFile, "utf8");
      const checkpoints: AnuraCheckpointStorage = JSON.parse(data);
      const checkpoint = checkpoints[startDate.toString()];

      if (checkpoint) {
        console.log("Loaded checkpoint from JSON file");
        return checkpoint;
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
  checkpoint: Partial<AnuraReportCheckpoint> & { startDate: number }
): Promise<void> => {
  try {
    const fs = require("fs");
    const path = require("path");
    const checkpointFile = path.join(process.cwd(), "anura-checkpoint.json");

    let checkpoints: AnuraCheckpointStorage = {};
    if (fs.existsSync(checkpointFile)) {
      const data = fs.readFileSync(checkpointFile, "utf8");
      checkpoints = JSON.parse(data);
    }

    const existingCheckpoint =
      checkpoints[checkpoint.startDate.toString()] || {};

    checkpoints[checkpoint.startDate.toString()] = {
      ...existingCheckpoint,
      ...checkpoint,
      updatedAt: new Date().toISOString(),
    };

    fs.writeFileSync(checkpointFile, JSON.stringify(checkpoints, null, 2));
    console.log("Checkpoint saved to JSON file");
  } catch (error) {
    console.error("Error saving checkpoint:", error);
  }
};

// Clear checkpoint from JSON file
// const clearCheckpoint = async (startDate: number): Promise<void> => {
//   try {
//     const fs = require("fs");
//     const path = require("path");
//     const checkpointFile = path.join(process.cwd(), "anura-checkpoint.json");

//     if (fs.existsSync(checkpointFile)) {
//       const data = fs.readFileSync(checkpointFile, "utf8");
//       const checkpoints: AnuraCheckpointStorage = JSON.parse(data);

//       if (checkpoints[startDate.toString()]) {
//         delete checkpoints[startDate.toString()];
//         fs.writeFileSync(checkpointFile, JSON.stringify(checkpoints, null, 2));
//         console.log("Checkpoint cleared from JSON file");
//       }
//     }
//   } catch (error) {
//     console.error("Error clearing checkpoint:", error);
//   }
// };

// Print report summary
const printReportSummary = (
  summary: Omit<
    AnuraReportSummary,
    "createdAt" | "updatedAt" | "id" | "anuraReportId"
  > & {
    sourceMetrics?: any[];
  }
): void => {
  console.log("\n" + "=".repeat(60));
  console.log("ANURA REPORT SUMMARY");
  console.log("=".repeat(60));

  console.log(`\nOVERALL SUMMARY:`);
  console.log(`Total Requests: ${summary.totalRequests.toLocaleString()}`);
  console.log(`Total Responses: ${summary.totalResponses.toLocaleString()}`);
  console.log(`Drop Rate: ${Number(summary.dropRate).toFixed(2)}%`);
  console.log(
    `Good: ${summary.good.toLocaleString()} (${Number(summary.goodRate).toFixed(
      2
    )}%)`
  );
  console.log(
    `Bad: ${summary.bad.toLocaleString()} (${Number(summary.badRate).toFixed(
      2
    )}%)`
  );
  console.log(
    `Warn: ${summary.warn.toLocaleString()} (${Number(summary.warnRate).toFixed(
      2
    )}%)`
  );

  if (summary.sourceMetrics && summary.sourceMetrics.length > 0) {
    console.log(
      `\nSOURCE BREAKDOWN (${summary.sourceMetrics.length} sources):`
    );
    console.log("================================================");

    // Sort by total requests descending
    const sortedMetrics = summary.sourceMetrics.sort(
      (a, b) => b.totalRequests - a.totalRequests
    );

    sortedMetrics.forEach((metric, index) => {
      console.log(`${index + 1}. ${metric.source}`);
      console.log(`   Requests: ${metric.totalRequests.toLocaleString()}`);
      console.log(`   Responses: ${metric.totalResponses.toLocaleString()}`);
      console.log(`   Drop Rate: ${Number(metric.dropRate).toFixed(2)}%`);
      console.log(
        `   Good: ${metric.good.toLocaleString()} (${Number(
          metric.goodRate
        ).toFixed(2)}%)`
      );
      console.log(
        `   Bad: ${metric.bad.toLocaleString()} (${Number(
          metric.badRate
        ).toFixed(2)}%)`
      );
      console.log(
        `   Warn: ${metric.warn.toLocaleString()} (${Number(
          metric.warnRate
        ).toFixed(2)}%)`
      );
      console.log("");
    });
  }

  console.log("\n" + "=".repeat(60));
};

// Save report data to database
const saveReportToBigQuery = async (
  startDate: number,
  endDate: number,
  reportHours: number,
  requestReportId: string,
  responseReportId: string,
  requestReportName: string,
  responseReportName: string,
  metrics: {
    totalRequests: number;
    totalResponses: number;
    dropRate: number;
    good: number;
    goodRate: number;
    bad: number;
    badRate: number;
    warn: number;
    warnRate: number;
    otherMetrics: {
      source?: string | null;
      totalRequests: number;
      totalResponses: number;
      dropRate: number;
      good: number;
      goodRate: number;
      bad: number;
      badRate: number;
      warn: number;
      warnRate: number;
    }[];
  },
  displayDate: Date
): Promise<void> => {
  try {
    // Create summary for BigQuery
    const bigQuerySummary: AnuraReportSummary = {
      startDate: startDate,
      endDate: endDate,
      reportHours: reportHours,
      requestReportId: requestReportId,
      responseReportId: responseReportId,
      requestReportName: requestReportName,
      responseReportName: responseReportName,
      totalRequests: metrics.totalRequests,
      totalResponses: metrics.totalResponses,
      dropRate: metrics.dropRate,
      good: metrics.good,
      goodRate: metrics.goodRate,
      bad: metrics.bad,
      badRate: metrics.badRate,
      warn: metrics.warn,
      warnRate: metrics.warnRate,
      status: "COMPLETED",
      displayDate,
    };

    // Insert report summary
    await insertAnuraReportSummary(bigQuerySummary);
    console.log("bigQuerySummary", bigQuerySummary);

    // Insert source metrics data
    const sourceMetricsData: AnuraSourceMetrics[] = metrics.otherMetrics.map(
      (metric) => ({
        startDate: startDate,
        endDate: endDate,
        reportHours: reportHours,
        requestReportId: requestReportId,
        responseReportId: responseReportId,
        source: metric.source || "(not set)",
        totalRequests: metric.totalRequests,
        totalResponses: metric.totalResponses,
        dropRate: metric.dropRate,
        good: metric.good,
        goodRate: metric.goodRate,
        bad: metric.bad,
        badRate: metric.badRate,
        warn: metric.warn,
        warnRate: metric.warnRate,
        displayDate,
      })
    );

    await insertAnuraSourceMetrics(sourceMetricsData);
    console.log("sourceMetricsData", sourceMetricsData);

    console.log(
      `Saved report data to BigQuery: ${metrics.otherMetrics.length} sources`
    );
  } catch (error) {
    console.error("Error saving report to BigQuery:", error);
    throw error;
  }
};

// Create a report request
const createAnuraReport = async (
  config: AnuraReportRequest
): Promise<AnuraReportResponse> => {
  try {
    console.log(`Creating ${config.type} report...`);
    console.log("Report creation payload:", JSON.stringify(config, null, 2));

    const response = await fetch(`${ANURA_BASE_URL}/raw/request`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(config),
    });

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    const data = await response.json();
    console.log(`${config.type} report created successfully`);
    console.log("Report creation response:", JSON.stringify(data, null, 2));

    return data;
  } catch (error) {
    console.error(`Error creating ${config.type} report:`, error);
    throw error;
  }
};

// Check if report is ready
const waitForReportReady = async (
  reportId: string,
  maxAttempts: number = 60
): Promise<boolean> => {
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      console.log(
        `Checking report status (attempt ${attempt}/${maxAttempts}) for report ${reportId}...`
      );
      const response = await fetch(`${ANURA_BASE_URL}/raw/table`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          token: ANURA_API_TOKEN,
        }),
      });

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      const data = await response.json();
      const tables = data.table.rows;
      console.log(`Found ${tables.length} reports in table`);

      const report = tables.find((table: any) => table.id === reportId);

      if (!report) {
        console.log(
          `Report ${reportId} not found in table. Available reports:`
        );
        tables.forEach((table: any) => {
          console.log(`  - ${table.id}: ${table.name} (${table.status})`);
        });
      } else {
        console.log(`Report ${reportId} found with status: ${report.status}`);
      }

      if (report?.status === "Ready") {
        console.log(`Report ${reportId} is ready!`);
        return true;
      } else if (report?.status === "Failed") {
        console.error(`Report ${reportId} failed to generate`);
        return false;
      }

      console.log(`Report status: ${report?.status || "Unknown"}`);
      await new Promise((resolve) => setTimeout(resolve, 10000)); // Wait 10 seconds
    } catch (error) {
      console.error(`Error checking report status:`, error);
    }
  }

  console.error(`Report ${reportId} timed out after ${maxAttempts} attempts`);
  return false;
};

// Get all available reports
const getAvailableReports = async (): Promise<any[]> => {
  try {
    const response = await fetch(`${ANURA_BASE_URL}/raw/table`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        token: ANURA_API_TOKEN,
      }),
    });

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    const data = await response.json();
    return data.table.rows;
  } catch (error) {
    console.error("Error fetching available reports:", error);
    throw error;
  }
};

// Find existing ready report that matches criteria
const findExistingReport = async (
  type: "request" | "response",
  reportName: string
): Promise<string | undefined> => {
  try {
    const reports = await getAvailableReports();
    const existingReport = reports.find(
      (report: any) =>
        report.status === "Ready" &&
        report.name === reportName &&
        report.type === type
    );

    if (existingReport) {
      console.log(`Found existing ready ${type} report: ${existingReport.id}`);
      return existingReport.id;
    }

    return undefined;
  } catch (error) {
    console.error("Error finding existing report:", error);
    return undefined;
  }
};

// Get report metadata
const getReportMetadata = async (
  reportId: string
): Promise<AnuraReportData> => {
  try {
    const response = await fetch(`${ANURA_BASE_URL}/raw/report`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        token: ANURA_API_TOKEN,
        raw: reportId,
      }),
    });

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    const data = await response.json();
    return data;
  } catch (error) {
    console.error("Error fetching report metadata:", error);
    throw error;
  }
};

// Download and parse report data
const downloadReportData = async (
  reportId: string,
  type: "request" | "response"
): Promise<any[]> => {
  try {
    console.log(`Downloading report data for ${reportId}...`);

    const response = await fetch(`${ANURA_BASE_URL}/raw/download/${reportId}`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        token: ANURA_API_TOKEN,
        raw: reportId,
      }),
    });

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    const data = (await response.json()) as { url: string };
    const url = decodeURIComponent(data.url);

    const fileResponse = await fetch(url);

    if (!fileResponse.ok) {
      throw new Error(`HTTP error! status: ${fileResponse.status}`);
    }

    const arrayBuffer = await fileResponse.arrayBuffer();
    const buffer = Buffer.from(arrayBuffer);

    // Check if response is a ZIP file
    const isZip = buffer.slice(0, 4).toString("hex") === "504b0304"; // ZIP file signature

    if (isZip) {
      console.log("Detected ZIP file, extracting CSV data...");

      // Extract ZIP file
      const tempDir = path.join(tmpdir(), `anura-report-${type}`);

      // Create temp directory if it doesn't exist
      if (!fs.existsSync(tempDir)) {
        fs.mkdirSync(tempDir);
      }

      const zipPath = path.join(tempDir, `${reportId}-$.zip`);
      fs.writeFileSync(zipPath, buffer);

      // Extract ZIP
      await extract(zipPath, { dir: tempDir });

      // Find CSV file
      const files = fs.readdirSync(tempDir);
      const csvFile = files.find((file) => file.endsWith(".csv"));

      if (!csvFile) {
        throw new Error("No CSV file found in ZIP");
      }

      const csvPath = path.join(tempDir, csvFile);
      const csvContent = fs.readFileSync(csvPath, "utf-8");

      if (process.env.GCP_BUCKET) {
        await uploadToGCP(
          csvPath,
          csvFile,
          new Date(),
          process.env.GCP_BUCKET
        ).catch(console.error);
      }

      console.log(`CSV file: ${csvPath}`);

      // Parse CSV
      const result = Papa.parse(csvContent, { header: true });

      // Clean up temp files
      fs.unlinkSync(zipPath);
      //   fs.unlinkSync(csvPath);

      console.log(`Downloaded and parsed ${result.data.length} rows`);
      return result.data;
    } else {
      // Try to parse as JSON (fallback)
      const jsonData = JSON.parse(buffer.toString());
      console.log(`Downloaded and parsed ${jsonData.length} rows`);
      return jsonData;
    }
  } catch (error) {
    console.error(`Error downloading report data:`, error);
    throw error;
  }
};

// Calculate metrics from report data
const calculateMetrics = (
  requestData: {
    SOURCE: string;
    CAMPAIGN: string;
  }[],
  responseData: {
    RESULT: "good" | "bad" | "warn";
    SOURCE: string;
    CAMPAIGN: string;
  }[]
): ReportMetrics => {
  const totalRequests = requestData.length;
  const totalResponses = responseData.length;

  // Count results
  const resultCounts: Record<string, number> = responseData.reduce(
    (acc: Record<string, number>, row) => {
      const result = (row.RESULT || "").toLowerCase();
      acc[result] = (acc[result] || 0) + 1;
      return acc;
    },
    {} as Record<string, number>
  );

  const good = resultCounts.good || 0;
  const bad = resultCounts.bad || 0;
  const warn = resultCounts.warn || 0;

  // Calculate rates
  const goodRate = totalResponses > 0 ? (good / totalResponses) * 100 : 0;
  const badRate = totalResponses > 0 ? (bad / totalResponses) * 100 : 0;
  const warnRate = totalResponses > 0 ? (warn / totalResponses) * 100 : 0;
  const dropRate =
    totalRequests > 0
      ? ((totalRequests - totalResponses) / totalRequests) * 100
      : 0;

  // Calculate metrics by source
  const sourceMap = new Map<
    string,
    {
      requests: number;
      responses: { good: number; bad: number; warn: number };
    }
  >();

  // Count requests by source
  requestData.forEach((row) => {
    const source = row.SOURCE;
    const existing = sourceMap.get(source);
    if (existing) {
      existing.requests++;
    } else {
      sourceMap.set(source, {
        requests: 1,
        responses: { good: 0, bad: 0, warn: 0 },
      });
    }
  });

  // Count responses by source
  responseData.forEach((row) => {
    const source = row.SOURCE;
    const existing = sourceMap.get(source);
    if (existing) {
      const result = (row.RESULT || "").toLowerCase();
      if (result === "good") existing.responses.good++;
      else if (result === "bad") existing.responses.bad++;
      else if (result === "warn") existing.responses.warn++;
    }
  });

  // Convert to SourceMetrics array
  const otherMetrics: SourceMetrics[] = Array.from(sourceMap.entries()).map(
    ([source, data]) => {
      const totalResponses =
        data.responses.good + data.responses.bad + data.responses.warn;

      return {
        source,
        totalRequests: data.requests,
        totalResponses,
        dropRate:
          data.requests > 0
            ? ((data.requests - totalResponses) / data.requests) * 100
            : 0,
        good: data.responses.good,
        goodRate:
          totalResponses > 0 ? (data.responses.good / totalResponses) * 100 : 0,
        bad: data.responses.bad,
        badRate:
          totalResponses > 0 ? (data.responses.bad / totalResponses) * 100 : 0,
        warn: data.responses.warn,
        warnRate:
          totalResponses > 0 ? (data.responses.warn / totalResponses) * 100 : 0,
      };
    }
  );

  return {
    totalRequests,
    totalResponses,
    dropRate,
    good,
    goodRate,
    bad,
    badRate,
    warn,
    warnRate,
    otherMetrics,
  };
};

const getReportDate = (): Date => {
  const reportDate = new Date(process.env.REPORT_DATE || Date.now());
  return reportDate;
};

// Main function to generate Anura report
async function generateAnuraReport(): Promise<AnuraReportSummary> {
  const reportDate = getReportDate();

  // as anura doesn't allow time range
  // we need to get data for last 3 days
  // get from 2 days ago
  const from = new Date(reportDate);
  from.setDate(from.getDate() - 2);

  // get till today
  const to = new Date(reportDate);

  console.log(from.toISOString());
  console.log(to.toISOString());

  const startDate = formatDateToNumber(from);
  const endDate = formatDateToNumber(to);

  const requestReportName = getReportName("request", startDate, endDate);
  const responseReportName = getReportName("response", startDate, endDate);

  // Initialize BigQuery
  await initializeBigQuery();

  // Check if force rerun is enabled
  const forceRerun = process.env.FORCE_RERUN === "true";

  if (forceRerun) {
    console.log(
      `FORCE_RERUN enabled - will generate new report for ${startDate}-${endDate}`
    );
  }

  // Load checkpoint (skip if force rerun)
  let checkpoint = await loadCheckpoint(startDate);

  if (!forceRerun) {
    console.log(
      `Report for ${startDate}-${endDate} already completed according to checkpoint.`
    );

    // Check if data actually exists in BigQuery
    try {
      const existingData = await queryAnuraReports(startDate, 1);
      if (existingData.length > 0) {
        console.log(existingData);
        console.log("Data found in BigQuery, skipping report generation");
        return {
          startDate: startDate,
          endDate: endDate,
          reportHours: REPORT_HOURS,
          requestReportId: existingData?.[0]?.["request_report_id"] || "",
          responseReportId: existingData?.[0]?.["response_report_id"] || "",
          requestReportName: existingData?.[0]?.["request_report_name"] || "",
          responseReportName: existingData?.[0]?.["response_report_name"] || "",
          totalRequests: 0, // Would need to query summary table for actual values
          totalResponses: 0,
          dropRate: 0,
          good: 0,
          goodRate: 0,
          bad: 0,
          badRate: 0,
          warn: 0,
          warnRate: 0,
          status: "COMPLETED",
          displayDate: from,
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
      startDate,
      endDate,
      reportHours: REPORT_HOURS,
      requestReportName,
      responseReportName,
      status: "PROCESSING",
    };
  }

  try {
    console.log("Starting Anura Report Generation...");

    // Check for existing ready reports first
    console.log("Checking for existing ready reports...");
    if (!checkpoint.requestReportId) {
      const found = await findExistingReport("request", requestReportName);
      checkpoint.requestReportId = found || undefined;
      await saveCheckpoint({
        startDate,
        requestReportId: checkpoint.requestReportId,
      });
    }
    if (!checkpoint.responseReportId) {
      const found = await findExistingReport("response", responseReportName);
      checkpoint.responseReportId = found || undefined;
      await saveCheckpoint({
        startDate,
        responseReportId: checkpoint.responseReportId,
      });
    }

    // Create reports if not found
    if (!checkpoint.requestReportId) {
      const requestReport = await createAnuraReport({
        token: ANURA_API_TOKEN,
        instance: INSTANCE_ID,
        type: "request",
        start: startDate,
        end: endDate,
        standard: ["source", "campaign"],
        name: requestReportName,
        send_email: false,
      });

      console.log("Request report created with ID: ", requestReport);

      if (!requestReport.id) {
        throw new Error("Request report not created");
      }

      checkpoint.requestReportId = requestReport.id;
      await saveCheckpoint({
        startDate,
        requestReportId: checkpoint.requestReportId,
      });
      console.log(`Request report created with ID: ${requestReport.id}`);
    }

    // Wait for request report to be ready
    if (!checkpoint.requestReady) {
      console.log("Waiting for request report to be ready...");
      checkpoint.requestReady = await waitForReportReady(
        checkpoint.requestReportId
      );
      await saveCheckpoint({
        startDate,
        requestReady: checkpoint.requestReady,
      });
      if (!checkpoint.requestReady) throw new Error("Request report not ready");
    }

    if (!checkpoint.responseReportId) {
      const responseReport = await createAnuraReport({
        token: ANURA_API_TOKEN,
        instance: INSTANCE_ID,
        type: "response",
        start: startDate,
        end: endDate,
        standard: [
          "source",
          "campaign",
          "result",
          "rule_sets",
          "invalid_traffic_type",
          "ad_blocker",
          "page",
        ],
        name: responseReportName,
        send_email: false,
      });

      if (!responseReport.id) {
        throw new Error("Response report not created");
      }

      checkpoint.responseReportId = responseReport.id;
      await saveCheckpoint({
        startDate,
        responseReportId: checkpoint.responseReportId,
      });
      console.log(
        `Response report created with ID: ${checkpoint.responseReportId}`
      );
    }

    // Wait for response report to be ready
    if (!checkpoint.responseReady) {
      console.log("Waiting for response report to be ready...");
      checkpoint.responseReady = await waitForReportReady(
        checkpoint.responseReportId
      );
      await saveCheckpoint({
        startDate,
        responseReady: checkpoint.responseReady,
      });
      if (!checkpoint.responseReady)
        throw new Error("Response report not ready");
    }

    // Get report metadata
    if (!checkpoint.requestMetadataId) {
      const requestMetadata = await getReportMetadata(
        checkpoint.requestReportId
      );
      checkpoint.requestMetadataId = requestMetadata.id;
      await saveCheckpoint({
        startDate,
        requestMetadataId: checkpoint.requestMetadataId,
      });
    }
    if (!checkpoint.responseMetadataId) {
      const responseMetadata = await getReportMetadata(
        checkpoint.responseReportId
      );
      checkpoint.responseMetadataId = responseMetadata.id;
      await saveCheckpoint({
        startDate,
        responseMetadataId: checkpoint.responseMetadataId,
      });
    }

    // Download and parse report data
    console.log("Downloading report data...");
    const [requestData, responseData] = await Promise.all([
      downloadReportData(checkpoint.requestMetadataId, "request"),
      downloadReportData(checkpoint.responseMetadataId, "response"),
    ]);

    // filter out data
    // if today is 14 july,
    // we need data from 12 JULY PST to 13 JULY PST
    // UTC-8 is 8 hours behind UTC
    function getLosAngelesMidnightRange(reportDate: string | Date) {
      const tz = "America/Los_Angeles";

      // Parse report date as LA time
      const laDate = DateTime.fromISO(new Date(reportDate).toISOString(), {
        zone: tz,
      });

      // Go to previous day and get start & end of that day
      const start = laDate.minus({ days: 1 }).startOf("day").toUTC();
      const end = start.plus({ days: 1 });

      return {
        startTime: start.toISO(),
        endTime: end.toISO(),
      };
    }

    const { startTime, endTime } = getLosAngelesMidnightRange(reportDate);

    if (!startTime || !endTime) {
      throw new Error("Failed to get start and end time");
    }

    const filteredRequestData = requestData.filter((row) => {
      const rowDate = new Date(row.TIMESTAMP);
      return (
        rowDate.getTime() >= new Date(startTime).getTime() &&
        rowDate.getTime() <= new Date(endTime).getTime()
      );
    });
    const filteredResponseData = responseData.filter((row) => {
      const rowDate = new Date(row.TIMESTAMP);
      return (
        rowDate.getTime() >= new Date(startTime).getTime() &&
        rowDate.getTime() <= new Date(endTime).getTime()
      );
    });

    console.log(filteredRequestData.length);
    console.log(filteredResponseData.length);

    // Calculate metrics
    const metrics = calculateMetrics(filteredRequestData, filteredResponseData);

    // Print report
    printReportSummary({
      startDate: formatDateToNumber(new Date(startTime)),
      endDate: formatDateToNumber(new Date(endTime)),
      reportHours: REPORT_HOURS,
      requestReportId: checkpoint.requestReportId || "",
      responseReportId: checkpoint.responseReportId || "",
      requestReportName: requestReportName,
      responseReportName: responseReportName,
      totalRequests: metrics.totalRequests,
      totalResponses: metrics.totalResponses,
      dropRate: metrics.dropRate,
      good: metrics.good,
      goodRate: metrics.goodRate,
      bad: metrics.bad,
      badRate: metrics.badRate,
      warn: metrics.warn,
      warnRate: metrics.warnRate,
      status: "COMPLETED",
      sourceMetrics: metrics.otherMetrics.map((m) => ({
        source: m.source,
        totalRequests: m.totalRequests,
        totalResponses: m.totalResponses,
        dropRate: m.dropRate,
        good: m.good,
        goodRate: m.goodRate,
        bad: m.bad,
        badRate: m.badRate,
        warn: m.warn,
        warnRate: m.warnRate,
      })),
      displayDate: from,
    });

    // Save to BigQuery
    await saveReportToBigQuery(
      formatDateToNumber(new Date(startTime)),
      formatDateToNumber(new Date(endTime)),
      REPORT_HOURS,
      checkpoint.requestReportId || "",
      checkpoint.responseReportId || "",
      requestReportName,
      responseReportName,
      metrics,
      from
    );

    // Mark checkpoint as completed
    await saveCheckpoint({
      startDate,
      status: "COMPLETED",
    });

    // await clearCheckpoint(startDate);

    console.log("Anura Report completed successfully!");

    // Return a summary object for compatibility
    return {
      startDate: startDate,
      endDate: endDate,
      reportHours: REPORT_HOURS,
      requestReportId: checkpoint.requestReportId || "",
      responseReportId: checkpoint.responseReportId || "",
      requestReportName: requestReportName,
      responseReportName: responseReportName,
      totalRequests: metrics.totalRequests,
      totalResponses: metrics.totalResponses,
      dropRate: metrics.dropRate,
      good: metrics.good,
      goodRate: metrics.goodRate,
      bad: metrics.bad,
      badRate: metrics.badRate,
      warn: metrics.warn,
      warnRate: metrics.warnRate,
      status: "COMPLETED",
      displayDate: from,
    };
  } catch (error) {
    console.error("Error generating Anura report:", error);
    await saveCheckpoint({
      startDate,
      status: "FAILED",
    });
    throw error;
  }
}

// Run the report
const main = async () => {
  try {
    const reportSummary = await generateAnuraReport();
    console.log("Report generation completed!", reportSummary.requestReportId);
  } catch (error) {
    console.error("Failed to generate report:", error);
    process.exit(1);
  }
};

// Run if this file is executed directly
if (require.main === module) {
  main();
}

export { generateAnuraReport, ReportMetrics, SourceMetrics };
