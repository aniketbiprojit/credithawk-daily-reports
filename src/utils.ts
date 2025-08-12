import * as fs from "fs";
import { Storage } from "@google-cloud/storage";

/**
 * Uploads a file to Google Cloud Storage
 * @param localFilePath Path to the local file
 * @param filename Name for the file in GCS
 * @param reportDate Date of the report for organizing in GCS
 * @param bucketName GCS bucket name
 */
export async function uploadToGCP(
  localFilePath: string,
  filename: string,
  reportDate: Date,
  bucketName: string
): Promise<void> {
  try {
    console.log("Uploading file to GCP:", localFilePath);
    // Initialize Google Cloud Storage
    const storage = new Storage();
    const bucket = storage.bucket(bucketName);

    // Create GCS path with YEAR/MONTH structure
    const year = reportDate.getFullYear();
    const month = String(reportDate.getMonth() + 1).padStart(2, "0");
    const gcsPath = `${year}/${month}/${filename}`;

    // Upload file
    await bucket.upload(localFilePath, {
      destination: gcsPath,
      metadata: {
        metadata: {
          reportDate: reportDate.toISOString().split("T")[0],
          uploadedAt: new Date().toISOString(),
        },
      },
    });

    console.log(`File uploaded to GCS: gs://${bucketName}/${gcsPath}`);

    // Optionally clean up local file after upload
    if (process.env.CLEANUP_LOCAL_FILES === "true") {
      fs.unlinkSync(localFilePath);
      console.log(`Local file cleaned up: ${localFilePath}`);
    }
  } catch (error) {
    console.error("Error uploading to GCP:", error);
    // Don't throw here - we still want the local file even if GCP upload fails
  }
}

/**
 * Helper function to format date as YYYY-MM-DD
 */
export function formatDateToString(date: Date): string {
  return date.toISOString().split("T")[0];
}
