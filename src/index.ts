import { generateAdxYesterdayReport } from "./adx-report";
import { generateAnuraReport } from "./anura-report";
import { initializeBigQuery } from "./bigquery";
import { generateGa4YesterdayReport } from "./ga4-report";

if (!process.env.REPORT_DATE) {
  process.env.REPORT_DATE = new Date().toISOString().split("T")[0];
}

const fails: string[] = [];
const success: string[] = [];

export async function sendSlackNotification({
  fails,
  success,
}: {
  fails: string[];
  success: string[];
}) {
  const slackWebhookUrl = process.env.SLACK_WEBHOOK_URL;
  if (!slackWebhookUrl) {
    return;
  }

  const payload = {
    blocks: [
      {
        type: "header",
        text: {
          type: "plain_text",
          text: "📄 Report Generation Completed!",
          emoji: true,
        },
      },
      {
        type: "section",
        text: {
          type: "mrkdwn",
          text: "*✅ Success:*\n" + success.join(", "),
        },
      },
      ...(fails.length > 0
        ? [
            {
              type: "section",
              text: {
                type: "mrkdwn",
                text: "*❌ Fails:*\n" + fails.join(", "),
              },
            },
          ]
        : []),
    ],
  };

  await fetch(slackWebhookUrl, {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

async function main() {
  try {
    try {
      // Initialize BigQuery
      await initializeBigQuery();

      await generateAdxYesterdayReport();
      success.push("adx");
    } catch (error) {
      console.error("Error generating adx reports:", error);
      fails.push("adx");
    }

    try {
      await generateAnuraReport();
      success.push("anura");
    } catch (error) {
      console.error("Error generating anura reports:", error);
      fails.push("anura");
    }

    try {
      await generateGa4YesterdayReport();
      success.push("ga4");
    } catch (error) {
      console.error("Error generating ga4 reports:", error);
      fails.push("ga4");
    }

    await sendSlackNotification({ fails, success });
  } catch (error) {
    console.error("Error generating reports:", error);
    fails.push("unknown");

    await sendSlackNotification({ fails: ["unknown failure"], success });
  }
}

if (require.main === module) {
  main()
    .then(() => {
      console.log("Report generation completed!");

      process.exit(0);
    })
    .catch((error) => {
      if (error?.response || error?.errors) {
        console.error(error.response);
        console.error(error.errors);
      }

      if (error?.response || error?.errors) {
        console.error(error.response);
        console.error(error.errors);
      }

      console.error("Report generation failed:", error);
      process.exit(1);
    });
}
