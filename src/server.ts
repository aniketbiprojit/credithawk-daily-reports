import Fastify from "fastify";
import { loadEnvConfig } from "@next/env";

loadEnvConfig(process.cwd());

const fastify = Fastify({});

fastify.get("/", async (_req, reply) => {
  return reply.send({
    status: "ok",
  });
});

fastify.get("/cron", async (_req, reply) => {
  const runCronJob = (await import("./index")).runCronJob;
  await runCronJob();
  return reply.send({
    status: "ok",
  });
});

fastify.listen(
  {
    host: "0.0.0.0",
    port: parseInt(process.env.PORT ?? "") || 8080,
  },
  () => {
    console.log(`Listening on port ${process.env.PORT ?? 8080}`);
  }
);
