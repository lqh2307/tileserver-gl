"use strict";

import { cleanUp, config, seed } from "./configs/index.js";
import { setupPrimary } from "@socket.io/cluster-adapter";
import { setupMaster } from "@socket.io/sticky";
import { Worker } from "node:worker_threads";
import cluster from "node:cluster";
import express from "express";
import http from "node:http";
import path from "node:path";
import cors from "cors";
import {
  responseCompressionMiddleware,
  jsonBodyMiddleware,
  loggerMiddleware,
} from "./middlewares/index.js";
import {
  isTaskTargetMatched,
  resolveProjectPath,
  getTaskTargets,
  setupWSServer,
  getTaskKey,
  printLog,
} from "./utils/index.js";
import {
  serve_prometheus,
  serve_summary,
  serve_geojson,
  serve_swagger,
  serve_common,
  serve_sprite,
  serve_export,
  serve_render,
  serve_style,
  serve_font,
  serve_data,
  serve_task,
  serve_wmts,
  serve_wms,
  serve_wfs,
} from "./serves/index.js";

const taskJobs = new Map();
const taskQueue = [];
let currentTaskKey;
let restartAfterTasks;

/** Start the next queued resource-level task. */
function startNextTaskWorker() {
  if (currentTaskKey) {
    return;
  }

  let job;
  while (taskQueue.length && !job) {
    const queuedJob = taskQueue.shift();

    if (taskJobs.get(queuedJob.key) === queuedJob) {
      job = queuedJob;
    }
  }

  if (!job) {
    if (restartAfterTasks) {
      printLog("info", "All sync tasks completed. Restarting server...");

      process.exit(1);
    }

    return;
  }

  currentTaskKey = job.key;
  job.status = "running";

  printLog("info", `Starting sync task "${job.key}"...`);

  job.worker = new Worker(resolveProjectPath("src", "task_worker.js"), {
    workerData: job.target,
  })
    .on("error", (error) => {
      printLog("error", `Sync task "${job.key}" worker error: ${error}`);
    })
    .on("message", (message) => {
      if (message.error) {
        printLog(
          "error",
          `Sync task "${job.key}" failed: ${message.error.message || message.error}`,
        );
      }
    })
    .on("exit", (code) => {
      taskJobs.delete(job.key);

      currentTaskKey = undefined;

      if (job.cancelled) {
        printLog("info", `Canceled sync task "${job.key}".`);
      } else if (code !== 0) {
        printLog(
          "error",
          `Sync task "${job.key}" worker exited with code: ${code}`,
        );
      } else {
        printLog("info", `Completed sync task "${job.key}"!`);

        if (job.restart) {
          restartAfterTasks = true;
        }
      }

      startNextTaskWorker();
    });
}

/**
 * Start task in worker
 * @param {{ [key: string]: any }} opts Options
 * @returns {number} Number of newly queued tasks
 */
export function startTaskInWorker(opts) {
  const targets = getTaskTargets(opts, seed, cleanUp);

  let queuedTasks = 0;

  for (const target of targets) {
    const key = getTaskKey(target);
    const current = taskJobs.get(key);

    if (current) {
      if (opts.restart === true) {
        current.restart = true;
      }

      printLog("warn", `Sync task "${key}" is already queued or running.`);

      continue;
    }

    const job = {
      key,
      target,
      restart: opts.restart === true,
      status: "queued",
    };

    taskJobs.set(key, job);
    taskQueue.push(job);

    queuedTasks++;
  }

  if (!queuedTasks) {
    printLog("warn", "No new sync task matched. Skipping...");
  }

  startNextTaskWorker();

  return queuedTasks;
}

/**
 * Cancel task in worker
 * @param {{ type?: string, id?: string }} selector Task selector
 * @returns {number} Number of canceled tasks
 */
export function cancelTaskInWorker(selector = {}) {
  let canceledTasks = 0;

  for (const job of taskJobs.values()) {
    if (job.cancelled || !isTaskTargetMatched(job.target, selector)) {
      continue;
    }

    job.cancelled = true;
    job.restart = false;

    canceledTasks++;

    if (job.status === "running") {
      job.worker.terminate().catch((error) => {
        printLog("error", `Failed to cancel sync task "${job.key}": ${error}`);
      });
    } else {
      taskJobs.delete(job.key);

      printLog("info", `Canceled queued sync task "${job.key}".`);
    }
  }

  if (!selector.type && !selector.id) {
    restartAfterTasks = false;
  }

  if (!canceledTasks) {
    printLog("warn", "No matching sync task is running or queued.");
  }

  if (!currentTaskKey) {
    startNextTaskWorker();
  }

  return canceledTasks;
}

/**
 * Load data
 * @returns {Promise<void>}
 */
async function loadData() {
  /* Load datas */
  printLog("info", "Loading data...");

  await Promise.all([
    serve_font.add(),
    serve_sprite.add(),
    serve_data.add(),
    serve_geojson.add(),
  ])
    .then(serve_style.add)
    .then(() => {
      printLog("info", "Completed startup!");

      config.isStarted = true;
    })
    .catch((error) => {
      throw new Error(`Failed to load data: ${error}`);
    });
}

/**
 * Setup static folders
 * @param {express.Application} app - Express app
 * @returns {void}
 */
function setupStaticFolders(app) {
  printLog("info", "Setting statics...");

  app.use(express.static(resolveProjectPath("public", "resources")));
  app.use(
    "/statics",
    express.static(path.join(process.env.DATA_DIR, "statics")),
  );
}

/**
 * Start server
 * @returns {Promise<void>}
 */
export async function startServer() {
  try {
    const enableSocket = config.options?.enableSocket;

    if (cluster.isPrimary) {
      const server = http.createServer();

      if (enableSocket) {
        setupMaster(server, {
          loadBalancingMethod: "least-connection",
        });

        setupPrimary();

        server
          .listen(+process.env.LISTEN_PORT, () => {
            printLog(
              "info",
              `HTTP/WS server is listening on port "${process.env.LISTEN_PORT}"...`,
            );
          })
          .on("error", (error) => {
            printLog("error", `HTTP/WS server is stopped by: ${error}`);
          });
      } else {
        printLog(
          "info",
          `HTTP server is listening on port "${process.env.LISTEN_PORT}"...`,
        );
      }
    } else {
      const serverType = enableSocket ? "HTTP/WS" : "HTTP";

      /* Start HTTP/WS server */
      printLog("info", `Starting ${serverType} server...`);

      const app = express()
        .disable("x-powered-by")
        .enable("trust proxy")
        .use(
          cors({
            origin: "*",
          }),
        )
        .use(jsonBodyMiddleware())
        .use(responseCompressionMiddleware())
        .use(loggerMiddleware());

      setupStaticFolders(app);

      const server = http.createServer(app);

      if (enableSocket) {
        setupWSServer(server);
      }

      // Workers always need to listen
      server.listen(+process.env.LISTEN_PORT, () => {
        printLog("info", `${serverType} server worker is listening...`);
      });

      /* Load datas */
      await loadData();

      /* Register handlers */
      serve_common.init(app);
      serve_swagger.init(app);
      serve_prometheus.init(app);
      serve_summary.init(app);
      serve_export.init(app);
      serve_data.init(app);
      serve_geojson.init(app);
      serve_font.init(app);
      serve_sprite.init(app);
      serve_style.init(app);
      serve_task.init(app);
      serve_render.init(app);
      serve_wms.init(app);
      serve_wfs.init(app);
      serve_wmts.init(app);
    }
  } catch (error) {
    printLog("error", `Failed to start server: ${error}. Exited!`);

    process.send({
      action: "killServer",
    });
  }
}
