"use strict";

import { serve_prometheus } from "./serve_prometheus.js";
import { config, loadConfigFile } from "./config.js";
import { loggerMiddleware } from "./middleware.js";
import { serve_summary } from "./serve_summary.js";
import { serve_geojson } from "./serve_geojson.js";
import { serve_swagger } from "./serve_swagger.js";
import { serve_common } from "./serve_common.js";
import { serve_sprite } from "./serve_sprite.js";
import { serve_style } from "./serve_style.js";
import { Worker } from "node:worker_threads";
import { serve_font } from "./serve_font.js";
import { serve_data } from "./serve_data.js";
import { serve_task } from "./serve_task.js";
import { loadSeedFile } from "./seed.js";
import { printLog } from "./logger.js";
import express from "express";
import cors from "cors";

let currentTaskWorker;

/**
 * Start task in worker
 * @param {Object} opts Options
 * @returns {void}
 */
export function startTaskInWorker(opts) {
  if (currentTaskWorker === undefined) {
    currentTaskWorker = new Worker("./src/task_worker.js", {
      workerData: opts,
    })
      .on("error", (error) => {
        printLog("error", `Task worker error: ${error}`);

        currentTaskWorker = undefined;
      })
      .on("exit", (code) => {
        currentTaskWorker = undefined;

        if (code !== 0) {
          printLog("error", `Task worker exited with code: ${code}`);
        }
      })
      .on("message", (message) => {
        if (message.error) {
          printLog("error", `Task worker error: ${message.error}`);
        }

        if (message.action === "restartServer") {
          process.exit(1);
        }

        currentTaskWorker = undefined;
      });
  } else {
    printLog("warn", "A task is already running. Skipping start task...");
  }
}

/**
 * Cancel task in worker
 * @returns {void}
 */
export function cancelTaskInWorker() {
  if (currentTaskWorker !== undefined) {
    currentTaskWorker
      .terminate()
      .catch((error) => {
        printLog("error", `Task worker error: ${error}`);
      })
      .finally(() => {
        currentTaskWorker = undefined;
      });
  } else {
    printLog("warn", "No task is currently running. Skipping cancel task...");
  }
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
    .then(() => serve_style.add())
    .then(() => {
      printLog("info", "Completed startup!");

      config.isStarted = true;
    })
    .catch((error) => {
      throw new Error(`Failed to load data: ${error}`);
    });
}

/**
 * Load configs
 * @returns {Promise<void>}
 */
async function loadConfigs() {
  try {
    printLog("info", "Loading config.json and seed.json files...");

    await Promise.all([loadConfigFile(), loadSeedFile()]);
  } catch (error) {
    throw new Error(`Failed to load config.json and seed.json files: ${error}`);
  }
}

/**
 * Start server
 * @returns {Promise<void>}
 */
export async function startServer() {
  try {
    /* Load configs */
    await loadConfigs();

    /* Start HTTP server */
    printLog("info", "Starting HTTP server...");

    const app = express()
      .disable("x-powered-by")
      .enable("trust proxy")
      .use(cors())
      .use(express.json())
      .use(loggerMiddleware())
      .use(express.static("public/resources"));

    /* Register common handlers */
    serve_common.init(app);
    serve_swagger.init(app);
    serve_prometheus.init(app);

    const server = app
      .listen(config.options?.listenPort || 8080, () => {
        printLog(
          "info",
          `HTTP server is listening on port "${server.address().port}"...`
        );
      })
      .on("error", (error) => {
        printLog("error", `HTTP server is stopped by: ${error}`);
      });

    /* Load datas */
    await loadData();

    /* Register service handlers */
    serve_summary.init(app);
    serve_data.init(app);
    serve_geojson.init(app);
    serve_font.init(app);
    serve_sprite.init(app);
    serve_style.init(app);
    serve_task.init(app);
  } catch (error) {
    printLog("error", `Failed to start server: ${error}. Exited!`);

    process.send({
      action: "killServer",
    });
  }
}
