"use strict";

import { printLog, setupWSServer } from "./utils/index.js";
import { setupPrimary } from "@socket.io/cluster-adapter";
import { loggerMiddleware } from "./middlewares/index.js";
import { setupMaster } from "@socket.io/sticky";
import { Worker } from "node:worker_threads";
import { config } from "./configs/index.js";
import cluster from "node:cluster";
import express from "express";
import http from "node:http";
import cors from "cors";
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
} from "./serves/index.js";

let currentTaskWorker;

/**
 * Start task in worker
 * @param {object} opts Options
 * @returns {void}
 */
export function startTaskInWorker(opts) {
  if (!currentTaskWorker) {
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
          printLog(
            "info",
            `Received "${message.action}" message from task worker. Restarting server...`,
          );

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
  if (currentTaskWorker) {
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

  app.use(express.static("public/resources"));
  app.use("/statics", express.static(`${process.env.DATA_DIR}/statics`));
}

/**
 * Start server
 * @returns {Promise<void>}
 */
export async function startServer() {
  try {
    if (cluster.isPrimary) {
      const server = http.createServer();

      if (config.enableSocket) {
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
      const serverType = config.enableSocket ? "HTTP/WS" : "HTTP";

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
        .use(
          express.json({
            limit: "1gb",
          }),
        )
        .use(loggerMiddleware());

      setupStaticFolders(app);

      const server = http.createServer(app);

      if (config.enableSocket) {
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
    }
  } catch (error) {
    printLog("error", `Failed to start server: ${error}. Exited!`);

    process.send({
      action: "killServer",
    });
  }
}
