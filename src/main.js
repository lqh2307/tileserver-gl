"use strict";

import { removeOldCacheLocks, runCommand } from "./utils.js";
import { config, validateConfigFile } from "./config.js";
import { validateCleanUpFile } from "./cleanup.js";
import { validateSeedFile } from "./seed.js";
import { printLog } from "./logger.js";
import chokidar from "chokidar";
import cluster from "cluster";
import cron from "node-cron";
import os from "os";
import {
  cancelTaskInWorker,
  startTaskInWorker,
  startServer,
} from "./server.js";

/**
 * Start cluster server
 * @returns {Promise<void>}
 */
async function startClusterServer() {
  if (cluster.isPrimary === true) {
    /* Set default ENVs */
    process.env.DATA_DIR = process.env.DATA_DIR || "data"; // Data dir
    process.env.SERVICE_NAME = process.env.SERVICE_NAME || "tile-server"; // Service name
    process.env.RESTART_AFTER_CONFIG_CHANGE =
      process.env.RESTART_AFTER_CONFIG_CHANGE || "true"; // Restart server after config change
    process.env.LOGGING_TO_FILE = process.env.LOGGING_TO_FILE || "true"; // Logging to file

    let log = `Starting server with:`;
    log += `\n\tData dir: ${process.env.DATA_DIR}`;
    log += `\n\tService name: ${process.env.SERVICE_NAME}`;
    log += `\n\tRestart server after config change: ${process.env.RESTART_AFTER_CONFIG_CHANGE}`;
    log += `\n\tLogging to file: ${process.env.LOGGING_TO_FILE}`;

    printLog("info", log);

    /* Validate config.json, seed.json and cleanup.json files */
    printLog(
      "info",
      "Validate config.json, seed.json and cleanup.json files..."
    );

    try {
      await Promise.all([
        validateConfigFile(),
        validateSeedFile(),
        validateCleanUpFile(),
      ]);
    } catch (error) {
      printLog(
        "error",
        `Failed to validate config.json, seed.json and cleanup.json files: ${error}`
      );

      process.exit(1);
    }

    // Store ENVs
    process.env.NUM_OF_PROCESS =
      process.env.NUM_OF_PROCESS || config.options?.process || 1; // Number of process
    process.env.UV_THREADPOOL_SIZE =
      process.env.NUM_OF_THREAD || config.options?.thread || os.cpus().length; // For libuv (Number of thread)
    process.env.POSTGRESQL_BASE_URI =
      config.options?.postgreSQLBaseURI || "postgresql://localhost:5432"; // PostgreSQL base URI
    process.env.SERVE_FRONT_PAGE = config.options?.serveFrontPage || "true"; // Serve front page
    process.env.SERVE_SWAGGER = config.options?.serveSwagger || "true"; // Serve swagger
    process.env.LISTEN_PORT =
      process.env.LISTEN_PORT || config.options?.listenPort || 8080; // Server port

    // Check GDAL
    try {
      const gdalVersion = await runCommand("gdalinfo --version");

      printLog(
        "info",
        `Found gdal version "${gdalVersion.trim()}". Enable export render!`
      );

      process.env.ENABLE_EXPORT = "true";
      process.env.GDAL_NUM_THREADS = "ALL_CPUS";
    } catch (error) {
      printLog("info", "Not found gdal. Disable export render!");
    }

    // Check MLGL
    try {
      await import("@maplibre/maplibre-gl-native");

      printLog(
        "info",
        `Success to import "@maplibre/maplibre-gl-native". Enable backend render!`
      );

      process.env.BACKEND_RENDER = "true";
    } catch (error) {
      printLog(
        "error",
        `Failed to import "@maplibre/maplibre-gl-native": ${error}. Disable backend render!`
      );

      process.env.BACKEND_RENDER = "false";
    }

    /* Remove old cache locks */
    printLog("info", "Removing old cache locks before start server...");

    await removeOldCacheLocks();

    printLog(
      "info",
      `Starting server with ${process.env.NUM_OF_PROCESS} processes - ${process.env.UV_THREADPOOL_SIZE} threads...`
    );

    /* Setup watch config file change */
    if (process.env.RESTART_AFTER_CONFIG_CHANGE === "true") {
      chokidar
        .watch(`${process.env.DATA_DIR}/config.json`, {
          usePolling: true,
          awaitWriteFinish: true,
          interval: 500,
        })
        .on("change", () => {
          printLog("info", "Config file has changed. Restarting server...");

          process.exit(1);
        });
    }

    /* Setup cron */
    if (config.options?.taskSchedule !== undefined) {
      printLog(
        "info",
        `Schedule run seed and clean up tasks at: "${config.options.taskSchedule}"`
      );

      cron.schedule(config.options.taskSchedule, () => {
        printLog(
          "info",
          "Seed and clean up tasks triggered by schedule. Starting task..."
        );

        startTaskInWorker({
          restart: true,
          cleanUpSprites: true,
          cleanUpFonts: true,
          cleanUpStyles: true,
          cleanUpGeoJSONs: true,
          cleanUpDatas: true,
          seedSprites: true,
          seedFonts: true,
          seedStyles: true,
          seedGeoJSONs: true,
          seedDatas: true,
        });
      });
    }

    /* Fork servers */
    printLog("info", "Creating workers...");

    for (let i = 0; i < Number(process.env.NUM_OF_PROCESS); i++) {
      cluster.fork();
    }

    cluster
      .on("exit", (worker, code, signal) => {
        printLog(
          "info",
          `Worker with PID = ${worker.process.pid} is died - Code: ${code} - Signal: ${signal}. Creating new one...`
        );

        cluster.fork();
      })
      .on("message", (worker, message) => {
        switch (message.action) {
          case "killServer": {
            printLog(
              "info",
              `Received "killServer" message from worker with PID = ${worker.process.pid}. Killing server...`
            );

            process.exit(0);
          }

          case "restartServer": {
            printLog(
              "info",
              `Received "restartServer" message from worker with PID = ${worker.process.pid}. Restarting server...`
            );

            process.exit(1);
          }

          case "startTask": {
            printLog(
              "info",
              `Received "startTask" message from worker with PID = ${worker.process.pid}. Starting task...`
            );

            startTaskInWorker(message);

            break;
          }

          case "cancelTask": {
            printLog(
              "info",
              `Received "cancelTask" message from worker with PID = ${worker.process.pid}. Canceling task...`
            );

            cancelTaskInWorker();

            break;
          }

          default: {
            printLog(
              "warn",
              `Received unknown message "${message.action}" from worker with PID = ${worker.process.pid}. Skipping...`
            );

            break;
          }
        }
      });
  } else {
    startServer();
  }
}

/* Run start cluster server */
startClusterServer();
