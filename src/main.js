"use strict";

import { removeOldLocks, printLog } from "./utils/index.js";
import { validateConfig, config } from "./configs/index.js";
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
  if (cluster.isPrimary) {
    /* Set default ENVs */
    process.env.DATA_DIR = process.env.DATA_DIR || "data"; // Data dir
    process.env.SERVICE_NAME = process.env.SERVICE_NAME || "tile-server"; // Service name
    process.env.RESTART_AFTER_CONFIG_CHANGE =
      process.env.RESTART_AFTER_CONFIG_CHANGE || "true"; // Restart server after config change
    process.env.LOG_LEVEL = process.env.LOG_LEVEL || "info"; // Log level

    let log = `Starting ${process.env.SERVICE_NAME} server with:`;
    log += `\n\tData dir: ${process.env.DATA_DIR}`;
    log += `\n\tRestart server after config change: ${process.env.RESTART_AFTER_CONFIG_CHANGE}`;

    printLog("info", log);

    /* Validate config files */
    printLog("info", "Validate config files...");

    try {
      await Promise.all([
        validateConfig("config"),
        validateConfig("seed"),
        validateConfig("cleanup"),
      ]);
    } catch (error) {
      printLog("error", `Failed to validate config files: ${error}`);

      process.exit(1);
    }

    const configOptions = config.options || {};

    // Store ENVs
    process.env.NUM_OF_PROCESS =
      process.env.NUM_OF_PROCESS || configOptions.process || 1; // Number of process
    process.env.UV_THREADPOOL_SIZE =
      process.env.NUM_OF_THREAD || configOptions.thread || os.cpus().length; // For libuv (Number of thread)
    process.env.POSTGRESQL_BASE_URI =
      configOptions.postgreSQLBaseURI || "postgresql://localhost:5432"; // PostgreSQL base URI
    process.env.SERVE_FRONT_PAGE = configOptions.serveFrontPage || "true"; // Serve front page
    process.env.SERVE_SWAGGER = configOptions.serveSwagger || "true"; // Serve swagger
    process.env.LISTEN_PORT =
      process.env.LISTEN_PORT || configOptions.listenPort || 8080; // Server port

    // Check MLGL
    try {
      await import("@maplibre/maplibre-gl-native");

      printLog("info", "Enable backend render!");

      process.env.BACKEND_RENDER = "true";
    } catch (error) {
      printLog(
        "warn",
        `Failed to import "@maplibre/maplibre-gl-native": ${error}. Disable backend render!`,
      );

      process.env.BACKEND_RENDER = "false";
    }

    /* Remove old locks */
    printLog("info", "Removing old locks before start server...");

    await removeOldLocks();

    printLog(
      "info",
      `Starting server with ${process.env.NUM_OF_PROCESS} processes - ${process.env.UV_THREADPOOL_SIZE} threads...`,
    );

    /* Setup watch config file change */
    if (process.env.RESTART_AFTER_CONFIG_CHANGE === "true") {
      chokidar
        .watch(
          [
            `${process.env.DATA_DIR}/config.json`,
            `${process.env.DATA_DIR}/seed.json`,
            `${process.env.DATA_DIR}/cleanup.json`,
          ],
          {
            usePolling: true,
            awaitWriteFinish: true,
            interval: 500, // 500 mliliseconds
          },
        )
        .on("change", () => {
          printLog("info", "Config file has changed. Restarting server...");

          process.exit(1);
        });
    }

    /* Setup task cron */
    if (configOptions.taskSchedule !== undefined) {
      printLog(
        "info",
        `Schedule run seed and cleanup tasks at: "${configOptions.taskSchedule}"`,
      );

      cron.schedule(configOptions.taskSchedule, () => {
        printLog(
          "info",
          "Seed and cleanup tasks triggered by schedule. Starting task...",
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

    /* Setup task cron */
    if (configOptions.restartSchedule !== undefined) {
      printLog(
        "info",
        `Schedule restart server at: "${configOptions.restartSchedule}"`,
      );

      cron.schedule(configOptions.restartSchedule, () => {
        printLog(
          "info",
          "Restart server triggered by schedule. Restarting server...",
        );

        process.exit(1);
      });
    }

    /* Start server */
    startServer();

    /* Fork servers */
    printLog("info", "Creating workers...");

    for (let i = 0; i < +process.env.NUM_OF_PROCESS; i++) {
      cluster.fork();
    }

    cluster
      .on("exit", (worker, code, signal) => {
        printLog(
          "info",
          `Worker with PID = ${worker.process.pid} is died - Code: ${code} - Signal: ${signal}. Creating new one...`,
        );

        cluster.fork();
      })
      .on("message", (worker, message) => {
        switch (message.action) {
          case "killServer": {
            printLog(
              "info",
              `Received "${message.action}" message from worker with PID = ${worker.process.pid}. Killing server...`,
            );

            process.exit(0);
          }

          case "restartServer": {
            printLog(
              "info",
              `Received "${message.action}" message from worker with PID = ${worker.process.pid}. Restarting server...`,
            );

            process.exit(1);
          }

          case "startTask": {
            printLog(
              "info",
              `Received "${message.action}" message from worker with PID = ${worker.process.pid}. Starting task...`,
            );

            startTaskInWorker(message);

            break;
          }

          case "cancelTask": {
            printLog(
              "info",
              `Received "${message.action}" message from worker with PID = ${worker.process.pid}. Canceling task...`,
            );

            cancelTaskInWorker();

            break;
          }
        }
      });
  } else {
    /* Start server */
    startServer();
  }
}

/* Run start cluster server */
startClusterServer();
