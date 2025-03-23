"use strict";

import { parentPort, workerData } from "node:worker_threads";
import { initLogger, printLog } from "./logger.js";
import { runTasks } from "./task.js";

(async () => {
  try {
    /* Init logger */
    initLogger();

    printLog("info", "Starting seed and clean up task...");

    /* Run task */
    await runTasks(workerData);

    /* Restart server */
    if (workerData.restart !== "false") {
      printLog(
        "info",
        "Completed seed and clean up tasks. Restarting server..."
      );

      parentPort.postMessage({
        action: "restartServer",
      });
    }
  } catch (error) {
    parentPort.postMessage({
      error: error,
    });
  }
})();
