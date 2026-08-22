"use strict";

/**
 * Worker-thread entry point for background cleanup and seed tasks.
 *
 * The worker receives task options through workerData, sends a restart
 * request when configured, and serializes any task failure back to its parent.
 */

import { parentPort, workerData } from "node:worker_threads";
import { runTasks } from "./task.js";

/** Execute the assigned task and report its terminal state to the parent. */
(() => {
  runTasks(workerData)
    .then(() => {
      if (workerData.restart !== "false") {
        parentPort.postMessage({
          action: "restartServer",
        });
      }
    })
    .catch((error) => {
      parentPort.postMessage({
        error,
      });
    });
})();
