"use strict";

/**
 * Worker-thread entry point for background cleanup and seed tasks.
 *
 * The worker receives one resource-level selector through workerData and
 * serializes any task failure back to its parent.
 */

import { parentPort, workerData } from "node:worker_threads";
import { runTasks } from "./task.js";

/** Execute the assigned task and report its terminal state to the parent. */
(() => {
  runTasks(workerData).catch((error) => {
    parentPort.postMessage({
      error: {
        message: error.message,
        stack: error.stack,
      },
    });
  });
})();
