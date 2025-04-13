"use strict";

import { parentPort, workerData } from "node:worker_threads";
import { runTasks } from "./task.js";

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
        error: error,
      });
    });
})();
