"use strict";

import { readFile } from "node:fs/promises";
import { readFileSync } from "node:fs";
import {
  createFileWithLock,
  getJSONSchema,
  validateJSON,
  printLog,
} from "../utils/index.js";

let cleanUp;

/* Load cleanup.json */
if (cleanUp === undefined) {
  try {
    cleanUp = JSON.parse(
      readFileSync(`${process.env.DATA_DIR || "data"}/cleanup.json`, "utf8")
    );
  } catch (error) {
    printLog("error", `Failed to load cleanup.json file: ${error}`);

    cleanUp = {};
  }
}

/**
 * Validate cleanup.json file
 * @returns {Promise<void>}
 */
async function validateCleanUpFile() {
  validateJSON(await getJSONSchema("cleanup"), cleanUp);
}

/**
 * Read cleanup.json file
 * @param {boolean} isParse Parse JSON?
 * @returns {Promise<object>}
 */
async function readCleanUpFile(isParse) {
  const data = await readFile(`${process.env.DATA_DIR}/cleanup.json`, "utf8");

  if (isParse) {
    return JSON.parse(data);
  } else {
    return data;
  }
}

/**
 * Update cleanup.json file content with lock
 * @param {object} cleanUp Cleanup object
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<void>}
 */
async function updateCleanUpFile(cleanUp, timeout) {
  await createFileWithLock(
    `${process.env.DATA_DIR}/cleanup.json`,
    JSON.stringify(cleanUp, null, 2),
    timeout
  );
}

export {
  validateCleanUpFile,
  updateCleanUpFile,
  readCleanUpFile,
  cleanUp,
};
