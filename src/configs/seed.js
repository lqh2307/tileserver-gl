"use strict";

import { readFile } from "node:fs/promises";
import { readFileSync } from "node:fs";
import {
  createFileWithLock,
  getJSONSchema,
  validateJSON,
  printLog,
} from "../utils/index.js";

let seed;

/* Load seed.json */
if (seed === undefined) {
  try {
    seed = JSON.parse(
      readFileSync(`${process.env.DATA_DIR || "data"}/seed.json`, "utf8")
    );
  } catch (error) {
    printLog("error", `Failed to load seed.json file: ${error}`);

    seed = {};
  }
}

/**
 * Validate seed.json file
 * @returns {Promise<void>}
 */
async function validateSeedFile() {
  validateJSON(await getJSONSchema("seed"), seed);
}

/**
 * Read seed.json file
 * @param {boolean} isParse Parse JSON?
 * @returns {Promise<object>}
 */
async function readSeedFile(isParse) {
  const data = await readFile(`${process.env.DATA_DIR}/seed.json`, "utf8");

  if (isParse) {
    return JSON.parse(data);
  } else {
    return data;
  }
}

/**
 * Update seed.json file content with lock
 * @param {object} seed Seed object
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<void>}
 */
async function updateSeedFile(seed, timeout) {
  await createFileWithLock(
    `${process.env.DATA_DIR}/seed.json`,
    JSON.stringify(seed, null, 2),
    timeout
  );
}

export {
  validateSeedFile,
  updateSeedFile,
  readSeedFile,
  seed,
};
