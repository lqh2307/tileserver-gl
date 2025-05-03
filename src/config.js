"use strict";

import { createFileWithLock, getJSONSchema, validateJSON } from "./utils.js";
import { readFile } from "node:fs/promises";
import { readFileSync } from "node:fs";

let config;

/* Load config.json */
if (config === undefined) {
  config = JSON.parse(
    readFileSync(`${process.env.DATA_DIR}/config.json`, "utf8")
  );

  config.repo = {
    styles: {},
    geojsons: {},
    datas: {},
    fonts: {},
    sprites: {},
  };
}

/**
 * Validate config.json file
 * @returns {Promise<void>}
 */
async function validateConfigFile() {
  validateJSON(await getJSONSchema("config"), config);
}

/**
 * Read config.json file
 * @returns {Promise<Object>}
 */
async function readConfigFile() {
  return await readFile(`${process.env.DATA_DIR}/config.json`, "utf8");
}

/**
 * Update config.json file content with lock
 * @param {object} config Config object
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<void>}
 */
async function updateConfigFile(config, timeout) {
  await createFileWithLock(
    `${process.env.DATA_DIR}/config.json`,
    JSON.stringify(config, null, 2),
    timeout
  );
}

export { validateConfigFile, updateConfigFile, readConfigFile, config };
