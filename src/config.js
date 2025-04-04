"use strict";

import { createFileWithLock, getJSONSchema, validateJSON } from "./utils.js";
import fsPromise from "node:fs/promises";

let config = {};

/**
 * Read config.json file
 * @param {boolean} isValidate Is validate file content?
 * @returns {Promise<Object>}
 */
async function readConfigFile(isValidate) {
  /* Read config.json file */
  const data = await fsPromise.readFile(
    `${process.env.DATA_DIR}/config.json`,
    "utf8"
  );

  Object.assign(config, JSON.parse(data));

  /* Validate config.json file */
  if (isValidate === true) {
    validateJSON(await getJSONSchema("config"), config);
  }

  return config;
}

/**
 * Load config.json file content to global variable
 * @returns {Promise<void>}
 */
async function loadConfigFile() {
  config = await readConfigFile(false);

  config.repo = {
    styles: {},
    geojsons: {},
    datas: {},
    fonts: {},
    sprites: {},
  };
}

/**
 * Update config.json file content with lock
 * @param {Object} config Config object
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

export { updateConfigFile, readConfigFile, loadConfigFile, config };
