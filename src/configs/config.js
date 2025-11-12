"use strict";

import { readFileSync } from "node:fs";
import {
  createFileWithLock,
  getJSONSchema,
  validateJSON,
  printLog,
} from "../utils/index.js";

let config;
let seed;
let cleanUp;

/* Load config.json */
if (config === undefined) {
  const type = "config";

  try {
    config = readConfigFile(type, true);

    config.repo = {
      styles: {},
      geojsons: {},
      datas: {},
      fonts: {},
      sprites: {},
    };
  } catch (error) {
    printLog("error", `Failed to load ${type}.json file: ${error}`);

    config = {};
  }
}

/* Load seed.json */
if (seed === undefined) {
  const type = "seed";

  try {
    seed = readConfigFile(type, true);
  } catch (error) {
    printLog("error", `Failed to load ${type}.json file: ${error}`);

    seed = {};
  }
}

/* Load cleanup.json */
if (cleanUp === undefined) {
  const type = "cleanup";

  try {
    cleanUp = readConfigFile(type, true);
  } catch (error) {
    printLog("error", `Failed to load ${type}.json file: ${error}`);

    cleanUp = {};
  }
}

/**
 * Read config file content
 * @param {"config"|"seed"|"cleanup"} type Config type
 * @param {boolean} isParse Parse JSON?
 * @returns {object}
 */
function readConfigFile(type, isParse) {
  const data = readFileSync(
    `${process.env.DATA_DIR || "data"}/${type}.json`,
    "utf8",
  );

  if (isParse) {
    return JSON.parse(data);
  } else {
    return data;
  }
}

/**
 * Update config file content with lock
 * @param {"config"|"seed"|"cleanup"} type Config type
 * @param {object} configObj Config object
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<void>}
 */
async function updateConfigFile(type, configObj, timeout) {
  await createFileWithLock(
    `${process.env.DATA_DIR || "data"}/${type}.json`,
    JSON.stringify(configObj, null, 2),
    timeout,
  );
}

/**
 * Validate config file content
 * @param {"config"|"seed"|"cleanup"} type Config type
 * @param {object} configObj Config object
 * @returns {Promise<void>}
 */
async function validateConfig(type, configObj) {
  if (!configObj) {
    if (type === "seed") {
      configObj = seed;
    } else if (type === "cleanup") {
      configObj = cleanUp;
    } else {
      configObj = config;
    }
  }

  validateJSON(await getJSONSchema(type), configObj);
}

export {
  updateConfigFile,
  readConfigFile,
  validateConfig,
  cleanUp,
  config,
  seed,
};
