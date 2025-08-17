"use strict";

import { readFile } from "node:fs/promises";
import Ajv from "ajv";

/**
 * Validate tileJSON
 * @param {object} schema JSON schema
 * @param {object} jsonData JSON data
 * @returns {void}
 */
export function validateJSON(schema, jsonData) {
  try {
    const validate = new Ajv({
      allErrors: true,
    }).compile(schema);

    if (!validate(jsonData)) {
      throw validate.errors
        .map((error) => `\n\t${error.instancePath} ${error.message}`)
        .join();
    }
  } catch (error) {
    throw error;
  }
}

/**
 * Get JSON schema
 * @param {"delete"|"cleanup"|"config"|"seed"|"style_render"|"data_export"|"coverages"|"sprite"} schema
 * @returns {Promise<object>}
 */
export async function getJSONSchema(schema) {
  return JSON.parse(await readFile(`public/schemas/${schema}.json`, "utf8"));
}
