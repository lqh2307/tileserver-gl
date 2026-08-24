"use strict";

import { MAX_LON, MAX_LAT } from "./spatial.js";
import { resolveProjectPath } from "./path.js";
import { readFile } from "node:fs/promises";
import { isSameNumber } from "./number.js";
import Ajv from "ajv";

const ajv = new Ajv({
  allErrors: true,
});
const schemaCache = new Map();
const validatorCache = new WeakMap();

/**
 * Validate tileJSON
 * @param {{ [key: string]: any }} schema JSON schema
 * @param {{ [key: string]: any }} jsonData JSON data
 * @returns {void}
 */
export function validateJSON(schema, jsonData) {
  let validate = validatorCache.get(schema);
  if (!validate) {
    validate = ajv.compile(schema);

    validatorCache.set(schema, validate);
  }

  if (!validate(jsonData)) {
    throw new Error(
      validate.errors
        .map((error) => {
          return `\n\tPath ${error.instancePath || "/"}: ${error.keyword} - ${error.message} - ${JSON.stringify(error.params)}`;
        })
        .join(""),
    );
  }
}

/**
 * Get JSON schema
 * @param {"delete"|"cleanup"|"config"|"seed"|"style_render"|"render_svg"|"render_pdf"|"render_stylejson"|"data_export"|"export_all"|"render_high_quality_pdf"|"coverages"|"tile_bounds"|"sprite"|"add_frame"} schema
 * @returns {Promise<object>}
 */
export function getJSONSchema(schema) {
  let schemaPromise = schemaCache.get(schema);
  if (!schemaPromise) {
    schemaPromise = readFile(
      resolveProjectPath("public", "schemas", `${schema}.json`),
      "utf8",
    ).then(JSON.parse);

    schemaCache.set(schema, schemaPromise);

    schemaPromise.catch(() => {
      schemaCache.delete(schema);
    });
  }

  return schemaPromise;
}

/**
 * Validate a finite number with optional integer and bounds checks.
 * @param {any} value Value to validate
 * @param {boolean} checkInteger If true, requires an integer
 * @param {number} min Minimum value (inclusive)
 * @param {number} max Maximum value (inclusive)
 * @returns {boolean} True if valid
 */
export function isValidNumber(value, checkInteger, min, max) {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return false;
  }

  if (checkInteger && !Number.isInteger(value)) {
    return false;
  }

  if (min !== undefined && value < min) {
    return false;
  }

  if (max !== undefined && value > max) {
    return false;
  }

  return true;
}

/**
 * Validate a longitude value.
 * @param {any} lon Longitude value to validate
 * @param {boolean} isWGS84 If true, enforces WGS84 bounds (-180 to 180)
 * @returns {boolean} True if valid
 */
export function isValidLongitude(lon, isWGS84) {
  return isWGS84
    ? isValidNumber(lon, false, -MAX_LON, MAX_LON)
    : isValidNumber(lon, false);
}

/**
 * Validate a latitude value.
 * @param {number} lat Latitude value to validate
 * @param {boolean} isWGS84 If true, enforces WGS84 bounds (-90 to 90)
 * @returns {boolean} True if valid
 */
export function isValidLatitude(lat, isWGS84) {
  return isWGS84
    ? isValidNumber(lat, false, -MAX_LAT, MAX_LAT)
    : isValidNumber(lat, false);
}

/**
 * Validate an extent array [minLon, maxLat, maxLon, minLat].
 * @param {number[]} extent Extent array
 * @param {boolean} isWGS84 If true, enforces WGS84 bounds
 * @returns {boolean} True if valid
 */
export function isValidExtent(extent, isWGS84) {
  if (extent?.length !== 4) {
    return false;
  }

  if (
    !isValidLongitude(extent[0], isWGS84) ||
    !isValidLongitude(extent[2], isWGS84)
  ) {
    return false;
  }

  if (
    !isValidLatitude(extent[1], isWGS84) ||
    !isValidLatitude(extent[3], isWGS84)
  ) {
    return false;
  }

  if (extent[0] >= extent[2] || extent[1] <= extent[3]) {
    return false;
  }

  return true;
}

/**
 * Validate a bbox array [minLon, minLat, maxLon, maxLat].
 * @param {number[]} bbox BBox array
 * @param {boolean} isWGS84 If true, enforces WGS84 bounds
 * @returns {boolean} True if valid
 */
export function isValidBBox(bbox, isWGS84) {
  if (bbox?.length !== 4) {
    return false;
  }

  if (
    !isValidLongitude(bbox[0], isWGS84) ||
    !isValidLongitude(bbox[2], isWGS84)
  ) {
    return false;
  }

  if (
    !isValidLatitude(bbox[1], isWGS84) ||
    !isValidLatitude(bbox[3], isWGS84)
  ) {
    return false;
  }

  if (bbox[0] >= bbox[2] || bbox[1] >= bbox[3]) {
    return false;
  }

  return true;
}

/**
 * Check whether a BBox is exactly the zero box `[0, 0, 0, 0]`.
 * Note: This does not validate ordering or coordinate ranges.
 *
 * @param {number[]} value - BBox array.
 * @returns {boolean} True if the bbox is a 4-tuple and all values equal 0.
 */
export function isEmptyBBox(value) {
  if (value?.length !== 4) {
    return false;
  }

  return value[0] === 0 && value[1] === 0 && value[2] === 0 && value[3] === 0;
}

/**
 * Validate a map zoom level (0-25).
 * @param {any} zoom Zoom value
 * @returns {boolean} True if valid
 */
export function isValidZoom(zoom) {
  return isValidNumber(zoom, false, 0, 25);
}

/**
 * Check if two BBoxes are the same within a tolerance.
 * @param {number[]} bbox1 First BBox array
 * @param {number[]} bbox2 Second BBox array
 * @param {number} tolerance Tolerance for comparison
 * @returns {boolean} True if the BBoxes are the same within the tolerance
 */
export function isSameBBox(bbox1, bbox2, tolerance) {
  if (bbox1?.length !== 4 || bbox2?.length !== 4) {
    return false;
  }

  return !bbox1.some((value, index) => {
    return !isSameNumber(value, bbox2[index], tolerance);
  });
}
