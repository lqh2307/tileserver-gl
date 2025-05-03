"use strict";

import { readFile, stat } from "node:fs/promises";
import { StatusCodes } from "http-status-codes";
import { printLog } from "./logger.js";
import {
  removeFileWithLock,
  createFileWithLock,
  getDataFromURL,
  retry,
} from "./utils.js";

/**
 * Remove GeoJSON data file with lock
 * @param {string} filePath File path to remove GeoJSON data file
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<void>}
 */
export async function removeGeoJSONFile(filePath, timeout) {
  await removeFileWithLock(filePath, timeout);
}

/**
 * Download GeoJSON file
 * @param {string} url The URL to download the file from
 * @param {string} filePath File path
 * @param {number} maxTry Number of retry attempts on failure
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<void>}
 */
export async function downloadGeoJSONFile(url, filePath, maxTry, timeout) {
  await retry(async () => {
    try {
      // Get data from URL
      const response = await getDataFromURL(url, timeout, "arraybuffer");

      // Store data to file
      await cacheGeoJSONFile(filePath, response.data);
    } catch (error) {
      if (error.statusCode !== undefined) {
        printLog(
          "error",
          `Failed to download GeoJSON file "${filePath}" - From "${url}": ${error}`
        );

        if (
          error.statusCode === StatusCodes.NO_CONTENT ||
          error.statusCode === StatusCodes.NOT_FOUND
        ) {
          return;
        } else {
          throw error;
        }
      } else {
        throw error;
      }
    }
  }, maxTry);
}

/**
 * Cache GeoJSON file
 * @param {string} filePath File path
 * @param {Buffer} data Tile data buffer
 * @returns {Promise<void>}
 */
export async function cacheGeoJSONFile(filePath, data) {
  await createFileWithLock(
    filePath,
    data,
    300000 // 5 mins
  );
}

/**
 * Get GeoJSON from a URL
 * @param {string} url The URL to fetch data from
 * @param {number} timeout Timeout in milliseconds
 * @param {boolean} isParse
 * @returns {Promise<object|Buffer>}
 */
export async function getGeoJSONFromURL(url, timeout, isParse) {
  try {
    const response = await getDataFromURL(
      url,
      timeout,
      isParse === true ? "json" : "arraybuffer"
    );

    return response.data;
  } catch (error) {
    if (error.statusCode !== undefined) {
      if (
        error.statusCode === StatusCodes.NO_CONTENT ||
        error.statusCode === StatusCodes.NOT_FOUND
      ) {
        throw new Error("GeoJSON does not exist");
      } else {
        throw new Error(`Failed to get GeoJSON from "${url}": ${error}`);
      }
    } else {
      throw new Error(`Failed to get GeoJSON from "${url}": ${error}`);
    }
  }
}

/**
 * Get GeoJSON
 * @param {string} filePath
 * @param {boolean} isParse
 * @returns {Promise<object|Buffer>}
 */
export async function getGeoJSON(filePath, isParse) {
  try {
    const data = await readFile(filePath);
    if (!data) {
      throw new Error("GeoJSON does not exist");
    }

    if (isParse === true) {
      return JSON.parse(data);
    } else {
      return data;
    }
  } catch (error) {
    if (error.code === "ENOENT") {
      throw new Error("GeoJSON does not exist");
    } else {
      throw error;
    }
  }
}

/**
 * Get created of GeoJSON
 * @param {string} filePath The path of the file
 * @returns {Promise<number>}
 */
export async function getGeoJSONCreated(filePath) {
  try {
    const stats = await stat(filePath);

    return stats.ctimeMs;
  } catch (error) {
    if (error.code === "ENOENT") {
      throw new Error("GeoJSON created does not exist");
    } else {
      throw error;
    }
  }
}

/**
 * Get the size of GeoJSON
 * @param {string} filePath The path of the file
 * @returns {Promise<number>}
 */
export async function getGeoJSONSize(filePath) {
  const stats = await stat(filePath);

  return stats.size;
}

/**
 * Validate GeoJSON and get geometry types
 * @param {object} geoJSON GeoJSON
 * @returns {string[]} List of geometry types
 */
export function validateAndGetGeometryTypes(geoJSON) {
  const geometryTypes = new Set();

  function addGeometryType(geometryType) {
    switch (geometryType) {
      case "Polygon":
      case "MultiPolygon": {
        geometryTypes.add("polygon");

        break;
      }

      case "LineString":
      case "MultiLineString": {
        geometryTypes.add("line");

        break;
      }

      case "Point":
      case "MultiPoint": {
        geometryTypes.add("circle");

        break;
      }
    }
  }

  switch (geoJSON.type) {
    case "FeatureCollection": {
      if (Array.isArray(geoJSON.features) === false) {
        throw new Error(`"features" property is invalid`);
      }

      for (const feature of geoJSON.features) {
        if (feature.type !== "Feature") {
          throw new Error(`"type" property is invalid`);
        }

        if (feature.geometry === null) {
          continue;
        }

        if (feature.geometry.type === "GeometryCollection") {
          if (Array.isArray(feature.geometry.geometries) === false) {
            throw new Error(`"geometries" property is invalid`);
          }

          for (const geometry of feature.geometry.geometries) {
            if (
              [
                "Polygon",
                "MultiPolygon",
                "LineString",
                "MultiLineString",
                "Point",
                "MultiPoint",
              ].includes(geometry.type) === false
            ) {
              throw new Error(`"type" property is invalid`);
            }

            if (
              geometry.coordinates !== null &&
              Array.isArray(geometry.coordinates) === false
            ) {
              throw new Error(`"coordinates" property is invalid`);
            }

            addGeometryType(geometry.type);
          }
        } else if (
          [
            "Polygon",
            "MultiPolygon",
            "LineString",
            "MultiLineString",
            "Point",
            "MultiPoint",
          ].includes(feature.geometry.type) === true
        ) {
          if (
            feature.geometry.coordinates !== null &&
            Array.isArray(feature.geometry.coordinates) === false
          ) {
            throw new Error(`"coordinates" property is invalid`);
          }

          addGeometryType(feature.geometry.type);
        } else {
          throw new Error(`"type" property is invalid`);
        }
      }

      break;
    }

    case "Feature": {
      if (geoJSON.geometry === null) {
        break;
      }

      if (geoJSON.geometry.type === "GeometryCollection") {
        if (Array.isArray(geoJSON.geometry.geometries) === false) {
          throw new Error(`"geometries" property is invalid`);
        }

        for (const geometry of geoJSON.geometry.geometries) {
          if (
            [
              "Polygon",
              "MultiPolygon",
              "LineString",
              "MultiLineString",
              "Point",
              "MultiPoint",
            ].includes(geometry.type) === false
          ) {
            throw new Error(`"type" property is invalid`);
          }

          if (
            geometry.coordinates !== null &&
            Array.isArray(geometry.coordinates) === false
          ) {
            throw new Error(`"coordinates" property is invalid`);
          }

          addGeometryType(geometry.type);
        }
      } else if (
        [
          "Polygon",
          "MultiPolygon",
          "LineString",
          "MultiLineString",
          "Point",
          "MultiPoint",
        ].includes(geoJSON.geometry.type) === true
      ) {
        if (
          geoJSON.geometry.coordinates !== null &&
          Array.isArray(geoJSON.geometry.coordinates) === false
        ) {
          throw new Error(`"coordinates" property is invalid`);
        }

        addGeometryType(geoJSON.geometry.type);
      } else {
        throw new Error(`"type" property is invalid`);
      }

      break;
    }

    case "GeometryCollection": {
      if (Array.isArray(geoJSON.geometries) === false) {
        throw new Error(`"geometries" property is invalid`);
      }

      for (const geometry of geoJSON.geometries) {
        if (
          [
            "Polygon",
            "MultiPolygon",
            "LineString",
            "MultiLineString",
            "Point",
            "MultiPoint",
          ].includes(geometry.type) === false
        ) {
          throw new Error(`"type" property is invalid`);
        }

        if (
          geometry.coordinates !== null &&
          Array.isArray(geometry.coordinates) === false
        ) {
          throw new Error(`"coordinates" property is invalid`);
        }

        addGeometryType(geometry.type);
      }

      break;
    }

    case "Polygon":
    case "MultiPolygon":
    case "LineString":
    case "MultiLineString":
    case "Point":
    case "MultiPoint": {
      if (
        geoJSON.coordinates !== null &&
        Array.isArray(geoJSON.coordinates) === false
      ) {
        throw new Error(`"coordinates" property is invalid`);
      }

      addGeometryType(geoJSON.type);

      break;
    }

    default: {
      throw new Error(`"type" property is invalid`);
    }
  }

  return Array.from(geometryTypes);
}
