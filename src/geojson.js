"use strict";

import { readFile, stat } from "node:fs/promises";
import { StatusCodes } from "http-status-codes";
import {
  removeFileWithLock,
  createFileWithLock,
  getDataFromURL,
  printLog,
  retry,
} from "./utils/index.js";

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
 * @param {object} headers Headers
 * @returns {Promise<void>}
 */
export async function downloadGeoJSONFile(
  url,
  filePath,
  maxTry,
  timeout,
  headers
) {
  await retry(async () => {
    try {
      // Get data from URL
      const response = await getDataFromURL(
        url,
        timeout,
        "arraybuffer",
        false,
        headers
      );

      // Store data to file
      await cacheGeoJSONFile(filePath, response.data);
    } catch (error) {
      if (error.statusCode) {
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
 * Get GeoJSON
 * @param {string} filePath
 * @returns {Promise<Buffer>}
 */
export async function getGeoJSON(filePath) {
  try {
    return await readFile(filePath);
  } catch (error) {
    if (error.code === "ENOENT") {
      throw new Error("JSON does not exist");
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
      if (!Array.isArray(geoJSON.features)) {
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
          if (!Array.isArray(feature.geometry.geometries)) {
            throw new Error(`"geometries" property is invalid`);
          }

          for (const geometry of feature.geometry.geometries) {
            if (
              ![
                "Polygon",
                "MultiPolygon",
                "LineString",
                "MultiLineString",
                "Point",
                "MultiPoint",
              ].includes(geometry.type)
            ) {
              throw new Error(`"type" property is invalid`);
            }

            if (
              geometry.coordinates !== null &&
              !Array.isArray(geometry.coordinates)
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
          ].includes(feature.geometry.type)
        ) {
          if (
            feature.geometry.coordinates !== null &&
            !Array.isArray(feature.geometry.coordinates)
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
        if (!Array.isArray(geoJSON.geometry.geometries)) {
          throw new Error(`"geometries" property is invalid`);
        }

        for (const geometry of geoJSON.geometry.geometries) {
          if (
            ![
              "Polygon",
              "MultiPolygon",
              "LineString",
              "MultiLineString",
              "Point",
              "MultiPoint",
            ].includes(geometry.type)
          ) {
            throw new Error(`"type" property is invalid`);
          }

          if (
            geometry.coordinates !== null &&
            !Array.isArray(geometry.coordinates)
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
        ].includes(geoJSON.geometry.type)
      ) {
        if (
          geoJSON.geometry.coordinates !== null &&
          !Array.isArray(geoJSON.geometry.coordinates)
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
      if (!Array.isArray(geoJSON.geometries)) {
        throw new Error(`"geometries" property is invalid`);
      }

      for (const geometry of geoJSON.geometries) {
        if (
          ![
            "Polygon",
            "MultiPolygon",
            "LineString",
            "MultiLineString",
            "Point",
            "MultiPoint",
          ].includes(geometry.type)
        ) {
          throw new Error(`"type" property is invalid`);
        }

        if (
          geometry.coordinates !== null &&
          !Array.isArray(geometry.coordinates)
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
      if (geoJSON.coordinates !== null && !Array.isArray(geoJSON.coordinates)) {
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
