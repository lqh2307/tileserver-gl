"use strict";

import { config } from "../configs/index.js";
import { readFile } from "node:fs/promises";
import {
  removeFileWithLock,
  createFileWithLock,
  calculateMD5OfFile,
  getDataFromURL,
  getFileCreated,
  getFileSize,
  printLog,
} from "../utils/index.js";

/*********************************** GeoJSON *************************************/

/**
 * Remove GeoJSON data file with lock
 * @param {string} filePath GeoJSON file path to remove
 * @returns {Promise<void>}
 */
export async function removeGeoJSONFile(filePath) {
  await removeFileWithLock(
    filePath,
    30000, // 30 seconds
  );
}

/**
 * Store GeoJSON file
 * @param {string} filePath GeoJSON file path to store
 * @param {Buffer} data Tile data buffer
 * @returns {Promise<void>}
 */
export async function storeGeoJSONFile(filePath, data) {
  await createFileWithLock(
    filePath,
    data,
    300000, // 5 mins
  );
}

/**
 * Get GeoJSON buffer
 * @param {string} filePath GeoJSON file path to get
 * @returns {Promise<Buffer>}
 */
export async function getGeoJSON(filePath) {
  try {
    return await readFile(filePath);
  } catch (error) {
    if (error.code === "ENOENT") {
      throw new Error("Not Found");
    }

    throw error;
  }
}

/**
 * Get created time of GeoJSON file
 * @param {string} filePath GeoJSON file path to get
 * @returns {Promise<number>}
 */
export async function getGeoJSONCreated(filePath) {
  return await getFileCreated(filePath);
}

/**
 * Get MD5 of GeoJSON
 * @param {string} filePath GeoJSON file path to get
 * @returns {Promise<string>}
 */
export async function getGeoJSONMD5(filePath) {
  return await calculateMD5OfFile(filePath);
}

/**
 * Get the size of GeoJSON file
 * @param {string} filePath GeoJSON file path to get
 * @returns {Promise<number>}
 */
export async function getGeoJSONSize(filePath) {
  return await getFileSize(filePath);
}

/**
 * Validate GeoJSON and get geometry types
 * @param {object|string} data GeoJSON or GeoJSON file path
 * @returns {Promise<string[]>} List of geometry types
 */
export async function validateAndGetGeometryTypes(data) {
  const geoJSON =
    typeof data === "object" ? data : JSON.parse(await readFile(data));

  const geometryTypes = new Set();

  const GEOMETRY_TYPES = new Set([
    "Polygon",
    "MultiPolygon",
    "LineString",
    "MultiLineString",
    "Point",
    "MultiPoint",
  ]);

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

        if (!feature.geometry) {
          continue;
        }

        if (feature.geometry.type === "GeometryCollection") {
          if (!Array.isArray(feature.geometry.geometries)) {
            throw new Error(`"geometries" property is invalid`);
          }

          for (const geometry of feature.geometry.geometries) {
            if (!GEOMETRY_TYPES.has(geometry.type)) {
              throw new Error(`"type" property is invalid`);
            }

            if (geometry.coordinates && !Array.isArray(geometry.coordinates)) {
              throw new Error(`"coordinates" property is invalid`);
            }

            addGeometryType(geometry.type);
          }
        } else if (GEOMETRY_TYPES.has(feature.geometry.type)) {
          if (
            feature.geometry.coordinates &&
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
      if (!geoJSON.geometry) {
        break;
      }

      if (geoJSON.geometry.type === "GeometryCollection") {
        if (!Array.isArray(geoJSON.geometry.geometries)) {
          throw new Error(`"geometries" property is invalid`);
        }

        for (const geometry of geoJSON.geometry.geometries) {
          if (!GEOMETRY_TYPES.has(geometry.type)) {
            throw new Error(`"type" property is invalid`);
          }

          if (geometry.coordinates && !Array.isArray(geometry.coordinates)) {
            throw new Error(`"coordinates" property is invalid`);
          }

          addGeometryType(geometry.type);
        }
      } else if (GEOMETRY_TYPES.has(geoJSON.geometry.type)) {
        if (
          geoJSON.geometry.coordinates &&
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
        if (!GEOMETRY_TYPES.has(geometry.type)) {
          throw new Error(`"type" property is invalid`);
        }

        if (geometry.coordinates && !Array.isArray(geometry.coordinates)) {
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
      if (geoJSON.coordinates && !Array.isArray(geoJSON.coordinates)) {
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

/**
 * Get and cache data GeoJSON
 * @param {string} id GeoJSON group id
 * @param {string} layer GeoJSON group layer
 * @returns {Promise<Buffer>}
 */
export async function getAndCacheDataGeoJSON(id, layer) {
  const item = config.geojsons[id]?.[layer];
  if (!item) {
    throw new Error(`GeoJSON id "${id}" - Layer "${layer}" does not exist`);
  }

  try {
    return await getGeoJSON(item.path);
  } catch (error) {
    if (item.sourceURL && error.message.includes("Not Found")) {
      printLog(
        "info",
        `Forwarding GeoJSON "${id}" - To "${item.sourceURL}"...`,
      );

      const geoJSON = await getDataFromURL(item.sourceURL, {
        method: "GET",
        responseType: "arraybuffer",
        timeout: 30000, // 30 seconds
        headers: item.headers,
        decompress: true,
      });

      if (item.storeCache) {
        printLog("info", `Caching GeoJSON "${id}" - File "${item.path}"...`);

        storeGeoJSONFile(item.path, geoJSON).catch((error) =>
          printLog(
            "error",
            `Failed to cache GeoJSON "${id}" - File "${item.path}": ${error}`,
          ),
        );
      }

      return geoJSON;
    }

    throw error;
  }
}
