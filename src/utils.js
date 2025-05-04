"use strict";

import { StatusCodes } from "http-status-codes";
import { createReadStream } from "node:fs";
import { printLog } from "./logger.js";
import { exec } from "child_process";
import handlebars from "handlebars";
import https from "node:https";
import http from "node:http";
import path from "node:path";
import crypto from "crypto";
import axios from "axios";
import sharp from "sharp";
import zlib from "zlib";
import util from "util";
import Ajv from "ajv";
import {
  writeFile,
  readFile,
  readdir,
  rename,
  mkdir,
  stat,
  open,
  rm,
} from "node:fs/promises";

sharp.cache(false);

/**
 * Compile template
 * @param {"index"|"viewer"|"vector_data"|"raster_data"|"geojson_group"|"geojson"|"wmts"} template
 * @param {object} data
 * @returns {Promise<string>}
 */
export async function compileTemplate(template, data) {
  return handlebars.compile(
    await readFile(`public/templates/${template}.tmpl`, "utf8")
  )(data);
}

/**
 * Get data from URL
 * @param {string} url URL to fetch data from
 * @param {number} timeout Timeout in milliseconds
 * @param {"arraybuffer"|"json"|"text"|"stream"|"blob"|"document"|"formdata"} responseType Response type
 * @param {boolean} keepAlive Whether to keep the connection alive
 * @returns {Promise<axios.AxiosResponse>}
 */
export async function getDataFromURL(
  url,
  timeout,
  responseType,
  keepAlive = false
) {
  try {
    return await axios({
      method: "GET",
      url: url,
      timeout: timeout,
      responseType: responseType,
      headers: {
        "User-Agent": "Tile Server",
      },
      validateStatus: (status) => {
        return status === StatusCodes.OK;
      },
      httpAgent: new http.Agent({
        keepAlive: keepAlive,
      }),
      httpsAgent: new https.Agent({
        keepAlive: keepAlive,
      }),
    });
  } catch (error) {
    if (error.response) {
      error.message = `Status code: ${error.response.status} - ${error.response.statusText}`;
      error.statusCode = error.response.status;
    }

    throw error;
  }
}

/**
 * Get data tile from a URL
 * @param {string} url The URL to fetch data tile from
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<object>}
 */
export async function getDataTileFromURL(url, timeout) {
  try {
    const response = await getDataFromURL(url, timeout, "arraybuffer");

    return {
      data: response.data,
      headers: detectFormatAndHeaders(response.data).headers,
    };
  } catch (error) {
    if (error.statusCode !== undefined) {
      if (
        error.statusCode === StatusCodes.NO_CONTENT ||
        error.statusCode === StatusCodes.NOT_FOUND
      ) {
        throw new Error("Tile does not exist");
      } else {
        throw new Error(`Failed to get data tile from "${url}": ${error}`);
      }
    } else {
      throw new Error(`Failed to get data tile from "${url}": ${error}`);
    }
  }
}

/**
 * Get JSON from a URL
 * @param {string} url The URL to fetch data from
 * @param {number} timeout Timeout in milliseconds
 * @param {boolean} isParse Parse JSON?
 * @returns {Promise<object|Buffer>}
 */
export async function getJSONFromURL(url, timeout, isParse) {
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
        throw new Error("JSON does not exist");
      } else {
        throw new Error(`Failed to get JSON from "${url}": ${error}`);
      }
    } else {
      throw new Error(`Failed to get JSON from "${url}": ${error}`);
    }
  }
}

/**
 * Post data to URL
 * @param {string} url URL to post data
 * @param {number} timeout Timeout in milliseconds
 * @param {object} body Body
 * @param {"arraybuffer"|"json"|"text"|"stream"|"blob"|"document"|"formdata"} responseType Response type
 * @param {boolean} keepAlive Whether to keep the connection alive
 * @returns {Promise<axios.AxiosResponse>}
 */
export async function postDataToURL(
  url,
  timeout,
  body,
  responseType,
  keepAlive = false
) {
  try {
    return await axios({
      method: "POST",
      url: url,
      timeout: timeout,
      responseType: responseType,
      headers: {
        "User-Agent": "Tile Server",
        "Content-Type": "application/json",
      },
      data: body,
      validateStatus: (status) => {
        return status === StatusCodes.OK;
      },
      httpAgent: new http.Agent({
        keepAlive: keepAlive,
      }),
      httpsAgent: new https.Agent({
        keepAlive: keepAlive,
      }),
    });
  } catch (error) {
    if (error.response) {
      error.message = `Status code: ${error.response.status} - ${error.response.statusText}`;
      error.statusCode = error.response.status;
    }

    throw error;
  }
}

/**
 * Check tile URL is local?
 * @param {string} url URL tile to check
 * @returns {boolean}
 */
export function isLocalTileURL(url) {
  if (typeof url !== "string") {
    return false;
  }

  return (
    url.startsWith("mbtiles://") === true ||
    url.startsWith("pmtiles://") === true ||
    url.startsWith("xyz://") === true ||
    url.startsWith("pg://") === true ||
    url.startsWith("geojson://") === true
  );
}

/**
 * Convert coordinates from EPSG:4326 (lon, lat) to EPSG:3857 (x, y in meters)
 * @param {number} lon Longitude in degrees
 * @param {number} lat Latitude in degrees
 * @returns {[number, number]} Web Mercator x, y in meters
 */
export function lonLat4326ToXY3857(lon, lat) {
  // Limit longitude
  if (lon > 180) {
    lon = 180;
  } else if (lon < -180) {
    lon = -180;
  }

  // Limit latitude
  if (lat > 85.051129) {
    lat = 85.051129;
  } else if (lat < -85.051129) {
    lat = -85.051129;
  }

  return [
    ((lon * Math.PI) / 180) * 6378137.0,
    Math.log(Math.tan(Math.PI / 4 + (lat * Math.PI) / 360)) * 6378137.0,
  ];
}

/**
 * Convert coordinates from EPSG:3857 (x, y in meters) to EPSG:4326 (lon, lat in degrees)
 * @param {number} x X in meters (Web Mercator)
 * @param {number} y Y in meters (Web Mercator)
 * @returns {[number, number]} Longitude and latitude in degrees
 */
export function xy3857ToLonLat4326(x, y) {
  let lon = (x / 6378137.0) * (180 / Math.PI);
  let lat = Math.atan(Math.sinh(y / 6378137.0)) * (180 / Math.PI);

  // Limit longitude
  if (lon > 180) {
    lon = 180;
  } else if (lon < -180) {
    lon = -180;
  }

  // Limit latitude
  if (lat > 85.051129) {
    lat = 85.051129;
  } else if (lat < -85.051129) {
    lat = -85.051129;
  }

  return [lon, lat];
}

/**
 * Get xyz tile indices from longitude, latitude, and zoom level (tile size = 256)
 * @param {number} lon Longitude in EPSG:4326
 * @param {number} lat Latitude in EPSG:4326
 * @param {number} z Zoom level
 * @param {"xyz"|"tms"} scheme Tile scheme
 * @returns {[number, number, number]} Tile indices [x, y, z]
 */
export function getXYZFromLonLatZ(lon, lat, z, scheme = "xyz") {
  const size = 256 * (1 << z);
  const bc = size / 360;
  const cc = size / (2 * Math.PI);
  const zc = size / 2;
  const maxTileIndex = (1 << z) - 1;

  // Limit longitude
  if (lon > 180) {
    lon = 180;
  } else if (lon < -180) {
    lon = -180;
  }

  // Limit latitude
  if (lat > 85.051129) {
    lat = 85.051129;
  } else if (lat < -85.051129) {
    lat = -85.051129;
  }

  let x = Math.floor((zc + lon * bc) / 256);
  let y = Math.floor(
    (scheme === "tms"
      ? size -
        (zc - cc * Math.log(Math.tan(Math.PI / 4 + (lat * Math.PI) / 360)))
      : zc - cc * Math.log(Math.tan(Math.PI / 4 + (lat * Math.PI) / 360))) / 256
  );

  // Limit x
  if (x < 0) {
    x = 0;
  } else if (x > maxTileIndex) {
    x = maxTileIndex;
  }

  // Limit y
  if (y < 0) {
    y = 0;
  } else if (y > maxTileIndex) {
    y = maxTileIndex;
  }

  return [x, y, z];
}

/**
 * Get longitude, latitude from tile indices x, y, and zoom level (tile size = 256)
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @param {number} z Zoom level
 * @param {"center"|"topLeft"|"bottomRight"} position Tile position: "center", "topLeft", or "bottomRight"
 * @param {"xyz"|"tms"} scheme Tile scheme
 * @returns {[number, number]} [longitude, latitude] in EPSG:4326
 */
export function getLonLatFromXYZ(
  x,
  y,
  z,
  position = "topLeft",
  scheme = "xyz"
) {
  const size = 256 * (1 << z);
  const bc = size / 360;
  const cc = size / (2 * Math.PI);
  const zc = size / 2;

  let px = x * 256;
  let py = y * 256;

  if (position === "center") {
    px = (x + 0.5) * 256;
    py = (y + 0.5) * 256;
  } else if (position === "bottomRight") {
    px = (x + 1) * 256;
    py = (y + 1) * 256;
  }

  return [
    (px - zc) / bc,
    (360 / Math.PI) *
      (Math.atan(Math.exp((zc - (scheme === "tms" ? size - py : py)) / cc)) -
        Math.PI / 4),
  ];
}

/**
 * Get grids for specific coverage with optional lat/lon steps (Keeps both head and tail residuals)
 * @param {{ zoom: number, bbox: [number, number, number, number] }} coverage
 * @param {number} lonStep Step for longitude
 * @param {number} latStep Step for latitude
 * @returns {Array<{ zoom: number, bbox: [number, number, number, number] }>}
 */
export function getGridsFromCoverage(coverage, lonStep = 1, latStep = 1) {
  const grids = [];

  function splitStep(start, end, step) {
    const ranges = [];

    let cur = Math.ceil(start / step) * step;

    if (cur > end) {
      return [[start, end]];
    }

    if (start < cur) {
      ranges.push([start, cur]);
    }

    while (cur + step <= end) {
      ranges.push([cur, cur + step]);

      cur += step;
    }

    if (cur < end) {
      ranges.push([cur, end]);
    }

    return ranges;
  }

  const lonRanges =
    typeof lonStep === "number"
      ? splitStep(coverage.bbox[0], coverage.bbox[2], lonStep)
      : [[coverage.bbox[0], coverage.bbox[2]]];
  const latRanges =
    typeof latStep === "number"
      ? splitStep(coverage.bbox[1], coverage.bbox[3], latStep)
      : [[coverage.bbox[1], coverage.bbox[3]]];

  for (const [lonStart, lonEnd] of lonRanges) {
    for (const [latStart, latEnd] of latRanges) {
      grids.push({
        bbox: [lonStart, latStart, lonEnd, latEnd],
        zoom: coverage.zoom,
      });
    }
  }

  return grids;
}

/**
 * Get tile bounds and total count for specific coverages
 * @param {{ zoom: number, bbox: [number, number, number, number]}[]} coverages Specific coverages
 * @param {"xyz"|"tms"} scheme Tile scheme
 * @returns {{ { z: number, x: [number, number], y: [number, number] }[] }}
 */
export function getTileBoundsFromCoverages(coverages, scheme) {
  let totalTile = 0;

  const tileBounds = coverages.map((coverage) => {
    let [xMin, yMin] = getXYZFromLonLatZ(
      coverage.bbox[0],
      coverage.bbox[3],
      coverage.zoom,
      scheme
    );
    let [xMax, yMax] = getXYZFromLonLatZ(
      coverage.bbox[2],
      coverage.bbox[1],
      coverage.zoom,
      scheme
    );

    if (yMin > yMax) {
      [yMin, yMax] = [yMax, yMin];
    }

    const tileBound = {
      total: (xMax - xMin + 1) * (yMax - yMin + 1),
      z: coverage.zoom,
      x: [xMin, xMax],
      y: [yMin, yMax],
    };

    totalTile += tileBound.total;

    return tileBound;
  });

  return {
    total: totalTile,
    tileBounds: tileBounds,
  };
}

/**
 * Get minzoom, maxzoom, bbox for specific coverages
 * @param {{ zoom: number, bbox: [number, number, number, number]}[]} coverages Specific coverages
 * @returns {{ minZoom: number, maxZoom: number, bbox: [number, number, number, number] }}
 */
export function getZoomsAndBBoxFromCoverages(coverages) {
  let minZoom = coverages[0].zoom;
  let maxZoom = coverages[0].zoom;
  let [minX, minY, maxX, maxY] = coverages[0].bbox;

  for (let i = 1; i < coverages.length; i++) {
    const { zoom, bbox } = coverages[i];
    const [xMin, yMin, xMax, yMax] = bbox;

    if (zoom < minZoom) {
      minZoom = zoom;
    }
    if (zoom > maxZoom) {
      maxZoom = zoom;
    }

    if (xMin < minX) {
      minX = xMin;
    }
    if (yMin < minY) {
      minY = yMin;
    }
    if (xMax > maxX) {
      maxX = xMax;
    }
    if (yMax > maxY) {
      maxY = yMax;
    }
  }

  return {
    minZoom,
    maxZoom,
    bbox: [minX, minY, maxX, maxY],
  };
}

/**
 * Get minzoom, maxzoom, bbox for specific coverages
 * @param {{ zoom: number, bbox: [number, number, number, number]}[], circle: { radius: number, center: [number, number] }} coverages Specific coverages
 * @param {[number, number, number, number]} limitedBBox Limited bounding box
 * @returns {{ minZoom: number, maxZoom: number, bbox: [number, number, number, number] }}
 */
export function processCoverages(coverages, limitedBBox) {
  return coverages.map((coverage) => {
    const bbox =
      coverage.circle !== undefined
        ? getBBoxFromCircle(coverage.circle.center, coverage.circle.radius)
        : deepClone(coverage.bbox);

    if (limitedBBox !== undefined) {
      if (bbox[0] < limitedBBox[0]) {
        bbox[0] = limitedBBox[0];
      }

      if (bbox[1] < limitedBBox[1]) {
        bbox[1] = limitedBBox[1];
      }

      if (bbox[2] > limitedBBox[2]) {
        bbox[2] = limitedBBox[2];
      }

      if (bbox[3] > limitedBBox[3]) {
        bbox[3] = limitedBBox[3];
      }
    }

    return {
      zoom: coverage.zoom,
      bbox: bbox,
    };
  });
}

/**
 * Convert tile indices to a bounding box that intersects the outer tiles
 * @param {number} xMin Minimum x tile index
 * @param {number} yMin Minimum y tile index
 * @param {number} xMax Maximum x tile index
 * @param {number} yMax Maximum y tile index
 * @param {number} z Zoom level
 * @param {"xyz"|"tms"} scheme Tile scheme
 * @returns {[number, number, number, number]} Bounding box [lonMin, latMin, lonMax, latMax] in EPSG:4326
 */
export function getBBoxFromTiles(xMin, yMin, xMax, yMax, z, scheme = "xyz") {
  const [lonMin, latMax] = getLonLatFromXYZ(xMin, yMin, z, "topLeft", scheme);
  const [lonMax, latMin] = getLonLatFromXYZ(
    xMax,
    yMax,
    z,
    "bottomRight",
    z,
    scheme
  );

  return [lonMin, latMin, lonMax, latMax];
}

/**
 * Get bounding box from center and radius
 * @param {[number, number]} center [lon, lat] of center (EPSG:4326)
 * @param {number} radius Radius in metter (EPSG:3857)
 * @returns {[number, number, number, number]} [minLon, minLat, maxLon, maxLat]
 */
export function getBBoxFromCircle(center, radius) {
  const [xCenter, yCenter] = lonLat4326ToXY3857(
    "EPSG:4326",
    "EPSG:3857",
    center
  );

  return [
    ...xy3857ToLonLat4326(xCenter - radius, yCenter - radius),
    ...xy3857ToLonLat4326(xCenter + radius, yCenter + radius),
  ];
}

/**
 * Get bounding box from an array of points
 * @param {[number, number][]} points Array of points in the format [lon, lat]
 * @returns {[number, number, number, number]} Bounding box in the format [minLon, minLat, maxLon, maxLat]
 */
export function getBBoxFromPoint(points) {
  let bbox = [-180, -85.051129, 180, 85.051129];

  if (points.length > 0) {
    bbox = [points[0][0], points[0][1], points[0][0], points[0][1]];

    for (let index = 1; index < points.length; index++) {
      if (points[index][0] < bbox[0]) {
        bbox[0] = points[index][0];
      }

      if (points[index][1] < bbox[1]) {
        bbox[1] = points[index][1];
      }

      if (points[index][0] > bbox[2]) {
        bbox[2] = points[index][0];
      }

      if (points[index][1] > bbox[3]) {
        bbox[3] = points[index][1];
      }
    }

    if (bbox[0] > 180) {
      bbox[0] = 180;
    } else if (bbox[0] < -180) {
      bbox[0] = -180;
    }

    if (bbox[1] > 180) {
      bbox[1] = 180;
    } else if (bbox[1] < -180) {
      bbox[1] = -180;
    }

    if (bbox[2] > 85.051129) {
      bbox[2] = 85.051129;
    } else if (bbox[2] < -85.051129) {
      bbox[2] = -85.051129;
    }

    if (bbox[3] > 85.051129) {
      bbox[3] = 85.051129;
    } else if (bbox[3] < -85.051129) {
      bbox[3] = -85.051129;
    }
  }

  return bbox;
}

/**
 * Get XYZ tile from bounding box for specific zoom levels intersecting a bounding box
 * @param {[number, number, number, number]} bbox [west, south, east, north] in EPSG:4326
 * @param {number[]} zooms Array of specific zoom levels
 * @returns {string[]} Array values as z/x/y
 */
export function getXYZTileFromBBox(bbox, zooms) {
  const tiles = [];

  for (const zoom of zooms) {
    const [xMin, yMin] = getXYZFromLonLatZ(bbox[0], bbox[3], zoom, "xyz");
    const [xMax, yMax] = getXYZFromLonLatZ(bbox[2], bbox[1], zoom, "xyz");

    for (let x = xMin; x <= xMax; x++) {
      for (let y = yMin; y <= yMax; y++) {
        tiles.push(`/${zoom}/${x}/${y}`);
      }
    }
  }

  return tiles;
}

/**
 * Delay function to wait for a specified time
 * @param {number} ms Time to wait in milliseconds
 * @returns {Promise<void>}
 */
export async function delay(ms) {
  if (ms >= 0) {
    await new Promise((resolve) => setTimeout(resolve, ms));
  }
}

/**
 * Calculate MD5 hash of a buffer
 * @param {Buffer} buffer The data buffer
 * @returns {string} The MD5 hash
 */
export function calculateMD5(buffer) {
  return crypto.createHash("md5").update(buffer).digest("hex");
}

/**
 * Calculate MD5 hash of a file
 * @param {string} filePath The data file path
 * @returns {Promise<string>} The MD5 hash
 */
export async function calculateMD5OfFile(filePath) {
  try {
    return new Promise((resolve, reject) => {
      const hash = crypto.createHash("md5");

      createReadStream(filePath)
        .on("error", (error) => reject(error))
        .on("data", (chunk) => hash.update(chunk))
        .on("end", () => resolve(hash.digest("hex")));
    });
  } catch (error) {
    if (error.code === "ENOENT") {
      throw new Error("File does not exist");
    } else {
      throw error;
    }
  }
}

/**
 * Attempt do function multiple times
 * @param {function} fn The function to attempt
 * @param {number} maxTry Number of retry attempts on failure
 * @param {number} after Delay in milliseconds between each retry
 * @returns {Promise<void>}
 */
export async function retry(fn, maxTry, after = 0) {
  for (let attempt = 1; attempt <= maxTry; attempt++) {
    try {
      return await fn();
    } catch (error) {
      const remainingAttempts = maxTry - attempt;
      if (remainingAttempts > 0) {
        printLog(
          "warn",
          `${error}. ${remainingAttempts} try remaining - After ${after} ms...`
        );

        await delay(after);
      } else {
        throw error;
      }
    }
  }
}

/**
 * Recursively removes empty folders in a directory
 * @param {string} folderPath The root directory to check for empty folders
 * @param {RegExp} regex The regex to match files
 * @returns {Promise<void>}
 */
export async function removeEmptyFolders(folderPath, regex) {
  const entries = await readdir(folderPath, {
    withFileTypes: true,
  });

  let hasMatchingFile = false;

  await Promise.all(
    entries.map(async (entry) => {
      const fullPath = `${folderPath}/${entry.name}`;

      if (
        entry.isFile() === true &&
        (regex === undefined || regex.test(entry.name) === true)
      ) {
        hasMatchingFile = true;
      } else if (entry.isDirectory() === true) {
        await removeEmptyFolders(fullPath, regex);

        const subEntries = await readdir(fullPath).catch(() => []);
        if (subEntries.length > 0) {
          hasMatchingFile = true;
        }
      }
    })
  );

  if (hasMatchingFile === false) {
    await rm(folderPath, {
      recursive: true,
      force: true,
    });
  }
}

/**
 * Recursively removes old cache locks
 * @returns {Promise<void>}
 */
export async function removeOldCacheLocks() {
  let fileNames = await findFiles(
    `${process.env.DATA_DIR}/caches`,
    /^.*\.(lock|tmp)$/,
    true,
    true
  );

  await Promise.all(
    fileNames.map((fileName) =>
      rm(fileName, {
        force: true,
      })
    )
  );
}

/**
 * Check folder is exist?
 * @param {string} dirPath Directory path
 * @returns {Promise<boolean>}
 */
export async function isExistFolder(dirPath) {
  try {
    const stats = await stat(dirPath);

    return stats.isDirectory();
  } catch (error) {
    if (error.code === "ENOENT") {
      return false;
    } else {
      throw error;
    }
  }
}

/**
 * Check file is exist?
 * @param {string} filePath File path
 * @returns {Promise<boolean>}
 */
export async function isExistFile(filePath) {
  try {
    const stats = await stat(filePath);

    return stats.isFile();
  } catch (error) {
    if (error.code === "ENOENT") {
      return false;
    } else {
      throw error;
    }
  }
}

/**
 * Find matching files in a directory
 * @param {string} dirPath The directory path to search
 * @param {RegExp} regex The regex to match files
 * @param {boolean} recurse Whether to search recursively in subdirectories
 * @param {boolean} includeDirPath Whether to include directory path
 * @returns {Promise<string[]>} Array of filepaths matching the regex
 */
export async function findFiles(
  dirPath,
  regex,
  recurse = false,
  includeDirPath = false
) {
  const entries = await readdir(dirPath, {
    withFileTypes: true,
  });

  const results = [];

  for (const entry of entries) {
    if (entry.isDirectory() === true) {
      if (recurse === true) {
        const fileNames = await findFiles(
          `${dirPath}/${entry.name}`,
          regex,
          recurse,
          includeDirPath
        );

        fileNames.forEach((fileName) => {
          if (includeDirPath === true) {
            results.push(`${dirPath}/${entry.name}/${fileName}`);
          } else {
            results.push(`${entry.name}/${fileName}`);
          }
        });
      }
    } else if (regex.test(entry.name) === true) {
      if (includeDirPath === true) {
        results.push(`${dirPath}/${entry.name}`);
      } else {
        results.push(entry.name);
      }
    }
  }

  return results;
}

/**
 * Find matching folders in a directory
 * @param {string} dirPath The directory path to search
 * @param {RegExp} regex The regex to match folders
 * @param {boolean} recurse Whether to search recursively in subdirectories
 * @param {boolean} includeDirPath Whether to include directory path
 * @returns {Promise<string[]>} Array of folder paths matching the regex
 */
export async function findFolders(
  dirPath,
  regex,
  recurse = false,
  includeDirPath = false
) {
  const entries = await readdir(dirPath, {
    withFileTypes: true,
  });

  const results = [];

  for (const entry of entries) {
    if (entry.isDirectory() === true) {
      if (regex.test(entry.name) === true) {
        if (includeDirPath === true) {
          results.push(`${dirPath}/${entry.name}`);
        } else {
          results.push(entry.name);
        }
      }

      if (recurse === true) {
        const directoryNames = await findFolders(
          `${dirPath}/${entry.name}`,
          regex,
          recurse,
          includeDirPath
        );

        directoryNames.forEach((directoryName) => {
          if (includeDirPath === true) {
            results.push(`${dirPath}/${entry.name}/${directoryName}`);
          } else {
            results.push(`${entry.name}/${directoryName}`);
          }
        });
      }
    }
  }

  return results;
}

/**
 * Remove files or folders
 * @param {string[]} fileOrFolders File or folder paths
 * @returns {Promise<void>}
 */
export async function removeFilesOrFolders(fileOrFolders) {
  await Promise.all(
    fileOrFolders.map((fileOrFolder) =>
      rm(fileOrFolder, {
        force: true,
        recursive: true,
      })
    )
  );
}

/**
 * Get request host
 * @param {Request} req Request object
 * @returns {string}
 */
export function getRequestHost(req) {
  const protocol = req.headers["x-forwarded-proto"] || req.protocol;
  const host = req.headers["x-forwarded-host"] || req.headers["host"];
  const prefix = req.headers["x-forwarded-prefix"] || "";

  return `${protocol}://${host}${prefix}`;
}

/**
 * Return either a format as an extension: png, pbf, jpg, webp, gif and
 * headers - Content-Type and Content-Encoding - for a response containing this kind of image
 * @param {Buffer} buffer Input data
 * @returns {object}
 */
export function detectFormatAndHeaders(buffer) {
  let format;
  const headers = {};

  if (
    buffer[0] === 0x89 &&
    buffer[1] === 0x50 &&
    buffer[2] === 0x4e &&
    buffer[3] === 0x47 &&
    buffer[4] === 0x0d &&
    buffer[5] === 0x0a &&
    buffer[6] === 0x1a &&
    buffer[7] === 0x0a
  ) {
    format = "png";
    headers["content-type"] = "image/png";
  } else if (
    buffer[0] === 0xff &&
    buffer[1] === 0xd8 &&
    buffer[buffer.length - 2] === 0xff &&
    buffer[buffer.length - 1] === 0xd9
  ) {
    format = "jpeg"; // equivalent jpg
    headers["content-type"] = "image/jpeg"; // equivalent image/jpg
  } else if (
    buffer[0] === 0x47 &&
    buffer[1] === 0x49 &&
    buffer[2] === 0x46 &&
    buffer[3] === 0x38 &&
    (buffer[4] === 0x39 || buffer[4] === 0x37) &&
    buffer[5] === 0x61
  ) {
    format = "gif";
    headers["content-type"] = "image/gif";
  } else if (
    buffer[0] === 0x52 &&
    buffer[1] === 0x49 &&
    buffer[2] === 0x46 &&
    buffer[3] === 0x46 &&
    buffer[8] === 0x57 &&
    buffer[9] === 0x45 &&
    buffer[10] === 0x42 &&
    buffer[11] === 0x50
  ) {
    format = "webp";
    headers["content-type"] = "image/webp";
  } else {
    format = "pbf";
    headers["content-type"] = "application/x-protobuf";

    if (buffer[0] === 0x78 && buffer[1] === 0x9c) {
      headers["content-encoding"] = "deflate";
    } else if (buffer[0] === 0x1f && buffer[1] === 0x8b) {
      headers["content-encoding"] = "gzip";
    }
  }

  return {
    format,
    headers,
  };
}

/**
 * Compress data using gzip algorithm asynchronously
 * @param {Buffer|string} input The data to compress
 * @param {zlib.ZlibOptions} options Optional zlib compression options
 * @returns {Promise<Buffer>} A Promise that resolves to the compressed data as a Buffer
 */
export const gzipAsync = util.promisify(zlib.gzip);

/**
 * Decompress gzip-compressed data asynchronously
 * @param {Buffer|string} input The compressed data to decompress
 * @param {zlib.ZlibOptions} options Optional zlib decompression options
 * @returns {Promise<Buffer>} A Promise that resolves to the decompressed data as a Buffer
 */
export const unzipAsync = util.promisify(zlib.unzip);

/**
 * Decompress deflate-compressed data asynchronously
 * @param {Buffer|string} input The compressed data to decompress
 * @param {zlib.ZlibOptions} options Optional zlib decompression options
 * @returns {Promise<Buffer>} A Promise that resolves to the decompressed data as a Buffer
 */
export const inflateAsync = util.promisify(zlib.inflate);

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

    if (validate(jsonData) === false) {
      throw validate.errors
        .map((error) => `\n\t${error.instancePath} ${error.message}`)
        .join();
    }
  } catch (error) {
    throw error;
  }
}

/**
 * Deep clone an object using JSON serialization
 * @param {object} obj The object to clone
 * @returns {object} The deep-cloned object
 */
export function deepClone(obj) {
  if (obj !== undefined) {
    return JSON.parse(JSON.stringify(obj));
  }
}

/**
 * Get version of server
 * @returns {Promise<string>}
 */
export async function getVersion() {
  return JSON.parse(await readFile("package.json", "utf8")).version;
}

/**
 * Get JSON schema
 * @param {"delete"|"cleanup"|"config"|"seed"|"style_render"|"data_export"|"coverages"|"sprite"} schema
 * @returns {Promise<object>}
 */
export async function getJSONSchema(schema) {
  return JSON.parse(await readFile(`public/schemas/${schema}.json`, "utf8"));
}

/**
 * Create random RGB
 * @param {number} r Red
 * @param {number} g Green
 * @param {number} b Blue
 * @param {number} a Alpha
 * @returns {string}
 */
export function createRandomRGBA(r, g, b, a) {
  const red = r || Math.floor(Math.random() * 256);
  const greed = g || Math.floor(Math.random() * 256);
  const blue = b || Math.floor(Math.random() * 256);
  const alpha = a || Math.random();

  return `rgba(${red}, ${greed}, ${blue}, ${alpha})`;
}

/**
 * Run an external command and wait for it to finish
 * @param {string} command The command to run
 * @returns {Promise<string>} The command's stdout
 */
export async function runCommand(command) {
  return new Promise((resolve, reject) => {
    exec(command, (error, stdout, stderr) => {
      if (error) {
        reject(stderr);
      } else {
        resolve(stdout);
      }
    });
  });
}

/**
 * Get PNG image metadata
 * @param {string} filePath File path to store file
 * @returns {Promise<object>}
 */
export async function getPNGImageMetadata(filePath) {
  return await sharp(filePath).metadata();
}

/**
 * Check if PNG image file/buffer is full transparent (alpha = 0)
 * @param {Buffer} buffer Buffer of the PNG image
 * @returns {Promise<boolean>}
 */
export async function isFullTransparentPNGImage(buffer) {
  try {
    if (
      buffer[0] !== 0x89 ||
      buffer[1] !== 0x50 ||
      buffer[2] !== 0x4e ||
      buffer[3] !== 0x47 ||
      buffer[4] !== 0x0d ||
      buffer[5] !== 0x0a ||
      buffer[6] !== 0x1a ||
      buffer[7] !== 0x0a
    ) {
      return false;
    }

    const { data, info } = await sharp(buffer).raw().toBuffer({
      resolveWithObject: true,
    });

    if (info.channels !== 4) {
      return false;
    }

    for (let i = 3; i < data.length; i += 4) {
      if (data[i] !== 0) {
        return false;
      }
    }

    return true;
  } catch (error) {
    return false;
  }
}

/**
 * Render image tile data
 * @param {Buffer} data Image data
 * @param {number} originSize Image origin size
 * @param {number} targetSize Image target size
 * @param {"jpeg"|"jpg"|"png"|"webp"|"gif"} format Tile format
 * @returns {Promise<Buffer>}
 */
export async function renderImageTileData(
  data,
  originSize,
  targetSize,
  format
) {
  const image = sharp(data, {
    raw: {
      premultiplied: true,
      width: originSize,
      height: originSize,
      channels: 4,
    },
  });

  if (targetSize !== undefined) {
    image.resize({
      width: targetSize,
      height: targetSize,
    });
  }

  switch (format) {
    case "gif": {
      image.gif({});

      break;
    }

    case "png": {
      image.png({
        compressionLevel: 9,
      });

      break;
    }

    case "jpg":
    case "jpeg": {
      image.jpeg({
        quality: 100,
      });

      break;
    }

    case "webp": {
      image.webp({
        quality: 100,
      });

      break;
    }
  }

  return await image.toBuffer();
}

/**
 * Create fallback tile data
 * @param {"jpeg"|"jpg"|"png"|"webp"|"gif"|"pbf"} format Tile format
 * @returns {Buffer}
 */
export function createFallbackTileData(format) {
  switch (format) {
    case "gif": {
      return Buffer.from([
        0x47, 0x49, 0x46, 0x38, 0x39, 0x61, 0x01, 0x00, 0x01, 0x00, 0x80, 0x00,
        0x00, 0x4c, 0x69, 0x71, 0x00, 0x00, 0x00, 0x21, 0xff, 0x0b, 0x4e, 0x45,
        0x54, 0x53, 0x43, 0x41, 0x50, 0x45, 0x32, 0x2e, 0x30, 0x03, 0x01, 0x00,
        0x00, 0x00, 0x21, 0xf9, 0x04, 0x05, 0x00, 0x00, 0x00, 0x00, 0x2c, 0x00,
        0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x02, 0x02, 0x44, 0x01,
        0x00, 0x3b,
      ]);
    }

    case "png": {
      return Buffer.from([
        0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d,
        0x49, 0x48, 0x44, 0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
        0x08, 0x06, 0x00, 0x00, 0x00, 0x1f, 0x15, 0xc4, 0x89, 0x00, 0x00, 0x00,
        0x09, 0x70, 0x48, 0x59, 0x73, 0x00, 0x00, 0x03, 0xe8, 0x00, 0x00, 0x03,
        0xe8, 0x01, 0xb5, 0x7b, 0x52, 0x6b, 0x00, 0x00, 0x00, 0x0d, 0x49, 0x44,
        0x41, 0x54, 0x78, 0x9c, 0x63, 0x60, 0x60, 0x60, 0x60, 0x00, 0x00, 0x00,
        0x05, 0x00, 0x01, 0xa5, 0xf6, 0x45, 0x40, 0x00, 0x00, 0x00, 0x00, 0x49,
        0x45, 0x4e, 0x44, 0xae, 0x42, 0x60, 0x82,
      ]);
    }

    case "jpg":
    case "jpeg": {
      return Buffer.from([
        0xff, 0xd8, 0xff, 0xdb, 0x00, 0x43, 0x00, 0x06, 0x04, 0x05, 0x06, 0x05,
        0x04, 0x06, 0x06, 0x05, 0x06, 0x07, 0x07, 0x06, 0x08, 0x0a, 0x10, 0x0a,
        0x0a, 0x09, 0x09, 0x0a, 0x14, 0x0e, 0x0f, 0x0c, 0x10, 0x17, 0x14, 0x18,
        0x18, 0x17, 0x14, 0x16, 0x16, 0x1a, 0x1d, 0x25, 0x1f, 0x1a, 0x1b, 0x23,
        0x1c, 0x16, 0x16, 0x20, 0x2c, 0x20, 0x23, 0x26, 0x27, 0x29, 0x2a, 0x29,
        0x19, 0x1f, 0x2d, 0x30, 0x2d, 0x28, 0x30, 0x25, 0x28, 0x29, 0x28, 0xff,
        0xdb, 0x00, 0x43, 0x01, 0x07, 0x07, 0x07, 0x0a, 0x08, 0x0a, 0x13, 0x0a,
        0x0a, 0x13, 0x28, 0x1a, 0x16, 0x1a, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28,
        0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28,
        0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28,
        0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28,
        0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0xff, 0xc0, 0x00, 0x11,
        0x08, 0x00, 0x01, 0x00, 0x01, 0x03, 0x01, 0x22, 0x00, 0x02, 0x11, 0x01,
        0x03, 0x11, 0x01, 0xff, 0xc4, 0x00, 0x15, 0x00, 0x01, 0x01, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x08, 0xff, 0xc4, 0x00, 0x14, 0x10, 0x01, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0xff, 0xc4, 0x00, 0x14, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xc4,
        0x00, 0x14, 0x11, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xda, 0x00, 0x0c,
        0x03, 0x01, 0x00, 0x02, 0x11, 0x03, 0x11, 0x00, 0x3f, 0x00, 0x95, 0x00,
        0x07, 0xff, 0xd9,
      ]);
    }

    case "webp": {
      return Buffer.from([
        0x52, 0x49, 0x46, 0x46, 0x40, 0x00, 0x00, 0x00, 0x57, 0x45, 0x42, 0x50,
        0x56, 0x50, 0x38, 0x58, 0x0a, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x41, 0x4c, 0x50, 0x48, 0x02, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x56, 0x50, 0x38, 0x20, 0x18, 0x00, 0x00, 0x00,
        0x30, 0x01, 0x00, 0x9d, 0x01, 0x2a, 0x01, 0x00, 0x01, 0x00, 0x01, 0x40,
        0x26, 0x25, 0xa4, 0x00, 0x03, 0x70, 0x00, 0xfe, 0xfd, 0x36, 0x68, 0x00,
      ]);
    }

    default: {
      return Buffer.from([]);
    }
  }
}

/**
 * Create file with lock
 * @param {string} filePath File path to store file
 * @param {Buffer} data Data buffer
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<void>}
 */
export async function createFileWithLock(filePath, data, timeout) {
  const startTime = Date.now();

  const lockFilePath = `${filePath}.lock`;
  let lockFileHandle;

  while (Date.now() - startTime <= timeout) {
    try {
      lockFileHandle = await open(lockFilePath, "wx");

      const tempFilePath = `${filePath}.tmp`;

      try {
        await mkdir(path.dirname(filePath), {
          recursive: true,
        });

        await writeFile(tempFilePath, data);

        await rename(tempFilePath, filePath);
      } catch (error) {
        await rm(tempFilePath, {
          force: true,
        });

        throw error;
      }

      await lockFileHandle.close();

      await rm(lockFilePath, {
        force: true,
      });

      return;
    } catch (error) {
      if (error.code === "ENOENT") {
        await mkdir(path.dirname(filePath), {
          recursive: true,
        });

        continue;
      } else if (error.code === "EEXIST") {
        await delay(50);
      } else {
        if (lockFileHandle !== undefined) {
          await lockFileHandle.close();

          await rm(lockFilePath, {
            force: true,
          });
        }

        throw error;
      }
    }
  }

  throw new Error(`Timeout to access lock file`);
}

/**
 * Remove file with lock
 * @param {string} filePath File path to remove file
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<void>}
 */
export async function removeFileWithLock(filePath, timeout) {
  const startTime = Date.now();

  const lockFilePath = `${filePath}.lock`;
  let lockFileHandle;

  while (Date.now() - startTime <= timeout) {
    try {
      lockFileHandle = await open(lockFilePath, "wx");

      await rm(filePath, {
        force: true,
      });

      await lockFileHandle.close();

      await rm(lockFilePath, {
        force: true,
      });

      return;
    } catch (error) {
      if (error.code === "ENOENT") {
        return;
      } else if (error.code === "EEXIST") {
        await delay(50);
      } else {
        if (lockFileHandle !== undefined) {
          await lockFileHandle.close();

          await rm(lockFilePath, {
            force: true,
          });
        }

        throw error;
      }
    }
  }

  throw new Error(`Timeout to access lock file`);
}

/**
 * Create tile metadata from template
 * @param {object} metadata Metadata object
 * @returns {object}
 */
export function createTileMetadataFromTemplate(metadata) {
  const data = {};

  if (metadata.name !== undefined) {
    data.name = metadata.name;
  } else {
    data.name = "Unknown";
  }

  if (metadata.description !== undefined) {
    data.description = metadata.description;
  } else {
    data.description = "Unknown";
  }

  if (metadata.attribution !== undefined) {
    data.attribution = metadata.attribution;
  } else {
    data.attribution = "<b>Viettel HighTech</b>";
  }

  if (metadata.version !== undefined) {
    data.version = metadata.version;
  } else {
    data.version = "1.0.0";
  }

  if (metadata.type !== undefined) {
    data.type = metadata.type;
  } else {
    data.type = "overlay";
  }

  if (metadata.format !== undefined) {
    data.format = metadata.format;
  } else {
    data.format = "png";
  }

  if (metadata.minzoom !== undefined) {
    data.minzoom = metadata.minzoom;
  } else {
    data.minzoom = 0;
  }

  if (metadata.maxzoom !== undefined) {
    data.maxzoom = metadata.maxzoom;
  } else {
    data.maxzoom = 22;
  }

  if (metadata.bounds !== undefined) {
    data.bounds = deepClone(metadata.bounds);
  } else {
    data.bounds = [-180, -85.051129, 180, 85.051129];
  }

  if (metadata.center !== undefined) {
    data.center = [
      (data.bounds[0] + data.bounds[2]) / 2,
      (data.bounds[1] + data.bounds[3]) / 2,
      Math.floor((data.minzoom + data.maxzoom) / 2),
    ];
  }

  if (data.format === "pbf") {
    if (metadata.vector_layers !== undefined) {
      data.vector_layers = deepClone(metadata.vector_layers);
    } else {
      data.vector_layers = [];
    }
  }

  if (metadata.cacheCoverages !== undefined) {
    data.cacheCoverages = deepClone(metadata.cacheCoverages);
  }

  return data;
}
