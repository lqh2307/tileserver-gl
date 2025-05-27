"use strict";

import { createReadStream, createWriteStream } from "node:fs";
import { StatusCodes } from "http-status-codes";
import { printLog } from "./logger.js";
import { spawn } from "child_process";
import handlebars from "handlebars";
import archiver from "archiver";
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

// sharp.cache(false);

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
 * Get data file from a URL
 * @param {string} url The URL to fetch data from
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<Buffer>}
 */
export async function getDataFileFromURL(url, timeout) {
  try {
    const response = await getDataFromURL(url, timeout, "arraybuffer");

    return response.data;
  } catch (error) {
    if (error.statusCode !== undefined) {
      if (
        error.statusCode === StatusCodes.NO_CONTENT ||
        error.statusCode === StatusCodes.NOT_FOUND
      ) {
        throw new Error("File does not exist");
      } else {
        throw new Error(`Failed to get file from "${url}": ${error}`);
      }
    } else {
      throw new Error(`Failed to get file from "${url}": ${error}`);
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
 * Download file with stream
 * @param {string} url The URL to download the file from
 * @param {string} filePath The path where the file will be saved
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<void>}
 */
export async function downloadFileWithStream(url, filePath, timeout) {
  try {
    await mkdir(path.dirname(filePath), {
      recursive: true,
    });

    const response = await getDataFromURL(url, timeout, "stream");

    const tempFilePath = `${filePath}.tmp`;

    const writer = createWriteStream(tempFilePath);

    response.data.pipe(writer);

    return await new Promise((resolve, reject) => {
      writer
        .on("finish", async () => {
          await rename(tempFilePath, filePath);

          resolve();
        })
        .on("error", async (error) => {
          await rm(tempFilePath, {
            force: true,
          });

          reject(error);
        });
    });
  } catch (error) {
    if (error.statusCode !== undefined) {
      if (
        error.statusCode === StatusCodes.NO_CONTENT ||
        error.statusCode === StatusCodes.NOT_FOUND
      ) {
        throw new Error("File does not exist");
      } else {
        throw new Error(
          `Failed to download file "${filePath}" - From "${url}": ${error}`
        );
      }
    } else {
      throw new Error(
        `Failed to download file "${filePath}" - From "${url}": ${error}`
      );
    }
  }
}

/**
 * Create folders
 * @param {string[]} dirPaths Create folders
 * @returns {Promise<void>}
 */
export async function createFolders(dirPaths) {
  await Promise.all(
    dirPaths.map((dirPath) =>
      mkdir(dirPath, {
        recursive: true,
      })
    )
  );
}

/**
 * Check URL is local?
 * @param {string} url URL to check
 * @returns {boolean}
 */
export function isLocalURL(url) {
  if (typeof url !== "string") {
    return false;
  }

  return ["mbtiles://", "pmtiles://", "xyz://", "pg://", "geojson://"].some(
    (scheme) => url.startsWith(scheme) === true
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
 * Get xyz tile indices from longitude, latitude, and zoom level
 * @param {number} lon Longitude in EPSG:4326
 * @param {number} lat Latitude in EPSG:4326
 * @param {number} z Zoom level
 * @param {"xyz"|"tms"} scheme Tile scheme
 * @param {256|512} tileSize Tile size
 * @returns {[number, number, number]} Tile indices [x, y, z]
 */
export function getXYZFromLonLatZ(lon, lat, z, scheme = "xyz", tileSize = 256) {
  const size = tileSize * (1 << z);
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

  let x = Math.floor((zc + lon * bc) / tileSize);
  let y = Math.floor(
    (scheme === "tms"
      ? size -
        (zc - cc * Math.log(Math.tan(Math.PI / 4 + (lat * Math.PI) / 360)))
      : zc - cc * Math.log(Math.tan(Math.PI / 4 + (lat * Math.PI) / 360))) /
      tileSize
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
 * Get longitude, latitude from tile indices x, y, and zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @param {number} z Zoom level
 * @param {"center"|"topLeft"|"bottomRight"} position Tile position: "center", "topLeft", or "bottomRight"
 * @param {"xyz"|"tms"} scheme Tile scheme
 * @param {256|512} tileSize Tile size
 * @returns {[number, number]} [longitude, latitude] in EPSG:4326
 */
export function getLonLatFromXYZ(
  x,
  y,
  z,
  position = "topLeft",
  scheme = "xyz",
  tileSize = 256
) {
  const size = tileSize * (1 << z);
  const bc = size / 360;
  const cc = size / (2 * Math.PI);
  const zc = size / 2;

  let px = x * tileSize;
  let py = y * tileSize;

  if (position === "center") {
    px = (x + 0.5) * tileSize;
    py = (y + 0.5) * tileSize;
  } else if (position === "bottomRight") {
    px = (x + 1) * tileSize;
    py = (y + 1) * tileSize;
  }

  return [
    (px - zc) / bc,
    (360 / Math.PI) *
      (Math.atan(Math.exp((zc - (scheme === "tms" ? size - py : py)) / cc)) -
        Math.PI / 4),
  ];
}

/**
 * Calculate zoom levels
 * @param {[number, number, number, number]} bbox Bounding box in EPSG:4326
 * @param {number} width Width of image
 * @param {number} height Height of image
 * @param {256|512} tileSize Tile size
 * @returns {{minZoom: number, maxZoom: number}} Zoom levels
 */
export function calculateZoomLevels(bbox, width, height, tileSize = 256) {
  const [minX, minY] = lonLat4326ToXY3857(bbox[0], bbox[1]);
  const [maxX, maxY] = lonLat4326ToXY3857(bbox[2], bbox[3]);

  const resolution = (6378137.0 * (2 * Math.PI)) / tileSize;

  let maxZoom = Math.round(
    Math.log2(
      resolution / Math.min((maxX - minX) / width, (maxY - minY) / height)
    )
  );
  if (maxZoom > 25) {
    maxZoom = 25;
  }

  let minZoom = maxZoom;

  const targetTileSize = Math.floor(tileSize * 0.95);

  while (minZoom > 0 && (width > targetTileSize || height > targetTileSize)) {
    width /= 2;
    height /= 2;

    minZoom--;
  }

  return {
    minZoom,
    maxZoom,
  };
}

/**
 * Calculate sizes
 * @param {number} z Zoom level
 * @param {[number, number, number, number]} bbox Bounding box in EPSG:4326
 * @param {256|512} tileSize Tile size
 * @returns {{width: number, height: number}} Sizes
 */
export function calculateSizes(z, bbox, tileSize = 256) {
  const [minX, minY] = lonLat4326ToXY3857(bbox[0], bbox[1], tileSize);
  const [maxX, maxY] = lonLat4326ToXY3857(bbox[2], bbox[3], tileSize);

  const resolution = (6378137.0 * (2 * Math.PI)) / tileSize;

  return {
    width: Math.round(((1 << z) * (maxX - minX)) / resolution),
    height: Math.round(((1 << z) * (maxY - minY)) / resolution),
  };
}

/**
 * Get grids for specific coverage with optional lat/lon steps (Keeps both head and tail residuals)
 * @param {{ zoom: number, bbox: [number, number, number, number] }} coverage
 * @param {number} lonStep Step for longitude
 * @param {number} latStep Step for latitude
 * @returns {{ zoom: number, bbox: [number, number, number, number] }[]}
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
 * @param {256|512} tileSize Tile size
 * @returns {{ realBBox: [number, number, number, number], total: number, tileBounds: { z: number, x: [number, number], y: [number, number] }[] }}
 */
export function getTileBoundsFromCoverages(coverages, scheme, tileSize = 256) {
  let totalTile = 0;
  let realBBox;

  const tileBounds = coverages.map((coverage, idx) => {
    let [xMin, yMin] = getXYZFromLonLatZ(
      coverage.bbox[0],
      coverage.bbox[3],
      coverage.zoom,
      scheme,
      tileSize
    );
    let [xMax, yMax] = getXYZFromLonLatZ(
      coverage.bbox[2],
      coverage.bbox[1],
      coverage.zoom,
      scheme,
      tileSize
    );

    if (yMin > yMax) {
      [yMin, yMax] = [yMax, yMin];
    }

    const _bbox = getBBoxFromTiles(
      xMin,
      yMin,
      xMax,
      yMax,
      coverage.zoom,
      scheme,
      tileSize
    );

    if (idx === 0) {
      realBBox = _bbox;
    } else {
      if (realBBox[0] < _bbox[0]) {
        realBBox[0] = _bbox[0];
      }
      if (realBBox[1] < _bbox[1]) {
        realBBox[1] = _bbox[1];
      }
      if (realBBox[2] > _bbox[2]) {
        realBBox[2] = _bbox[2];
      }
      if (realBBox[3] > _bbox[3]) {
        realBBox[3] = _bbox[3];
      }
    }

    const tileBound = {
      realBBox: _bbox,
      total: (xMax - xMin + 1) * (yMax - yMin + 1),
      z: coverage.zoom,
      x: [xMin, xMax],
      y: [yMin, yMax],
    };

    totalTile += tileBound.total;

    return tileBound;
  });

  return {
    realBBox: realBBox,
    total: totalTile,
    tileBounds: tileBounds,
  };
}

/**
 * Get minzoom, maxzoom, bbox for specific coverages
 * @param {{ zoom: number, bbox: [number, number, number, number]}[], circle: { radius: number, center: [number, number] }} coverages Specific coverages
 * @param {[number, number, number, number]} limitedBBox Limited bounding box
 * @returns {{ zoom: number, bbox: [number, number, number, number] }}
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
 * Create coverages from bbox and zooms
 * @param {[number, number, number, number]} bbox Bounding box in EPSG:4326
 * @param {number} minZoom Minzoom
 * @param {number} maxZoom Minzoom
 * @returns {{ minZoom: number, maxZoom: number, bbox: [number, number, number, number] }}
 */
export function createCoveragesFromBBoxAndZooms(bbox, minZoom, maxZoom) {
  const coverages = [];

  for (let zoom = minZoom; zoom <= maxZoom; zoom++) {
    coverages.push({
      zoom: zoom,
      bbox: bbox,
    });
  }

  return coverages;
}

/**
 * Convert tile indices to a bounding box that intersects the outer tiles
 * @param {number} xMin Minimum x tile index
 * @param {number} yMin Minimum y tile index
 * @param {number} xMax Maximum x tile index
 * @param {number} yMax Maximum y tile index
 * @param {number} z Zoom level
 * @param {"xyz"|"tms"} scheme Tile scheme
 * @param {256|512} tileSize Tile size
 * @returns {[number, number, number, number]} Bounding box [lonMin, latMin, lonMax, latMax] in EPSG:4326
 */
export function getBBoxFromTiles(
  xMin,
  yMin,
  xMax,
  yMax,
  z,
  scheme = "xyz",
  tileSize = 256
) {
  const [lonMin, latMax] = getLonLatFromXYZ(
    xMin,
    yMin,
    z,
    "topLeft",
    scheme,
    tileSize
  );
  const [lonMax, latMin] = getLonLatFromXYZ(
    xMax,
    yMax,
    z,
    "bottomRight",
    scheme,
    tileSize
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
  const [xCenter, yCenter] = lonLat4326ToXY3857(center[0], center[1]);

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
 * Get bounding box intersect
 * @param {[number, number, number, number]} bbox1 Bounding box 1 in the format [minLon, minLat, maxLon, maxLat]
 * @param {[number, number, number, number]} bbox2 Bounding box 2 in the format [minLon, minLat, maxLon, maxLat]
 * @returns {[number, number, number, number]} Intersect bounding box in the format [minLon, minLat, maxLon, maxLat]
 */
export function getIntersectBBox(bbox1, bbox2) {
  const minLon = Math.max(bbox1[0], bbox2[0]);
  const minLat = Math.max(bbox1[1], bbox2[1]);
  const maxLon = Math.min(bbox1[2], bbox2[2]);
  const maxLat = Math.min(bbox1[3], bbox2[3]);

  if (minLon >= maxLon || minLat >= maxLat) {
    return;
  }

  return [minLon, minLat, maxLon, maxLat];
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
 * Delay 25ms
 * @returns {Promise<void>}
 */
export async function wait25ms() {
  await new Promise((resolve) => setTimeout(resolve, 25));
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
 * Get content-type from format
 * @param {string} format Data format
 * @returns {string}
 */
export function detectContentTypeFromFormat(format) {
  switch (format) {
    case "png": {
      return "image/png";
    }

    case "jpg":
    case "jpeg": {
      return "image/jpeg";
    }

    case "gif": {
      return "image/gif";
    }

    case "webp": {
      return "image/webp";
    }

    case "pbf": {
      return "application/x-protobuf";
    }

    case "xml": {
      return "text/xml";
    }

    case "json":
    case "geojson": {
      return "application/json";
    }

    default: {
      return "application/octet-stream";
    }
  }
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
 * Compress zip folder
 * @param {string} iDirPath The input dir path
 * @param {string} oFilePath The output file path
 * @returns {Promise<void>}
 */
export async function zipFolder(iDirPath, oFilePath) {
  await mkdir(path.dirname(oFilePath), {
    recursive: true,
  });

  return new Promise((resolve, reject) => {
    const output = createWriteStream(oFilePath);
    const archive = archiver("zip", {
      zlib: {
        level: 9,
      },
    });

    output.on("close", () => resolve());

    archive.on("error", (error) => reject(error));

    archive.pipe(output);

    archive.directory(iDirPath, false);

    archive.finalize();
  });
}

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
 * Run an external command and optionally stream output via callback
 * @param {string} command The command to run
 * @param {number} interval Interval in milliseconds to call callback
 * @param {(partialOutput: string) => void} callback Function to call every interval with output so far
 * @returns {Promise<string>} The command's full stdout
 */
export async function runCommand(command, interval, callback) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, {
      shell: true,
    });

    let stdout = "";
    let stderr = "";

    child.stdout.on("data", (data) => {
      stdout += data.toString();
    });

    child.stderr.on("data", (data) => {
      stderr += data.toString();
    });

    const intervalID =
      interval > 0 && callback !== undefined
        ? setInterval(() => callback(stdout), interval)
        : undefined;

    child.on("close", (code) => {
      if (intervalID !== undefined) {
        clearInterval(intervalID);

        callback(stdout);
      }

      if (code === 0) {
        resolve(stdout);
      } else {
        reject(stderr || `Process exited with code: ${code}`);
      }
    });

    child.on("error", (err) => {
      if (intervalID !== undefined) {
        clearInterval(intervalID);
      }

      reject(err.message);
    });
  });
}

/**
 * Get PNG image metadata
 * @param {string} filePath File path to store file
 * @returns {Promise<object>}
 */
export async function getPNGImageMetadata(filePath) {
  return await sharp(filePath, {
    limitInputPixels: false,
  }).metadata();
}

/**
 * Format degree
 * @param {number} deg Degree
 * @param {boolean} isLat Is lat?
 * @param {"D"|"DMS"|"DMSD"} format Format
 * @returns {string}
 */
export function formatDegree(deg, isLat, format) {
  const rounded = Number(deg.toFixed(9));

  if (format === "DMS" || format === "DMSD") {
    const abs = Math.abs(rounded);
    let d = Math.floor(abs);
    let m = Math.floor((abs - d) * 60);
    let s = Math.round(((abs - d) * 60 - m) * 60);

    if (s === 60) {
      s = 0;
      m += 1;
    }

    if (m === 60) {
      m = 0;
      d += 1;
    }

    let direction = "";
    let sign = "";
    if (format === "DMSD") {
      if (isLat === true) {
        direction = rounded >= 0 ? "N" : "S";
      } else {
        direction = rounded >= 0 ? "E" : "W";
      }
    } else if (rounded < 0) {
      sign = "-";
    }

    const sFormat = s === 0 ? "" : `${s}"`;
    const mFormat = m === 0 ? "" : `${m}'`;

    return `${sign}${d}°${sFormat}${mFormat}${direction}`;
  } else {
    if (Number.isInteger(rounded) === true) {
      return Math.round(rounded).toString();
    }

    return rounded.toFixed(2);
  }
}

/**
 * Add frame to image
 * @param {{ filePath: string, bbox: [number, number, number, number] }} input Input object
 * @param {[number, number, number, number]} bbox Bounding box in EPSG:4326
 * @param {object} options Frame options object
 * @param {{ format: string, filePath: string, width: number, height: number }} output Output object
 * @returns {Promise<string>}
 */
export async function addFrameToImage(input, options, output) {
  const {
    padding = 10,

    frameInnerColor = "black",
    frameInnerWidth = 6,
    frameInnerStyle = "solid",

    frameOuterColor = "black",
    frameOuterWidth = 6,
    frameOuterStyle = "solid",

    frameSpace = 120,

    tickLabelFormat = "DMSD",

    tickMajorTickStep = 0.5,
    tickMinorTickStep = 0.05,

    tickMajorTickWidth = 6,
    tickMinorTickWidth = 4,

    tickMajorTickSize = 20,
    tickMinorTickSize = 15,

    tickMajorLabelSize = 18,
    tickMinorLabelSize = 0,

    tickMajorColor = "black",
    tickMinorColor = "black",

    tickMajorLabelColor = "black",
    tickMinorLabelColor = "black",

    tickMajorLabelFont = "sans-serif",
    tickMinorLabelFont = "sans-serif",

    xTickLabelOffset = 5,
    yTickLabelOffset = 5,

    xTickMajorLabelRotation = 0,
    xTickMinorLabelRotation = 0,

    yTickMajorLabelRotation = 0,
    yTickMinorLabelRotation = 0,
  } = options;

  const image = sharp(input.filePath, {
    limitInputPixels: false,
  });

  const { width, height } = await image.metadata();
  const bbox = input.bbox;

  let innerStrokeDasharray = "";
  if (frameInnerStyle === "solid") {
    innerStrokeDasharray = "";
  } else if (frameInnerStyle === "dashed") {
    innerStrokeDasharray = `stroke-dasharray="5,5"`;
  } else if (frameInnerStyle === "longDashed") {
    innerStrokeDasharray = `stroke-dasharray="10,5"`;
  } else if (frameInnerStyle === "dotted") {
    innerStrokeDasharray = `stroke-dasharray="1,3"`;
  } else if (frameInnerStyle === "dashDot") {
    innerStrokeDasharray = `stroke-dasharray="5,3,1,3"`;
  }

  let outerStrokeDasharray = " ";
  if (frameOuterStyle === "solid") {
    outerStrokeDasharray = " ";
  } else if (frameOuterStyle === "dashed ") {
    outerStrokeDasharray = `stroke-dasharray="5,5" `;
  } else if (frameOuterStyle === "longDashed") {
    outerStrokeDasharray = `stroke-dasharray="10,5" `;
  } else if (frameOuterStyle === "dotted") {
    outerStrokeDasharray = `stroke-dasharray="1,3" `;
  } else if (frameOuterStyle === "dashDot") {
    outerStrokeDasharray = `stroke-dasharray="5,3,1,3" `;
  }

  const degPerPixelX = (bbox[2] - bbox[0]) / width;
  const degPerPixelY = (bbox[3] - bbox[1]) / height;

  const esp = 1e-9;
  const svgElements = [];

  // Inner frame
  svgElements.push(
    `<rect x="${padding + frameSpace}" y="${
      padding + frameSpace
    }" width="${width}" height="${height}" fill="none" stroke="${frameInnerColor}" stroke-width="${frameInnerWidth}" ${innerStrokeDasharray}/>`
  );

  // Outer frame
  svgElements.push(
    `<rect x="${padding}" y="${padding}" width="${
      width + frameSpace * 2
    }" height="${
      height + frameSpace * 2
    }" fill="none" stroke="${frameOuterColor}" stroke-width="${frameOuterWidth}" ${outerStrokeDasharray}/>`
  );

  // X-axis major ticks & labels
  let xMajorStart = Math.round(bbox[0] / tickMajorTickStep) * tickMajorTickStep;
  for (let lon = xMajorStart; lon <= bbox[2] + esp; lon += tickMajorTickStep) {
    const x = (lon - bbox[0]) / degPerPixelX;

    // Top tick
    svgElements.push(
      `<line x1="${padding + x + frameSpace}" y1="${
        padding + frameSpace
      }" x2="${padding + x + frameSpace}" y2="${
        padding + frameSpace - tickMajorTickSize
      }" stroke="${tickMajorColor}" stroke-width="${tickMajorTickWidth}" />`
    );

    // Bottom tick
    svgElements.push(
      `<line x1="${padding + x + frameSpace}" y1="${
        padding + height + frameSpace
      }" x2="${padding + x + frameSpace}" y2="${
        padding + height + frameSpace + tickMajorTickSize
      }" stroke="${tickMajorColor}" stroke-width="${tickMajorTickWidth}" />`
    );

    if (tickMajorLabelSize > 0) {
      const label = formatDegree(
        bbox[0] + x * degPerPixelX,
        false,
        tickLabelFormat
      );

      // Top label
      svgElements.push(
        `<text x="${padding + x + frameSpace}" y="${
          padding + frameSpace - tickMajorTickSize - xTickLabelOffset
        }" font-size="${tickMajorLabelSize}" font-family="${tickMajorLabelFont}" fill="${tickMajorLabelColor}" text-anchor="middle" transform="rotate(${xTickMajorLabelRotation},${
          padding + x + frameSpace
        },${
          padding + frameSpace - tickMajorTickSize - xTickLabelOffset
        })">${label}</text>`
      );

      // Bottom label
      svgElements.push(
        `<text x="${padding + x + frameSpace}" y="${
          padding + height + frameSpace + tickMajorTickSize + xTickLabelOffset
        }" font-size="${tickMajorLabelSize}" font-family="${tickMajorLabelFont}" fill="${tickMajorLabelColor}" text-anchor="middle" dominant-baseline="text-before-edge" transform="rotate(${xTickMajorLabelRotation},${
          padding + x + frameSpace
        },${
          padding + height + frameSpace + tickMajorTickSize + xTickLabelOffset
        })">${label}</text>`
      );
    }
  }

  // X-axis minor & labels
  let xMinorStart = Math.round(bbox[0] / tickMinorTickStep) * tickMinorTickStep;
  for (let lon = xMinorStart; lon <= bbox[2] + esp; lon += tickMinorTickStep) {
    const x = (lon - bbox[0]) / degPerPixelX;

    // Top tick
    svgElements.push(
      `<line x1="${padding + x + frameSpace}" y1="${
        padding + frameSpace
      }" x2="${padding + x + frameSpace}" y2="${
        padding + frameSpace - tickMinorTickSize
      }" stroke="${tickMinorColor}" stroke-width="${tickMinorTickWidth}" />`
    );

    // Bottom tick
    svgElements.push(
      `<line x1="${padding + x + frameSpace}" y1="${
        padding + height + frameSpace
      }" x2="${padding + x + frameSpace}" y2="${
        padding + height + frameSpace + tickMinorTickSize
      }" stroke="${tickMinorColor}" stroke-width="${tickMinorTickWidth}" />`
    );

    if (tickMinorLabelSize > 0) {
      const label = formatDegree(
        bbox[0] + x * degPerPixelX,
        false,
        tickLabelFormat
      );

      // Top label
      svgElements.push(
        `<text x="${padding + x + frameSpace}" y="${
          padding + frameSpace - tickMinorTickSize - xTickLabelOffset
        }" font-size="${tickMinorLabelSize}" font-family="${tickMinorLabelFont}" fill="${tickMinorLabelColor}" text-anchor="middle" transform="rotate(${xTickMinorLabelRotation},${
          padding + x + frameSpace
        },${
          padding + frameSpace - tickMinorTickSize - xTickLabelOffset
        })">${label}</text>`
      );

      // Bottom label
      svgElements.push(
        `<text x="${padding + x + frameSpace}" y="${
          padding + height + frameSpace + tickMinorTickSize + xTickLabelOffset
        }" font-size="${tickMinorLabelSize}" font-family="${tickMinorLabelFont}" fill="${tickMinorLabelColor}" text-anchor="middle" dominant-baseline="text-before-edge" transform="rotate(${xTickMinorLabelRotation},${
          padding + x + frameSpace
        },${
          padding + height + frameSpace + tickMinorTickSize + xTickLabelOffset
        })">${label}</text>`
      );
    }
  }

  // Y-axis major ticks & labels
  let yMajorStart =
    Math.round((bbox[1] + esp) / tickMajorTickStep) * tickMajorTickStep;
  for (let lat = yMajorStart; lat <= bbox[3] + esp; lat += tickMajorTickStep) {
    const y = (bbox[3] - lat) / degPerPixelY;

    // Left tick
    svgElements.push(
      `<line x1="${padding + frameSpace}" y1="${
        padding + y + frameSpace
      }" x2="${padding + frameSpace - tickMajorTickSize}" y2="${
        padding + y + frameSpace
      }" stroke="${tickMajorColor}" stroke-width="${tickMajorTickWidth}" />`
    );

    // Right tick
    svgElements.push(
      `<line x1="${padding + width + frameSpace}" y1="${
        padding + y + frameSpace
      }" x2="${padding + width + frameSpace + tickMajorTickSize}" y2="${
        padding + y + frameSpace
      }" stroke="${tickMajorColor}" stroke-width="${tickMajorTickWidth}" />`
    );

    if (tickMajorLabelSize > 0) {
      const label = formatDegree(
        bbox[3] - y * degPerPixelY,
        true,
        tickLabelFormat
      );

      // Left label
      svgElements.push(
        `<text x="${
          padding + frameSpace - tickMajorTickSize - yTickLabelOffset
        }" y="${
          padding + y + frameSpace
        }" font-size="${tickMajorLabelSize}" font-family="${tickMajorLabelFont}" fill="${tickMajorLabelColor}" text-anchor="end" dominant-baseline="middle" transform="rotate(${yTickMajorLabelRotation},${
          padding + frameSpace - tickMajorTickSize - yTickLabelOffset
        },${padding + y + frameSpace})">${label}</text>`
      );

      // Right label
      svgElements.push(
        `<text x="${
          padding + width + frameSpace + tickMajorTickSize + yTickLabelOffset
        }" y="${
          padding + y + frameSpace
        }" font-size="${tickMajorLabelSize}" font-family="${tickMajorLabelFont}" fill="${tickMajorLabelColor}" text-anchor="start" dominant-baseline="middle" transform="rotate(${yTickMajorLabelRotation},${
          padding + width + frameSpace + tickMajorTickSize + yTickLabelOffset
        },${padding + y + frameSpace})">${label}</text>`
      );
    }
  }

  // Y-axis minor ticks & labels
  let yMinorStart = Math.round(bbox[1] / tickMinorTickStep) * tickMinorTickStep;
  for (let lat = yMinorStart; lat <= bbox[3] + esp; lat += tickMinorTickStep) {
    const y = (bbox[3] - lat) / degPerPixelY;

    // Left tick
    svgElements.push(
      `<line x1="${padding + frameSpace}" y1="${
        padding + y + frameSpace
      }" x2="${padding + frameSpace - tickMinorTickSize}" y2="${
        padding + y + frameSpace
      }" stroke="${tickMinorColor}" stroke-width="${tickMinorTickWidth}" />`
    );

    // Right tick
    svgElements.push(
      `<line x1="${padding + width + frameSpace}" y1="${
        padding + y + frameSpace
      }" x2="${padding + width + frameSpace + tickMinorTickSize}" y2="${
        padding + y + frameSpace
      }" stroke="${tickMinorColor}" stroke-width="${tickMinorTickWidth}" />`
    );

    if (tickMinorLabelSize > 0) {
      const label = formatDegree(
        bbox[3] - y * degPerPixelY,
        true,
        tickLabelFormat
      );

      // Left label
      svgElements.push(
        `<text x="${
          padding + frameSpace - tickMinorTickSize - yTickLabelOffset
        }" y="${
          padding + y + frameSpace
        }" font-size="${tickMinorLabelSize}" font-family="${tickMinorLabelFont}" fill="${tickMinorLabelColor}" text-anchor="end" dominant-baseline="middle" transform="rotate(${yTickMinorLabelRotation},${
          padding + frameSpace - tickMinorTickSize - yTickLabelOffset
        },${padding + y + frameSpace})">${label}</text>`
      );

      // Right label
      svgElements.push(
        `<text x="${
          padding + width + frameSpace + tickMinorTickSize + yTickLabelOffset
        }" y="${
          padding + y + frameSpace
        }" font-size="${tickMinorLabelSize}" font-family="${tickMinorLabelFont}" fill="${tickMinorLabelColor}" text-anchor="start" dominant-baseline="middle" transform="rotate(${yTickMinorLabelRotation},${
          padding + width + frameSpace + tickMinorTickSize + yTickLabelOffset
        },${padding + y + frameSpace})">${label}</text>`
      );
    }
  }

  // Create image
  image
    .extend({
      top: padding + frameSpace,
      left: padding + frameSpace,
      bottom: padding + frameSpace,
      right: padding + frameSpace,
      background: { r: 255, g: 255, b: 255, alpha: 1 },
    })
    .composite([
      {
        input: Buffer.from(
          `<svg width="${padding * 2 + width + frameSpace * 2}" height="${
            padding * 2 + height + frameSpace * 2
          }" xmlns="http://www.w3.org/2000/svg">${svgElements.join("")}</svg>`
        ),
        left: 0,
        top: 0,
      },
    ]);

  if (output.width > 0 || output.height > 0) {
    image.resize(output.width, output.height);
  }

  switch (output.format) {
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

  await image.toFile(output.filePath);
}

/**
 * Merge images
 * @param {{ content: string, bbox: [number, number, number, number] }} baselayer Baselayer object
 * @param {{ content: string, bbox: [number, number, number, number] }[]} overlays Array of overlay object
 * @param {{ format: string, filePath: string, width: number, height: number }} output Output object
 * @returns {Promise<void>}
 */
export async function mergeImages(baselayer, overlays, output) {
  await mkdir(path.dirname(output.filePath), {
    recursive: true,
  });

  const baseImage = sharp(
    (await isExistFile(baselayer.content))
      ? baselayer.content
      : Buffer.from(baselayer.content),
    {
      limitInputPixels: false,
    }
  );
  const baseImageMetadata = await baseImage.metadata();
  const baseImageExtent = [
    ...lonLat4326ToXY3857(baselayer.bbox[0], baselayer.bbox[1]),
    ...lonLat4326ToXY3857(baselayer.bbox[2], baselayer.bbox[3]),
  ];
  const baseImageWidthResolution =
    baseImageMetadata.width / (baseImageExtent[2] - baseImageExtent[0]);
  const baseImageHeightResolution =
    baseImageMetadata.height / (baseImageExtent[3] - baseImageExtent[1]);

  baseImage.composite(
    await Promise.all(
      overlays.map(async (overlay) => {
        const overlayImage = sharp(
          (await isExistFile(overlay.content))
            ? overlay.content
            : Buffer.from(overlay.content),
          {
            limitInputPixels: false,
          }
        );
        const overlayExtent = [
          ...lonLat4326ToXY3857(overlay.bbox[0], overlay.bbox[1]),
          ...lonLat4326ToXY3857(overlay.bbox[2], overlay.bbox[3]),
        ];

        return {
          input: await overlayImage
            .resize(
              Math.ceil(
                (overlayExtent[2] - overlayExtent[0]) * baseImageWidthResolution
              ),
              Math.ceil(
                (overlayExtent[3] - overlayExtent[1]) *
                  baseImageHeightResolution
              )
            )
            .toBuffer(),
          left: Math.floor(
            (overlayExtent[0] - baseImageExtent[0]) * baseImageWidthResolution
          ),
          top: Math.floor(
            (baseImageExtent[3] - overlayExtent[3]) * baseImageHeightResolution
          ),
        };
      })
    )
  );

  if (output.width > 0 || output.height > 0) {
    baseImage.resize(output.width, output.height);
  }

  switch (output.format) {
    case "gif": {
      baseImage.gif({});

      break;
    }

    case "png": {
      baseImage.png({
        compressionLevel: 9,
      });

      break;
    }

    case "jpg":
    case "jpeg": {
      baseImage.jpeg({
        quality: 100,
      });

      break;
    }

    case "webp": {
      baseImage.webp({
        quality: 100,
      });

      break;
    }
  }

  await baseImage.toFile(output.filePath);
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

    const { data, info } = await sharp(buffer, {
      limitInputPixels: false,
    })
      .raw()
      .toBuffer({
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
 * Process image data
 * @param {Buffer} data Image data
 * @param {number} originWidth Image origin width size
 * @param {number} originHeight Image origin height size
 * @param {number} targetWidth Image origin width size
 * @param {number} targetHeight Image origin height size
 * @param {"jpeg"|"jpg"|"png"|"webp"|"gif"} format Tile format
 * @returns {Promise<Buffer>}
 */
export async function processImageData(
  data,
  originWidth,
  originHeight,
  targetWidth,
  targetHeight,
  format
) {
  const image = sharp(data, {
    limitInputPixels: false,
    raw: {
      premultiplied: true,
      width: originWidth,
      height: originHeight,
      channels: 4,
    },
  });

  if (
    (targetWidth !== undefined && targetWidth !== originWidth) ||
    (targetHeight !== undefined && targetHeight !== originHeight)
  ) {
    image.resize({
      width: targetWidth,
      height: targetHeight,
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
  let data = [];

  switch (format) {
    case "gif": {
      data = [
        0x47, 0x49, 0x46, 0x38, 0x39, 0x61, 0x01, 0x00, 0x01, 0x00, 0x80, 0x00,
        0x00, 0x4c, 0x69, 0x71, 0x00, 0x00, 0x00, 0x21, 0xff, 0x0b, 0x4e, 0x45,
        0x54, 0x53, 0x43, 0x41, 0x50, 0x45, 0x32, 0x2e, 0x30, 0x03, 0x01, 0x00,
        0x00, 0x00, 0x21, 0xf9, 0x04, 0x05, 0x00, 0x00, 0x00, 0x00, 0x2c, 0x00,
        0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x02, 0x02, 0x44, 0x01,
        0x00, 0x3b,
      ];
    }

    case "png": {
      data = [
        0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d,
        0x49, 0x48, 0x44, 0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
        0x08, 0x06, 0x00, 0x00, 0x00, 0x1f, 0x15, 0xc4, 0x89, 0x00, 0x00, 0x00,
        0x09, 0x70, 0x48, 0x59, 0x73, 0x00, 0x00, 0x03, 0xe8, 0x00, 0x00, 0x03,
        0xe8, 0x01, 0xb5, 0x7b, 0x52, 0x6b, 0x00, 0x00, 0x00, 0x0d, 0x49, 0x44,
        0x41, 0x54, 0x78, 0x9c, 0x63, 0x60, 0x60, 0x60, 0x60, 0x00, 0x00, 0x00,
        0x05, 0x00, 0x01, 0xa5, 0xf6, 0x45, 0x40, 0x00, 0x00, 0x00, 0x00, 0x49,
        0x45, 0x4e, 0x44, 0xae, 0x42, 0x60, 0x82,
      ];
    }

    case "jpg":
    case "jpeg": {
      data = [
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
      ];
    }

    case "webp": {
      data = [
        0x52, 0x49, 0x46, 0x46, 0x40, 0x00, 0x00, 0x00, 0x57, 0x45, 0x42, 0x50,
        0x56, 0x50, 0x38, 0x58, 0x0a, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x41, 0x4c, 0x50, 0x48, 0x02, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x56, 0x50, 0x38, 0x20, 0x18, 0x00, 0x00, 0x00,
        0x30, 0x01, 0x00, 0x9d, 0x01, 0x2a, 0x01, 0x00, 0x01, 0x00, 0x01, 0x40,
        0x26, 0x25, 0xa4, 0x00, 0x03, 0x70, 0x00, 0xfe, 0xfd, 0x36, 0x68, 0x00,
      ];
    }
  }

  return Buffer.from(data);
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
        await wait25ms();
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
        await wait25ms();
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
    data.description = data.name;
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
    data.bounds = [...metadata.bounds];
  } else {
    data.bounds = [-180, -85.051129, 180, 85.051129];
  }

  if (metadata.center !== undefined) {
    data.center = [...metadata.center];
  } else {
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
