"use strict";

import { StatusCodes } from "http-status-codes";
import fsPromise from "node:fs/promises";
import { printLog } from "./logger.js";
import { exec } from "child_process";
import handlebars from "handlebars";
import https from "node:https";
import http from "node:http";
import path from "node:path";
import crypto from "crypto";
import axios from "axios";
import proj4 from "proj4";
import sharp from "sharp";
import zlib from "zlib";
import util from "util";
import Ajv from "ajv";

sharp.cache(false);

/**
 * Compile template
 * @param {"index"|"viewer"|"vector_data"|"raster_data"|"geojson_group"|"geojson"|"wmts"} template
 * @param {Object} data
 * @returns {Promise<string>}
 */
export async function compileTemplate(template, data) {
  return handlebars.compile(
    await fsPromise.readFile(`public/templates/${template}.tmpl`, "utf8")
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
 * Post data to URL
 * @param {string} url URL to post data
 * @param {number} timeout Timeout in milliseconds
 * @param {Object} body Body
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
 * Get tile bound for specific coverage
 * @param {{ zoom: number, bbox: [number, number, number, number]}} coverage Specific coverage
 * @param {"xyz"|"tms"} scheme Tile scheme
 * @returns {{ total: number, z: number x: [number, number], y: [number, number] }}
 */
export function getTileBoundFromCoverage(coverage, scheme) {
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

  return {
    total: (xMax - xMin + 1) * (yMax - yMin + 1),
    z: coverage.zoom,
    x: [xMin, xMax],
    y: [yMin, yMax],
  };
}

/**
 * Get tile bounds and total count for specific coverages
 * @param {{ zoom: number, bbox: [number, number, number, number]}[]} coverages Specific coverages
 * @param {"xyz"|"tms"} scheme Tile scheme
 * @returns {{ { z: number, x: [number, number], y: [number, number] }[] }}
 */
export function getTileBoundsFromCoverages(coverages, scheme) {
  let total = 0;

  const tileBounds = coverages.map((coverage) => {
    const tileBound = getTileBoundFromCoverage(coverage, scheme);

    total += tileBound.total;

    return tileBound;
  });

  return {
    total,
    tileBounds,
  };
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
  const [xCenter, yCenter] = proj4("EPSG:4326", "EPSG:3857", center);

  let [minLon, minLat] = proj4("EPSG:3857", "EPSG:4326", [
    xCenter - radius,
    yCenter - radius,
  ]);
  let [maxLon, maxLat] = proj4("EPSG:3857", "EPSG:4326", [
    xCenter + radius,
    yCenter + radius,
  ]);

  // Limit longitude
  if (minLon > 180) {
    minLon = 180;
  } else if (minLon < -180) {
    minLon = -180;
  }

  if (maxLon > 180) {
    maxLon = 180;
  } else if (maxLon < -180) {
    maxLon = -180;
  }

  // Limit latitude
  if (minLat > 85.051129) {
    minLat = 85.051129;
  } else if (minLat < -85.051129) {
    minLat = -85.051129;
  }

  if (maxLat > 85.051129) {
    maxLat = 85.051129;
  } else if (maxLat < -85.051129) {
    maxLat = -85.051129;
  }

  return [minLon, minLat, maxLon, maxLat];
}

/**
 * Get bounding box from an array of points
 * @param {[number, number][]} points Array of points in the format [lon, lat]
 * @returns {[number, number, number, number]} Bounding box in the format [minLon, minLat, maxLon, maxLat]
 */
export function getBBoxFromPoint(points) {
  let minLon = -180;
  let minLat = -85.051129;
  let maxLon = 180;
  let maxLat = 85.051129;

  for (let index = 0; index < points.length; index++) {
    if (index === 0) {
      minLon = points[index][0];
      minLat = points[index][1];
      maxLon = points[index][0];
      maxLat = points[index][1];
    } else {
      if (points[index][0] < minLon) {
        minLon = points[index][0];
      }

      if (points[index][1] < minLat) {
        minLat = points[index][1];
      }

      if (points[index][0] > maxLon) {
        maxLon = points[index][0];
      }

      if (points[index][1] > maxLat) {
        maxLat = points[index][1];
      }
    }
  }

  // Limit longitude
  if (minLon > 180) {
    minLon = 180;
  } else if (minLon < -180) {
    minLon = -180;
  }

  if (maxLon > 180) {
    maxLon = 180;
  } else if (maxLon < -180) {
    maxLon = -180;
  }

  // Limit latitude
  if (minLat > 85.051129) {
    minLat = 85.051129;
  } else if (minLat < -85.051129) {
    minLat = -85.051129;
  }

  if (maxLat > 85.051129) {
    maxLat = 85.051129;
  } else if (maxLat < -85.051129) {
    maxLat = -85.051129;
  }

  return [minLon, minLat, maxLon, maxLat];
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
  const entries = await fsPromise.readdir(folderPath, {
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

        const subEntries = await fsPromise.readdir(fullPath).catch(() => []);
        if (subEntries.length > 0) {
          hasMatchingFile = true;
        }
      }
    })
  );

  if (hasMatchingFile === false) {
    await fsPromise.rm(folderPath, {
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
      fsPromise.rm(fileName, {
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
    const stat = await fsPromise.stat(dirPath);

    return stat.isDirectory();
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
    const stat = await fsPromise.stat(filePath);

    return stat.isFile();
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
  const entries = await fsPromise.readdir(dirPath, {
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
  const entries = await fsPromise.readdir(dirPath, {
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
      fsPromise.rm(fileOrFolder, {
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
 * @returns {Object}
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
 * @param {Object} schema JSON schema
 * @param {Object} jsonData JSON data
 * @returns {void}
 */
export function validateJSON(schema, jsonData) {
  try {
    const validate = new Ajv({
      allErrors: true,
      useDefaults: true,
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
 * Deep clone an object using JSON serialization
 * @param {Object} obj The object to clone
 * @returns {Object} The deep-cloned object
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
  return JSON.parse(await fsPromise.readFile("package.json", "utf8")).version;
}

/**
 * Get JSON schema
 * @param {"delete"|"cleanup"|"config"|"seed"|"render"|"coverages"|"sprite"} schema
 * @returns {Promise<Object>}
 */
export async function getJSONSchema(schema) {
  return JSON.parse(await fsPromise.readFile(`schema/${schema}.json`, "utf8"));
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
 * @returns {Promise<Object>}
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
 * Render PNG image
 * @param {Buffer} data PNG image data
 * @param {number} originSize PNG image origin size
 * @param {number} targetSize PNG image target size
 * @param {"jpeg"|"jpg"|"png"|"webp"|"gif"} format Tile format
 * @returns {Promise<Buffer>}
 */
export async function renderImageData(data, originSize, targetSize, format) {
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
      lockFileHandle = await fsPromise.open(lockFilePath, "wx");

      const tempFilePath = `${filePath}.tmp`;

      try {
        await fsPromise.mkdir(path.dirname(filePath), {
          recursive: true,
        });

        await fsPromise.writeFile(tempFilePath, data);

        await fsPromise.rename(tempFilePath, filePath);
      } catch (error) {
        await fsPromise.rm(tempFilePath, {
          force: true,
        });

        throw error;
      }

      await lockFileHandle.close();

      await fsPromise.rm(lockFilePath, {
        force: true,
      });

      return;
    } catch (error) {
      if (error.code === "ENOENT") {
        await fsPromise.mkdir(path.dirname(filePath), {
          recursive: true,
        });

        continue;
      } else if (error.code === "EEXIST") {
        await delay(50);
      } else {
        if (lockFileHandle !== undefined) {
          await lockFileHandle.close();

          await fsPromise.rm(lockFilePath, {
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
      lockFileHandle = await fsPromise.open(lockFilePath, "wx");

      await fsPromise.rm(filePath, {
        force: true,
      });

      await lockFileHandle.close();

      await fsPromise.rm(lockFilePath, {
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

          await fsPromise.rm(lockFilePath, {
            force: true,
          });
        }

        throw error;
      }
    }
  }

  throw new Error(`Timeout to access lock file`);
}
