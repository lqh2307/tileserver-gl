"use strict";

import { createReadStream, createWriteStream } from "node:fs";
import { StatusCodes } from "http-status-codes";
import { printLog } from "./logger.js";
import { spawn } from "child_process";
import handlebars from "handlebars";
import archiver from "archiver";
import https from "node:https";
import { jsPDF } from "jspdf";
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
// sharp.timeout({
//   seconds: 300,
// });

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
    if (error.statusCode) {
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
    if (error.statusCode) {
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
    if (error.statusCode) {
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
    (scheme) => url.startsWith(scheme)
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
 * Calculate resolution
 * @param {{ filePath: string|Buffer, bbox: [number, number, number, number], width: number, height: number }} input Input object
 * @param {"km"|"m"|"dm"|"cm"|"mm"} unit unit
 * @returns {Promise<[number, number]>} [X resolution (m/pixel), Y resolution (m/pixel)]
 */
export async function calculateResolution(input, unit) {
  const [minX, minY] = lonLat4326ToXY3857(input.bbox[0], input.bbox[1]);
  const [maxX, maxY] = lonLat4326ToXY3857(input.bbox[2], input.bbox[3]);
  let resolution;

  if (input.filePath) {
    const { width, height } = await sharp(input.filePath, {
      limitInputPixels: false,
    }).metadata();

    resolution = [(maxX - minX) / width, (maxY - minY) / height];
  } else {
    resolution = [(maxX - minX) / input.width, (maxY - minY) / input.height];
  }

  switch (unit) {
    default: {
      return resolution;
    }

    case "km": {
      return [resolution[0] / 1000, resolution[1] / 1000];
    }

    case "hm": {
      return [resolution[0] / 100, resolution[1] / 100];
    }

    case "dam": {
      return [resolution[0] / 10, resolution[1] / 10];
    }

    case "m": {
      return resolution;
    }

    case "dm": {
      return [resolution[0] * 10, resolution[1] * 10];
    }

    case "cm": {
      return [resolution[0] * 100, resolution[1] * 100];
    }

    case "mm": {
      return [resolution[0] * 1000, resolution[1] * 1000];
    }
  }
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
 * @returns {Promise<{minZoom: number, maxZoom: number}>} Zoom levels
 */
export async function calculateZoomLevels(bbox, width, height, tileSize = 256) {
  const [xRes, yRes] = await calculateResolution({
    bbox: bbox,
    width: width,
    height: height,
  });

  let maxZoom = Math.round(
    Math.log2((2 * Math.PI * 6378137.0) / tileSize / Math.min(xRes, yRes))
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

  const resolution = (2 * Math.PI * 6378137.0) / tileSize;

  return {
    width: Math.round((Math.pow(2, z) * (maxX - minX)) / resolution),
    height: Math.round((Math.pow(2, z) * (maxY - minY)) / resolution),
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
    const bbox = coverage.circle
      ? getBBoxFromCircle(coverage.circle.center, coverage.circle.radius)
      : deepClone(coverage.bbox);

    if (limitedBBox) {
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

  if (points.length) {
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
 * Convert zoom to scale
 * @param {number} zoom Zoom
 * @param {number} ppi Pixel per inch
 * @param {256|512} tileSize Tile size
 * @returns {number} Scale
 */
export function zoomToScale(zoom, ppi = 96, tileSize = 256) {
  return (
    (ppi * ((2 * Math.PI * 6378137.0) / tileSize / Math.pow(2, zoom))) / 0.0254
  );
}

/**
 * Convert scale to zoom
 * @param {number} scale Scale
 * @param {number} ppi Pixel per inch
 * @param {256|512} tileSize Tile size
 * @returns {number} zoom
 */
export function scaleToZoom(scale, ppi = 96, tileSize = 256) {
  return Math.log2(
    ppi * ((2 * Math.PI * 6378137.0) / tileSize / scale / 0.0254)
  );
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

      if (entry.isFile() && (regex === undefined || regex.test(entry.name))) {
        hasMatchingFile = true;
      } else if (entry.isDirectory()) {
        await removeEmptyFolders(fullPath, regex);

        const subEntries = await readdir(fullPath).catch(() => []);
        if (subEntries.length) {
          hasMatchingFile = true;
        }
      }
    })
  );

  if (!hasMatchingFile) {
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
 * Check file or folder is exist?
 * @param {string} fileOrDirPath File or directory path
 * @param {string} isDir Is Folder?
 * @returns {Promise<boolean>}
 */
export async function isExistFile(fileOrDirPath, isDir) {
  try {
    const stats = await stat(fileOrDirPath);

    return isDir ? stats.isDirectory() : stats.isFile();
  } catch (error) {
    if (error.code === "ENOENT") {
      return false;
    } else {
      throw error;
    }
  }
}

/**
 * Find matching files or folders in a directory
 * @param {string} filePath Directory path
 * @param {RegExp} regex The regex to match file or folder names
 * @param {boolean} recurse Whether to search recursively in subdirectories
 * @param {boolean} includeDirPath Whether to include full directory path in results
 * @param {boolean} isDir If true, search for directories; otherwise, search for files
 * @returns {Promise<string[]>} Array of matching file or folder paths
 */
export async function findFiles(
  filePath,
  regex,
  recurse,
  includeDirPath,
  isDir
) {
  const entries = await readdir(filePath, {
    withFileTypes: true,
  });

  const results = [];

  for (const entry of entries) {
    const fullPath = `${filePath}/${entry.name}`;
    const isDirectory = entry.isDirectory();

    if (isDir) {
      if (isDirectory && regex.test(entry.name)) {
        results.push(includeDirPath ? fullPath : entry.name);
      }

      if (isDirectory && recurse) {
        const subEntries = await findFiles(
          fullPath,
          regex,
          recurse,
          includeDirPath,
          isDir
        );
        subEntries.forEach((sub) =>
          results.push(includeDirPath ? `${sub}` : `${entry.name}/${sub}`)
        );
      }
    } else {
      if (!isDirectory && regex.test(entry.name)) {
        results.push(includeDirPath ? fullPath : entry.name);
      }

      if (isDirectory && recurse) {
        const subEntries = await findFiles(
          fullPath,
          regex,
          recurse,
          includeDirPath,
          isDir
        );
        subEntries.forEach((sub) =>
          results.push(
            includeDirPath ? `${fullPath}/${sub}` : `${entry.name}/${sub}`
          )
        );
      }
    }
  }

  return results;
}

/**
 * Remove files or folders
 * @param {string[]} fileOrDirPath File or folder paths
 * @returns {Promise<void>}
 */
export async function removeFiles(fileOrDirPath) {
  await Promise.all(
    fileOrDirPath.map((fileOrFolder) =>
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

    case "pdf": {
      return "application/pdf";
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

    archive.directory(iDirPath);

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
      interval > 0 && callback
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
 * Get image metadata
 * @param {string|Buffer} filePath File path to store file
 * @returns {Promise<sharp.Metadata>}
 */
export async function getImageMetadata(filePath) {
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
      if (isLat) {
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
    if (Number.isInteger(rounded)) {
      return Math.round(rounded).toString();
    }

    return rounded.toFixed(2);
  }
}

/**
 * Convert SVG to Image
 * @param {string|Buffer} svg File path or buffer of SVG
 * @param {{ format: string, filePath: string, width: number, height: number, base64: boolean }} output Output object
 * @returns {Promise<sharp.OutputInfo|Buffer|string>}
 */
export async function convertSVGToImage(svg, output) {
  const image = sharp(svg, {
    limitInputPixels: false,
  });

  // Resize image
  if (output.width > 0 || output.height > 0) {
    image.resize(output.width, output.height);
  }

  // Create format
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

  // Write to output
  if (output.filePath) {
    await mkdir(path.dirname(output.filePath), {
      recursive: true,
    });

    return await image.toFile(output.filePath);
  } else if (output.base64) {
    return createBase64(await image.toBuffer(), output.format);
  } else {
    return await image.toBuffer();
  }
}

/**
 * Get SVG stroke dash array
 * @param {"solid"|"dashed"|"longDashed"|"dotted"|"dashedDot"} style Stroke style
 * @returns {string}
 */
export function getSVGStrokeDashArray(style) {
  let strokeDashArray = "";

  switch (style) {
    case "solid": {
      strokeDashArray = "";

      break;
    }

    case "dashed": {
      strokeDashArray = `stroke-dasharray="5,5"`;

      break;
    }

    case "longDashed": {
      strokeDashArray = `stroke-dasharray="10,5"`;

      break;
    }

    case "dotted": {
      strokeDashArray = `stroke-dasharray="1,3"`;

      break;
    }

    case "dashedDot": {
      strokeDashArray = `stroke-dasharray="5,3,1,3"`;

      break;
    }
  }

  return strokeDashArray;
}

/**
 * Create SVG
 * @param {{ content: string, width: number, height: number }} svg SVG object
 * @param {boolean} isBuffer Is buffer?
 * @returns {string|Buffer}
 */
export function createSVG(svg, isBuffer) {
  const svgString = `<svg width="${svg.width}" height="${
    svg.height
  }" xmlns="http://www.w3.org/2000/svg">${svg.content}</svg>`;

  return isBuffer ? Buffer.from(svgString) : svgString;
}

/**
 * Merge images
 * @param {{ content: string|Buffer, bbox: [number, number, number, number] }} baselayer Baselayer object
 * @param {{ content: string|Buffer, bbox: [number, number, number, number], format: "jpeg"|"jpg"|"png"|"webp"|"gif" }[]} overlays Array of overlay object
 * @param {{ format: "jpeg"|"jpg"|"png"|"webp"|"gif", filePath: string, width: number, height: number, base64: boolean }} output Output object
 * @returns {Promise<sharp.OutputInfo|Buffer|string>}
 */
export async function mergeImages(baselayer, overlays, output) {
  const image = sharp(baselayer.content, {
    limitInputPixels: false,
  });
  const { width, height } = await image.metadata();
  const [baseImageMinX, baseImageMinY] = lonLat4326ToXY3857(
    baselayer.bbox[0],
    baselayer.bbox[1]
  );
  const [baseImageMaxX, baseImageMaxY] = lonLat4326ToXY3857(
    baselayer.bbox[2],
    baselayer.bbox[3]
  );

  const baseImageWidthResolution = width / (baseImageMaxX - baseImageMinX);
  const baseImageHeightResolution = height / (baseImageMaxY - baseImageMinY);

  // Process overlays
  image.composite(
    await Promise.all(
      overlays.map(async (overlay) => {
        const [overlayMinX, overlayMinY] = lonLat4326ToXY3857(
          overlay.bbox[0],
          overlay.bbox[1]
        );
        const [overlayMaxX, overlayMaxY] = lonLat4326ToXY3857(
          overlay.bbox[2],
          overlay.bbox[3]
        );

        const input = sharp(overlay.content, {
          limitInputPixels: false,
        }).resize(
          Math.ceil((overlayMaxX - overlayMinX) * baseImageWidthResolution),
          Math.ceil((overlayMaxY - overlayMinY) * baseImageHeightResolution)
        );

        switch (overlay.format) {
          case "gif": {
            input.gif({});

            break;
          }

          case "png": {
            input.png({
              compressionLevel: 9,
            });

            break;
          }

          case "jpg":
          case "jpeg": {
            input.jpeg({
              quality: 100,
            });

            break;
          }

          case "webp": {
            input.webp({
              quality: 100,
            });

            break;
          }
        }

        return {
          limitInputPixels: false,
          input: await input.toBuffer(),
          left: Math.floor(
            (overlayMinX - baseImageMinX) * baseImageWidthResolution
          ),
          top: Math.floor(
            (baseImageMaxY - overlayMaxY) * baseImageHeightResolution
          ),
        };
      })
    )
  );

  // Resize image
  if (output.width > 0 || output.height > 0) {
    image.resize(output.width, output.height);
  }

  // Create format
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

  // Write to output
  if (output.filePath) {
    await mkdir(path.dirname(output.filePath), {
      recursive: true,
    });

    return await image.toFile(output.filePath);
  } else if (output.base64) {
    return createBase64(await image.toBuffer(), output.format);
  } else {
    return await image.toBuffer();
  }
}

/**
 * Add frame to image
 * @param {{ filePath: string|Buffer, bbox: [number, number, number, number] }} input Input object
 * @param {[number, number, number, number]} bbox Bounding box in EPSG:4326
 * @param {object} frame Frame options object
 * @param {object} grid Grid options object
 * @param {{ format: string, filePath: string, base64: boolean }} output Output object
 * @returns {Promise<sharp.OutputInfo|Buffer|string>}
 */
export async function addFrameToImage(input, frame, grid, output) {
  const image = sharp(input.filePath, {
    limitInputPixels: false,
  });

  const bbox = input.bbox;
  const { width, height } = await image.metadata();

  const svg = {
    content: "",
    width: width,
    height: height,
  };

  let totalMargin = 0;
  const degPerPixelX = (bbox[2] - bbox[0]) / width;
  const degPerPixelY = (bbox[3] - bbox[1]) / height;

  // Process image
  if (frame) {
    let {
      frameMargin,

      frameInnerColor = "rgba(0,0,0,1)",
      frameInnerWidth = 6,
      frameInnerStyle = "solid",

      frameOuterColor = "rgba(0,0,0,1)",
      frameOuterWidth = 6,
      frameOuterStyle = "solid",

      frameSpace = 120,

      tickLabelFormat = "DMSD",

      majorTickStep = 0.5,
      minorTickStep = 0.1,

      majorTickWidth = 6,
      minorTickWidth = 4,

      majorTickSize = 30,
      minorTickSize = 15,

      majorTickLabelSize = 15,
      minorTickLabelSize = 0,

      majorTickColor = "rgba(0,0,0,1)",
      minorTickColor = "rgba(0,0,0,1)",

      majorTickLabelColor = "rgba(0,0,0,1)",
      minorTickLabelColor = "rgba(0,0,0,1)",

      majorTickLabelFont = "sans-serif",
      minorTickLabelFont = "sans-serif",

      xTickLabelOffset = 5,
      yTickLabelOffset = 5,

      xTickMajorLabelRotation = 0,
      xTickMinorLabelRotation = 0,

      yTickMajorLabelRotation = 0,
      yTickMinorLabelRotation = 0,
    } = frame;

    if (frameMargin === undefined) {
      frameMargin = Math.ceil(frameOuterWidth / 2);
    }

    frameInnerStyle = getSVGStrokeDashArray(frameInnerStyle);
    frameOuterStyle = getSVGStrokeDashArray(frameOuterStyle);

    totalMargin = frameMargin + frameSpace;

    svg.width = totalMargin * 2 + width;
    svg.height = totalMargin * 2 + height;

    // Inner frame
    svg.content += `<rect x="${totalMargin}" y="${totalMargin}" width="${width}" height="${height}" fill="none" stroke="${frameInnerColor}" stroke-width="${frameInnerWidth}" ${frameInnerStyle}/>`;

    // Outer frame
    svg.content += `<rect x="${frameMargin}" y="${frameMargin}" width="${
      width + frameSpace * 2
    }" height="${
      height + frameSpace * 2
    }" fill="none" stroke="${frameOuterColor}" stroke-width="${frameOuterWidth}" ${frameOuterStyle}/>`;

    // X-axis major ticks & labels
    let xTickMajorStart = Math.round(bbox[0] / majorTickStep) * majorTickStep;
    if (xTickMajorStart < bbox[2]) {
      xTickMajorStart += majorTickStep;
    }
    for (
      let lon = xTickMajorStart;
      lon <= bbox[2] + 1e-9;
      lon += majorTickStep
    ) {
      const x = (lon - bbox[0]) / degPerPixelX;

      // Top tick
      svg.content += `<line x1="${totalMargin + x}" y1="${totalMargin}" x2="${
        totalMargin + x
      }" y2="${
        totalMargin - majorTickSize
      }" stroke="${majorTickColor}" stroke-width="${majorTickWidth}" />`;

      // Bottom tick
      svg.content += `<line x1="${totalMargin + x}" y1="${
        totalMargin + height
      }" x2="${totalMargin + x}" y2="${
        totalMargin + height + majorTickSize
      }" stroke="${majorTickColor}" stroke-width="${majorTickWidth}" />`;

      if (majorTickLabelSize > 0) {
        const label = formatDegree(
          bbox[0] + x * degPerPixelX,
          false,
          tickLabelFormat
        );

        // Top label
        svg.content += `<text x="${totalMargin + x}" y="${
          totalMargin - majorTickSize - xTickLabelOffset
        }" font-size="${majorTickLabelSize}" font-family="${majorTickLabelFont}" fill="${majorTickLabelColor}" text-anchor="middle" transform="rotate(${xTickMajorLabelRotation},${
          totalMargin + x
        },${totalMargin - majorTickSize - xTickLabelOffset})">${label}</text>`;

        // Bottom label
        svg.content += `<text x="${totalMargin + x}" y="${
          totalMargin + height + majorTickSize + xTickLabelOffset
        }" font-size="${majorTickLabelSize}" font-family="${majorTickLabelFont}" fill="${majorTickLabelColor}" text-anchor="middle" dominant-baseline="text-before-edge" transform="rotate(${xTickMajorLabelRotation},${
          totalMargin + x
        },${
          totalMargin + height + majorTickSize + xTickLabelOffset
        })">${label}</text>`;
      }
    }

    // X-axis minor ticks & labels
    let xTickMinorStart = Math.round(bbox[0] / minorTickStep) * minorTickStep;
    if (xTickMinorStart < bbox[2]) {
      xTickMinorStart += minorTickStep;
    }
    for (
      let lon = xTickMinorStart;
      lon <= bbox[2] + 1e-9;
      lon += minorTickStep
    ) {
      const x = (lon - bbox[0]) / degPerPixelX;

      // Top tick
      svg.content += `<line x1="${totalMargin + x}" y1="${totalMargin}" x2="${
        totalMargin + x
      }" y2="${
        totalMargin - minorTickSize
      }" stroke="${minorTickColor}" stroke-width="${minorTickWidth}" />`;

      // Bottom tick
      svg.content += `<line x1="${totalMargin + x}" y1="${
        totalMargin + height
      }" x2="${totalMargin + x}" y2="${
        totalMargin + height + minorTickSize
      }" stroke="${minorTickColor}" stroke-width="${minorTickWidth}" />`;

      if (minorTickLabelSize > 0) {
        const label = formatDegree(
          bbox[0] + x * degPerPixelX,
          false,
          tickLabelFormat
        );

        // Top label
        svg.content += `<text x="${totalMargin + x}" y="${
          totalMargin - minorTickSize - xTickLabelOffset
        }" font-size="${minorTickLabelSize}" font-family="${minorTickLabelFont}" fill="${minorTickLabelColor}" text-anchor="middle" transform="rotate(${xTickMinorLabelRotation},${
          totalMargin + x
        },${totalMargin - minorTickSize - xTickLabelOffset})">${label}</text>`;

        // Bottom label
        svg.content += `<text x="${totalMargin + x}" y="${
          totalMargin + height + minorTickSize + xTickLabelOffset
        }" font-size="${minorTickLabelSize}" font-family="${minorTickLabelFont}" fill="${minorTickLabelColor}" text-anchor="middle" dominant-baseline="text-before-edge" transform="rotate(${xTickMinorLabelRotation},${
          totalMargin + x
        },${
          totalMargin + height + minorTickSize + xTickLabelOffset
        })">${label}</text>`;
      }
    }

    // Y-axis major ticks & labels
    let yTickMajorStart = Math.round(bbox[1] / majorTickStep) * majorTickStep;
    if (yTickMajorStart < bbox[3]) {
      yTickMajorStart += majorTickStep;
    }
    for (
      let lat = yTickMajorStart;
      lat <= bbox[3] + 1e-9;
      lat += majorTickStep
    ) {
      const y = (bbox[3] - lat) / degPerPixelY;

      // Left tick
      svg.content += `<line x1="${totalMargin}" y1="${totalMargin + y}" x2="${
        totalMargin - majorTickSize
      }" y2="${
        totalMargin + y
      }" stroke="${majorTickColor}" stroke-width="${majorTickWidth}" />`;

      // Right tick
      svg.content += `<line x1="${totalMargin + width}" y1="${
        totalMargin + y
      }" x2="${totalMargin + width + majorTickSize}" y2="${
        totalMargin + y
      }" stroke="${majorTickColor}" stroke-width="${majorTickWidth}" />`;

      if (majorTickLabelSize > 0) {
        const label = formatDegree(
          bbox[3] - y * degPerPixelY,
          true,
          tickLabelFormat
        );

        // Left label
        svg.content += `<text x="${totalMargin - majorTickSize - yTickLabelOffset}" y="${
          totalMargin + y
        }" font-size="${majorTickLabelSize}" font-family="${majorTickLabelFont}" fill="${majorTickLabelColor}" text-anchor="end" dominant-baseline="middle" transform="rotate(${yTickMajorLabelRotation},${
          totalMargin - majorTickSize - yTickLabelOffset
        },${totalMargin + y})">${label}</text>`;

        // Right label
        svg.content += `<text x="${
          totalMargin + width + majorTickSize + yTickLabelOffset
        }" y="${
          totalMargin + y
        }" font-size="${majorTickLabelSize}" font-family="${majorTickLabelFont}" fill="${majorTickLabelColor}" text-anchor="start" dominant-baseline="middle" transform="rotate(${yTickMajorLabelRotation},${
          totalMargin + width + majorTickSize + yTickLabelOffset
        },${totalMargin + y})">${label}</text>`;
      }
    }

    // Y-axis minor ticks & labels
    let yTickMinorStart = Math.round(bbox[1] / minorTickStep) * minorTickStep;
    if (yTickMinorStart < bbox[3]) {
      yTickMinorStart += minorTickStep;
    }
    for (
      let lat = yTickMinorStart;
      lat <= bbox[3] + 1e-9;
      lat += minorTickStep
    ) {
      const y = (bbox[3] - lat) / degPerPixelY;

      // Left tick
      svg.content += `<line x1="${totalMargin}" y1="${totalMargin + y}" x2="${
        totalMargin - minorTickSize
      }" y2="${
        totalMargin + y
      }" stroke="${minorTickColor}" stroke-width="${minorTickWidth}" />`;

      // Right tick
      svg.content += `<line x1="${totalMargin + width}" y1="${
        totalMargin + y
      }" x2="${totalMargin + width + minorTickSize}" y2="${
        totalMargin + y
      }" stroke="${minorTickColor}" stroke-width="${minorTickWidth}" />`;

      if (minorTickLabelSize > 0) {
        const label = formatDegree(
          bbox[3] - y * degPerPixelY,
          true,
          tickLabelFormat
        );

        // Left label
        svg.content += `<text x="${totalMargin - minorTickSize - yTickLabelOffset}" y="${
          totalMargin + y
        }" font-size="${minorTickLabelSize}" font-family="${minorTickLabelFont}" fill="${minorTickLabelColor}" text-anchor="end" dominant-baseline="middle" transform="rotate(${yTickMinorLabelRotation},${
          totalMargin - minorTickSize - yTickLabelOffset
        },${totalMargin + y})">${label}</text>`;

        // Right label
        svg.content += `<text x="${
          totalMargin + width + minorTickSize + yTickLabelOffset
        }" y="${
          totalMargin + y
        }" font-size="${minorTickLabelSize}" font-family="${minorTickLabelFont}" fill="${minorTickLabelColor}" text-anchor="start" dominant-baseline="middle" transform="rotate(${yTickMinorLabelRotation},${
          totalMargin + width + minorTickSize + yTickLabelOffset
        },${totalMargin + y})">${label}</text>`;
      }
    }

    // Extend image
    image.extend({
      top: totalMargin,
      left: totalMargin,
      bottom: totalMargin,
      right: totalMargin,
      background: { r: 0, g: 0, b: 0, alpha: 0 },
    });
  }

  if (grid) {
    let {
      majorGridStyle = "longDashed",
      majorGridWidth = 6,
      majorGridStep = 0.5,
      majorGridColor = "rgba(0,0,0,0.3)",

      minorGridStyle = "longDashed",
      minorGridWidth = 0,
      minorGridStep = 0.1,
      minorGridColor = "rgba(0,0,0,0.3)",
    } = grid;

    majorGridStyle = getSVGStrokeDashArray(majorGridStyle);
    minorGridStyle = getSVGStrokeDashArray(minorGridStyle);

    // X-axis major grids
    if (majorGridWidth > 0) {
      let xGridMajorStart = Math.round(bbox[0] / majorGridStep) * majorGridStep;
      if (xGridMajorStart < bbox[2]) {
        xGridMajorStart += majorGridStep;
      }
      for (
        let lon = xGridMajorStart;
        lon <= bbox[2] + 1e-9;
        lon += majorGridStep
      ) {
        const x = (lon - bbox[0]) / degPerPixelX;

        svg.content += `<line x1="${
          totalMargin + x
        }" y1="${totalMargin}" x2="${totalMargin + x}" y2="${
          totalMargin + height
        }" stroke="${majorGridColor}" stroke-width="${majorGridWidth}" ${majorGridStyle}/>`;
      }
    }

    // X-axis minor grids
    if (minorGridWidth > 0) {
      let xGridMinorStart = Math.round(bbox[0] / minorGridStep) * minorGridStep;
      if (xGridMinorStart < bbox[2]) {
        xGridMinorStart += minorGridStep;
      }
      for (
        let lon = xGridMinorStart;
        lon <= bbox[2] + 1e-9;
        lon += minorGridStep
      ) {
        const x = (lon - bbox[0]) / degPerPixelX;

        svg.content += `<line x1="${
          totalMargin + x
        }" y1="${totalMargin}" x2="${totalMargin + x}" y2="${
          totalMargin + height
        }" stroke="${minorGridColor}" stroke-width="${minorGridWidth}" ${minorGridStyle}/>`;
      }
    }

    // Y-axis major grids
    if (majorGridWidth > 0) {
      let yGridMajorStart = Math.round(bbox[1] / majorGridStep) * majorGridStep;
      if (yGridMajorStart < bbox[3]) {
        yGridMajorStart += majorGridStep;
      }
      for (
        let lat = yGridMajorStart;
        lat <= bbox[3] + 1e-9;
        lat += majorGridStep
      ) {
        const y = (bbox[3] - lat) / degPerPixelY;

        svg.content += `<line x1="${totalMargin}" y1="${
          totalMargin + y
        }" x2="${totalMargin + width}" y2="${
          totalMargin + y
        }" stroke="${majorGridColor}" stroke-width="${majorGridWidth}" ${majorGridStyle}/>`;
      }
    }

    // Y-axis minor grids
    if (minorGridWidth > 0) {
      let yGridMinorStart = Math.round(bbox[1] / minorGridStep) * minorGridStep;
      if (yGridMinorStart < bbox[3]) {
        yGridMinorStart += minorGridStep;
      }
      for (
        let lat = yGridMinorStart;
        lat <= bbox[3] + 1e-9;
        lat += minorGridStep
      ) {
        const y = (bbox[3] - lat) / degPerPixelY;

        svg.content += `<line x1="${totalMargin}" y1="${
          totalMargin + y
        }" x2="${totalMargin + width}" y2="${
          totalMargin + y
        }" stroke="${minorGridColor}" stroke-width="${minorGridWidth}" ${minorGridStyle}/>`;
      }
    }
  }

  // Composite image
  if (svg.content) {
    image.composite([
      {
        limitInputPixels: false,
        input: createSVG(svg, true),
        left: 0,
        top: 0,
      },
    ]);
  }

  // Create format
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

  // Write to output
  if (output.filePath) {
    await mkdir(path.dirname(output.filePath), {
      recursive: true,
    });

    return await image.toFile(output.filePath);
  } else if (output.base64) {
    return createBase64(await image.toBuffer(), output.format);
  } else {
    return await image.toBuffer();
  }
}

/**
 * Split image to PDF
 * @param {{ image: string|Buffer, res: [number, number] }} input Input object
 * @param {object} preview Preview options object
 * @param {{ filePath: string, paperSize: [number, number], orientation: "portrait"|"landscape", base64: boolean, center: boolean }} output Output object
 * @returns {Promise<jsPDF|sharp.OutputInfo|Buffer|string>}
 */
export async function splitImage(input, preview, output) {
  const { width, height } = await sharp(input.image, {
    limitInputPixels: false,
  }).metadata();

  const paperHeight =
    output.orientation === "landscape"
      ? output.paperSize[0]
      : output.paperSize[1];
  const paperWidth =
    output.orientation === "landscape"
      ? output.paperSize[1]
      : output.paperSize[0];

  const stepHeightPX = Math.round(paperHeight / input.resolution[1]);
  const stepWidthPX = Math.round(paperWidth / input.resolution[0]);

  const heightPageNum = Math.ceil((height * input.resolution[1]) / paperHeight);
  const widthPageNum = Math.ceil((width * input.resolution[0]) / paperWidth);

  const newHeight = heightPageNum * stepHeightPX;
  const newWidth = widthPageNum * stepWidthPX;

  const extendTop = output.center ? Math.floor((newHeight - height) / 2) : 0;
  const extendLeft = output.center ? Math.floor((newWidth - width) / 2) : 0;
  const extendBottom = newHeight - height - extendTop;
  const extendRight = newWidth - width - extendLeft;

  const newImage = await sharp(input.image, {
    limitInputPixels: false,
  })
    .extend({
      top: extendTop,
      left: extendLeft,
      bottom: extendBottom,
      right: extendRight,
      background: { r: 0, g: 0, b: 0, alpha: 0 },
    })
    .png({
      compressionLevel: 9,
    })
    .toBuffer();

  if (preview) {
    let {
      format = "png",
      lineColor = "rgba(255,0,0,1)",
      lineWidth = 6,
      lineStyle = "solid",
      pageColor = "rgba(255,0,0,1)",
      pageSize = 100,
      pageFont = "sans-serif",
    } = preview;

    lineStyle = getSVGStrokeDashArray(lineStyle);

    const svg = {
      content: "",
      width: newWidth,
      height: newHeight,
    };

    for (let y = 0; y < heightPageNum; y++) {
      for (let x = 0; x < widthPageNum; x++) {
        if (lineWidth > 0) {
          svg.content += `<rect x="${x * stepWidthPX}" y="${
            y * stepHeightPX
          }" width="${stepWidthPX}" height="${stepHeightPX}" fill="none" stroke="${lineColor}" stroke-width="${lineWidth}" ${lineStyle}/>`;
        }

        if (pageSize > 0) {
          svg.content += `<text x="${x * stepWidthPX + stepWidthPX / 2}" y="${
            y * stepHeightPX + stepHeightPX / 2
          }" text-anchor="middle" alignment-baseline="middle" font-family="${pageFont}" font-size="${pageSize}" fill="${pageColor}">${
            y + x + 1
          }</text>`;
        }
      }
    }

    // Create image
    const image = sharp(newImage, {
      limitInputPixels: false,
    }).composite([
      {
        limitInputPixels: false,
        input: createSVG(svg, true),
        left: 0,
        top: 0,
      },
    ]);

    // Resize image
    if (preview.width > 0 || preview.height > 0) {
      image.resize(preview.width, preview.height);
    }

    // Create format
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

    // Write to output
    if (output.filePath) {
      await mkdir(path.dirname(output.filePath), {
        recursive: true,
      });

      return await image.toFile(output.filePath);
    } else if (output.base64) {
      return createBase64(await image.toBuffer(), format);
    } else {
      return await image.toBuffer();
    }
  } else {
    const doc = new jsPDF({
      orientation: output.orientation,
      unit: "mm",
      format: output.paperSize,
      compress: true,
    });

    for (let y = 0; y < heightPageNum; y++) {
      for (let x = 0; x < widthPageNum; x++) {
        const image = await sharp(newImage, {
          limitInputPixels: false,
        })
          .extract({
            left: x * stepWidthPX,
            top: y * stepHeightPX,
            width: stepWidthPX,
            height: stepHeightPX,
          })
          .png({
            compressionLevel: 9,
          })
          .toBuffer();

        if (x > 0 || y > 0) {
          doc.addPage(output.paperSize, output.orientation);
        }

        doc.addImage(
          image,
          "png",
          0,
          0,
          stepWidthPX * input.resolution[0],
          stepHeightPX * input.resolution[1]
        );
      }
    }

    // Write to output
    if (output.filePath) {
      await mkdir(path.dirname(output.filePath), {
        recursive: true,
      });

      return doc.save(output.filePath);
    } else if (output.base64) {
      return createBase64(Buffer.from(doc.output("arraybuffer")), "pdf");
    } else {
      return doc.output("arraybuffer");
    }
  }
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
 * @param {Buffer} data Image data buffer
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
    (targetWidth > 0 && targetWidth !== originWidth) ||
    (targetHeight > 0 && targetHeight !== originHeight)
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
 * Create fallback tile data (1px x 1px)
 * @param {"jpeg"|"jpg"|"png"|"webp"|"gif"|"pbf"} format Tile format
 * @returns {Buffer}
 */
export function createFallbackTileData(format) {
  // switch (format) {
  //   default: {
  //     return Buffer.from([]);
  //   }

  //   case "gif": {
  //     return await sharp(Buffer.from([0, 0, 0, 0]), {
  //       raw: {
  //         width: 1,
  //         height: 1,
  //         channels: 4,
  //       },
  //     })
  //       .gif({})
  //       .toBuffer();
  //   }

  //   case "png": {
  //     return await sharp(Buffer.from([0, 0, 0, 0]), {
  //       raw: {
  //         width: 1,
  //         height: 1,
  //         channels: 4,
  //       },
  //     })
  //       .png({
  //         compressionLevel: 9,
  //       })
  //       .toBuffer();
  //   }

  //   case "jpg":
  //   case "jpeg": {
  //     return await sharp(Buffer.from([255, 255, 255]), {
  //       raw: {
  //         width: 1,
  //         height: 1,
  //         channels: 3,
  //       },
  //     })
  //       .jpeg({
  //         quality: 100,
  //       })
  //       .toBuffer();
  //   }

  //   case "webp": {
  //     return await sharp(Buffer.from([0, 0, 0, 0]), {
  //       raw: {
  //         width: 1,
  //         height: 1,
  //         channels: 4,
  //       },
  //     })
  //       .webp({
  //         quality: 100,
  //       })
  //       .toBuffer();
  //   }
  // }

  switch (format) {
    default: {
      return Buffer.from([]);
    }

    case "gif": {
      return Buffer.from([
        0x47, 0x49, 0x46, 0x38, 0x39, 0x61, 0x01, 0x00, 0x01, 0x00, 0x80, 0x00,
        0x00, 0x4c, 0x69, 0x71, 0x00, 0x00, 0x00, 0x21, 0xf9, 0x04, 0x05, 0x00,
        0x00, 0x00, 0x00, 0x2c, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00,
        0x00, 0x02, 0x02, 0x44, 0x01, 0x00, 0x3b,
      ]);
    }

    case "png": {
      return Buffer.from([
        0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d,
        0x49, 0x48, 0x44, 0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
        0x08, 0x06, 0x00, 0x00, 0x00, 0x1f, 0x15, 0xc4, 0x89, 0x00, 0x00, 0x00,
        0x09, 0x70, 0x48, 0x59, 0x73, 0x00, 0x00, 0x03, 0xe8, 0x00, 0x00, 0x03,
        0xe8, 0x01, 0xb5, 0x7b, 0x52, 0x6b, 0x00, 0x00, 0x00, 0x0d, 0x49, 0x44,
        0x41, 0x54, 0x78, 0xda, 0x63, 0x60, 0x60, 0x60, 0x60, 0x00, 0x00, 0x00,
        0x05, 0x00, 0x01, 0x7a, 0xa8, 0x57, 0x50, 0x00, 0x00, 0x00, 0x00, 0x49,
        0x45, 0x4e, 0x44, 0xae, 0x42, 0x60, 0x82,
      ]);
    }

    case "jpg":
    case "jpeg": {
      return Buffer.from([
        0xff, 0xd8, 0xff, 0xdb, 0x00, 0x43, 0x00, 0x01, 0x01, 0x01, 0x01, 0x01,
        0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
        0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
        0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
        0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
        0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0xff,
        0xdb, 0x00, 0x43, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
        0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
        0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
        0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
        0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
        0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0xff, 0xc0, 0x00, 0x11,
        0x08, 0x00, 0x01, 0x00, 0x01, 0x03, 0x01, 0x22, 0x00, 0x02, 0x11, 0x01,
        0x03, 0x11, 0x01, 0xff, 0xc4, 0x00, 0x15, 0x00, 0x01, 0x01, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x0a, 0xff, 0xc4, 0x00, 0x14, 0x10, 0x01, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0xff, 0xc4, 0x00, 0x14, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xc4,
        0x00, 0x14, 0x11, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xda, 0x00, 0x0c,
        0x03, 0x01, 0x00, 0x02, 0x11, 0x03, 0x11, 0x00, 0x3f, 0x00, 0xbf, 0x80,
        0x01, 0xff, 0xd9,
      ]);
    }

    case "webp": {
      return Buffer.from([
        0x52, 0x49, 0x46, 0x46, 0x42, 0x00, 0x00, 0x00, 0x57, 0x45, 0x42, 0x50,
        0x56, 0x50, 0x38, 0x58, 0x0a, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x41, 0x4c, 0x50, 0x48, 0x02, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x56, 0x50, 0x38, 0x20, 0x1a, 0x00, 0x00, 0x00,
        0x30, 0x01, 0x00, 0x9d, 0x01, 0x2a, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00,
        0x00, 0x25, 0xa4, 0x00, 0x03, 0x70, 0x00, 0xfe, 0xff, 0xdf, 0x6e, 0xd4,
        0x00, 0x00,
      ]);
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
        await delay(25);
      } else {
        if (lockFileHandle) {
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
        await delay(25);
      } else {
        if (lockFileHandle) {
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
 * Create tile metadata
 * @param {object} metadata Metadata object
 * @returns {object}
 */
export function createTileMetadata(metadata = {}) {
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

/**
 * Create base64 string from buffer
 * @param {Buffer} buffer Input data
 * @param {format} format Data format
 * @returns {string} Base64 string
 */
export function createBase64(buffer, format) {
  return `data:${detectContentTypeFromFormat(format)};base64,${buffer.toString(
    "base64"
  )}`;
}
