"use strict";

import { calculateResolution } from "./image.js";
import { limitValue } from "./number.js";

const SPHERICAL_RADIUS = 6378137.0;
const MAX_GM = 2 * Math.PI * SPHERICAL_RADIUS;

/**
 * Convert coordinates from EPSG:4326 (lon, lat) to EPSG:3857 (x, y in meters)
 * @param {number} lon Longitude in degrees
 * @param {number} lat Latitude in degrees
 * @returns {[number, number]} Web Mercator x, y in meters
 */
export function lonLat4326ToXY3857(lon, lat) {
  lon = limitValue(lon, -180, 180);
  lat = limitValue(lat, -85.051129, 85.051129);

  return [
    lon * (Math.PI / 180) * SPHERICAL_RADIUS,
    Math.log(Math.tan((Math.PI * (lat + 90)) / 360)) * SPHERICAL_RADIUS,
  ];
}

/**
 * Convert coordinates from EPSG:3857 (x, y in meters) to EPSG:4326 (lon, lat in degrees)
 * @param {number} x X in meters (Web Mercator)
 * @param {number} y Y in meters (Web Mercator)
 * @returns {[number, number]} Longitude and latitude in degrees
 */
export function xy3857ToLonLat4326(x, y) {
  let lon = (x / SPHERICAL_RADIUS) * (180 / Math.PI);
  let lat = Math.atan(Math.sinh(y / SPHERICAL_RADIUS)) * (180 / Math.PI);

  lon = limitValue(lon, -180, 180);
  lat = limitValue(lat, -85.051129, 85.051129);

  return [lon, lat];
}

/**
 * Get xyz tile indices from longitude, latitude, and zoom level
 * @param {number} lon Longitude in EPSG:4326
 * @param {number} lat Latitude in EPSG:4326
 * @param {number} z Zoom level
 * @param {"xyz"|"tms"} scheme Tile scheme to output (Default: XYZ)
 * @returns {[number, number, number]} Tile indices [x, y, z]
 */
export function getXYZFromLonLatZ(lon, lat, z, scheme) {
  lon = limitValue(lon, -180, 180);
  lat = limitValue(lat, -85.051129, 85.051129);

  const maxTile = 1 << z;

  let x = (0.5 + lon / 360) * maxTile;
  let y =
    (0.5 - Math.log(Math.tan((Math.PI * (lat + 90)) / 360)) / (2 * Math.PI)) *
    maxTile;

  if (scheme === "tms") {
    y = maxTile - y;
  }

  x = limitValue(Math.floor(x), 0, maxTile - 1);
  y = limitValue(Math.floor(y), 0, maxTile - 1);

  return [x, y, z];
}

/**
 * Get xyz tile indices from global pixel coords and zoom
 * @param {number} pixelX Global pixel X at zoom z (origin top-left)
 * @param {number} pixelY Global pixel Y at zoom z (origin top-left)
 * @param {number} z Zoom level
 * @param {"xyz"|"tms"} scheme Output tile scheme
 * @param {256|512} tileSize Tile size (Default: 256)
 * @returns {[number, number, number]} [x, y, z]
 */
export function getXYZFromPixelZ(pixelX, pixelY, z, scheme, tileSize = 256) {
  if (scheme === "tms") {
    pixelY = tileSize * (1 << z) - pixelY;
  }

  return [Math.floor(pixelX / tileSize), Math.floor(pixelY / tileSize), z];
}

/**
 * Get longitude, latitude from z/x/y (Default: XYZ)
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @param {number} z Zoom level
 * @param {"center"|"topLeft"|"bottomRight"} position Tile position: "center", "topLeft", or "bottomRight"
 * @param {"xyz"|"tms"} scheme Tile scheme
 * @returns {[number, number]} [longitude, latitude] in EPSG:4326
 */
export function getLonLatFromXYZ(x, y, z, position, scheme) {
  const maxTile = 1 << z;

  if (scheme === "tms") {
    y = maxTile - 1 - y;
  }

  if (position === "center") {
    x += 0.5;
    y += 0.5;
  } else if (position === "bottomRight") {
    x += 1;
    y += 1;
  }

  return [
    360 * (x / maxTile - 0.5),
    (360 * Math.atan(Math.exp(Math.PI * (1 - (2 * y) / maxTile)))) / Math.PI -
      90,
  ];
}

/**
 * Calculate zoom levels
 * @param {[number, number, number, number]} bbox Bounding box in EPSG:4326
 * @param {number} width Width of image
 * @param {number} height Height of image
 * @param {256|512} tileSize Tile size (Default: 256)
 * @returns {Promise<{ minZoom: number, maxZoom: number }>} Zoom levels
 */
export async function calculateZoomLevels(bbox, width, height, tileSize = 256) {
  const [xRes, yRes] = await calculateResolution({
    bbox: bbox,
    width: width,
    height: height,
  });

  const res = xRes <= yRes ? xRes : yRes;

  const maxZoom = limitValue(
    Math.round(Math.log2(MAX_GM / tileSize / res)),
    0,
    25,
  );

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
 * Get pyramid tile ranges
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @param {"xyz"|"tms"} scheme Tile scheme
 * @param {number} deltaZ Delta zoom
 * @returns {{ x: [number, number], y: [number, number] }}
 */
export function getPyramidTileRanges(z, x, y, scheme, deltaZ) {
  const factor = 1 << deltaZ;

  const minX = x * factor;
  const maxX = (x + 1) * factor - 1;
  const minY = y * factor;
  const maxY = (y + 1) * factor - 1;

  if (scheme === "tms") {
    const maxTileIndex = (1 << (z + deltaZ)) - 1;

    return {
      x: [minX, maxX],
      y: [maxTileIndex - maxY, maxTileIndex - minY],
    };
  }

  return {
    x: [minX, maxX],
    y: [minY, maxY],
  };
}

/**
 * Calculate sizes
 * @param {number} z Zoom level
 * @param {[number, number, number, number]} bbox Bounding box in EPSG:4326
 * @param {256|512} tileSize Tile size (Default: 512)
 * @returns {{width: number, height: number}} Sizes
 */
export function calculateSizes(z, bbox, tileSize = 512) {
  const [minX, minY] = lonLat4326ToXY3857(bbox[0], bbox[1]);
  const [maxX, maxY] = lonLat4326ToXY3857(bbox[2], bbox[3]);

  const resolution = MAX_GM / (tileSize * Math.pow(2, z));

  return {
    width: Math.round((maxX - minX) / resolution),
    height: Math.round((maxY - minY) / resolution),
  };
}

/**
 * Get grids for specific bbox with optional lat/lon steps (Keeps both head and tail residuals)
 * @param {[number, number, number, number]} bbox [minLon, minLat, maxLon, maxLat]
 * @param {number} lonStep Step for longitude
 * @param {number} latStep Step for latitude
 * @returns {[number, number, number, number][]}
 */
export function splitBBox(bbox, lonStep, latStep) {
  const result = [];

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

  const lonRanges = lonStep
    ? splitStep(bbox.minLon, bbox.maxLon, lonStep)
    : [[bbox.minLon, bbox.maxLon]];
  const latRanges = latStep
    ? splitStep(bbox.minLat, bbox.maxLat, latStep)
    : [[bbox.minLat, bbox.maxLat]];

  for (const [lonStart, lonEnd] of lonRanges) {
    for (const [latStart, latEnd] of latRanges) {
      result.push([lonStart, latStart, lonEnd, latEnd]);
    }
  }

  return result;
}

/**
 * Get grids for specific coverage with optional lat/lon steps (Keeps both head and tail residuals)
 * @param {{ zoom: number, bbox: [number, number, number, number] }} coverage
 * @param {number} lonStep Step for longitude
 * @param {number} latStep Step for latitude
 * @returns {{ zoom: number, bbox: [number, number, number, number] }[]}
 */
export function getGridsFromCoverage(coverage, lonStep, latStep) {
  return splitBBox(coverage.bbox, lonStep, latStep).map((bbox) => ({
    bbox: bbox,
    zoom: coverage.zoom,
  }));
}

/**
 * Get tile bounds
 * @param {{ coverages: { zoom: number, bbox: [number, number, number, number], circle: { radius: number, center: [number, number] }}[], scheme: "xyz"|"tms", limitedBBox: [number, number, number, number], minZoom: number, maxZoom: number, bbox: [number, number, number, number] }} options Option object
 * @returns {{ targetCoverages: { zoom: number, bbox: [number, number, number, number] }[], realBBox: [number, number, number, number], bbox: [number, number, number, number], total: number, tileBounds: { realBBox: [number, number, number, number], total: number, z: number, x: [number, number], y: [number, number] }[] }}
 */
export function getTileBounds(options) {
  let totalTile = 0;
  let realBBox;
  const targetCoverages = [];
  let tileBounds = [];

  if (options.coverages) {
    tileBounds = options.coverages.map((coverage, idx) => {
      let bbox = coverage.circle
        ? getBBoxFromCircle(coverage.circle.center, coverage.circle.radius)
        : coverage.bbox;

      if (options.limitedBBox) {
        const intersecBBox = getIntersectBBox(bbox, options.limitedBBox);
        if (intersecBBox) {
          bbox = intersecBBox;
        }
      }

      const [xMin, yMin, xMax, yMax] = getTilesFromBBox(
        bbox,
        coverage.zoom,
        options.scheme,
      );

      const _bbox = getBBoxFromTiles(
        xMin,
        yMin,
        xMax,
        yMax,
        coverage.zoom,
        options.scheme,
      );

      realBBox = idx === 0 ? _bbox : getCoverBBox(realBBox, _bbox);

      const _total = (xMax - xMin + 1) * (yMax - yMin + 1);

      totalTile += _total;

      targetCoverages.push({
        zoom: coverage.zoom,
        bbox: bbox,
      });

      return {
        realBBox: _bbox,
        bbox: bbox,
        total: _total,
        z: coverage.zoom,
        x: [xMin, xMax],
        y: [yMin, yMax],
      };
    });
  } else {
    for (let zoom = options.minZoom; zoom <= options.maxZoom; zoom++) {
      let bbox = options.bbox;

      if (options.limitedBBox) {
        const intersecBBox = getIntersectBBox(bbox, options.limitedBBox);
        if (intersecBBox) {
          bbox = intersecBBox;
        }
      }

      const [xMin, yMin, xMax, yMax] = getTilesFromBBox(
        bbox,
        zoom,
        options.scheme,
      );

      const _bbox = getBBoxFromTiles(
        xMin,
        yMin,
        xMax,
        yMax,
        zoom,
        options.scheme,
      );

      realBBox =
        zoom === options.minZoom ? _bbox : getCoverBBox(realBBox, _bbox);

      const _total = (xMax - xMin + 1) * (yMax - yMin + 1);

      totalTile += _total;

      targetCoverages.push({
        zoom: zoom,
        bbox: bbox,
      });

      tileBounds.push({
        realBBox: _bbox,
        bbox: bbox,
        total: _total,
        z: zoom,
        x: [xMin, xMax],
        y: [yMin, yMax],
      });
    }
  }

  return {
    targetCoverages: targetCoverages,
    realBBox: realBBox,
    total: totalTile,
    tileBounds: tileBounds,
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
export function getBBoxFromTiles(xMin, yMin, xMax, yMax, z, scheme) {
  let [lonMin, latMax] = getLonLatFromXYZ(xMin, yMin, z, "topLeft", scheme);
  let [lonMax, latMin] = getLonLatFromXYZ(xMax, yMax, z, "bottomRight", scheme);

  if (lonMin > lonMax) {
    [lonMin, lonMax] = [lonMax, lonMin];
  }

  if (latMin > latMax) {
    [latMin, latMax] = [latMax, latMin];
  }

  return [lonMin, latMin, lonMax, latMax];
}

/**
 * Convert bbox to tiles
 * @param {[number, number, number, number]} bbox Bounding box [lonMin, latMin, lonMax, latMax] in EPSG:4326
 * @param {number} z Zoom level
 * @param {"xyz"|"tms"} scheme Tile scheme
 * @returns {[number, number, number, number]} Tiles [minX, maxX, minY, maxY]
 */
export function getTilesFromBBox(bbox, z, scheme) {
  let [xMin, yMin] = getXYZFromLonLatZ(bbox[0], bbox[3], z, scheme);
  let [xMax, yMax] = getXYZFromLonLatZ(bbox[2], bbox[1], z, scheme);

  if (xMin > xMax) {
    [xMin, xMax] = [xMax, xMin];
  }

  if (yMin > yMax) {
    [yMin, yMax] = [yMax, yMin];
  }

  return [xMin, yMin, xMax, yMax];
}

/**
 * Get real bbox
 * @param {[number, number, number, number]} bbox Bounding box [lonMin, latMin, lonMax, latMax] in EPSG:4326
 * @param {number} z Zoom level
 * @param {"xyz"|"tms"} scheme Tile scheme
 * @returns {[number, number, number, number]} Bounding box [lonMin, latMin, lonMax, latMax] in EPSG:4326
 */
export function getRealBBox(bbox, z, scheme) {
  let [xMin, yMin, xMax, yMax] = getTilesFromBBox(bbox, z, scheme);

  return getBBoxFromTiles(xMin, yMin, xMax, yMax, z, scheme);
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
  let bbox;

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

    bbox[0] = limitValue(bbox[0], -180, 180);
    bbox[2] = limitValue(bbox[2], -180, 180);
    bbox[1] = limitValue(bbox[1], -85.051129, 85.051129);
    bbox[3] = limitValue(bbox[3], -85.051129, 85.051129);
  }

  return bbox;
}

/**
 * Get center from bbox
 * @param {[number, number, number, number]} bbox Bounding box in the format [minLon, minLat, maxLon, maxLat]
 * @param {number} z Zoom level
 * @returns {[number, number] | [number, number, number]} Center
 */
export function getCenterFromBBox(bbox, z) {
  if (z === undefined) {
    return [
      (bbox.bounds[0] + bbox.bounds[2]) / 2,
      (bbox.bounds[1] + bbox.bounds[3]) / 2,
    ];
  } else {
    return [
      (bbox.bounds[0] + bbox.bounds[2]) / 2,
      (bbox.bounds[1] + bbox.bounds[3]) / 2,
      z,
    ];
  }
}

/**
 * Get bounding box intersect
 * @param {[number, number, number, number]} bbox1 Bounding box 1 in the format [minLon, minLat, maxLon, maxLat]
 * @param {[number, number, number, number]} bbox2 Bounding box 2 in the format [minLon, minLat, maxLon, maxLat]
 * @returns {[number, number, number, number]} Intersect bounding box in the format [minLon, minLat, maxLon, maxLat]
 */
export function getIntersectBBox(bbox1, bbox2) {
  const minLon = bbox1[0] > bbox2[0] ? bbox1[0] : bbox2[0];
  const minLat = bbox1[1] > bbox2[1] ? bbox1[1] : bbox2[1];
  const maxLon = bbox1[2] < bbox2[2] ? bbox1[2] : bbox2[2];
  const maxLat = bbox1[3] < bbox2[3] ? bbox1[3] : bbox2[3];

  if (minLon >= maxLon || minLat >= maxLat) {
    return;
  }

  return [minLon, minLat, maxLon, maxLat];
}

/**
 * Get bounding box cover
 * @param {[number, number, number, number]} bbox1 Bounding box 1 in the format [minLon, minLat, maxLon, maxLat]
 * @param {[number, number, number, number]} bbox2 Bounding box 2 in the format [minLon, minLat, maxLon, maxLat]
 * @returns {[number, number, number, number]} Cover bounding box in the format [minLon, minLat, maxLon, maxLat]
 */
export function getCoverBBox(bbox1, bbox2) {
  const minLon = bbox1[0] < bbox2[0] ? bbox1[0] : bbox2[0];
  const minLat = bbox1[1] < bbox2[1] ? bbox1[1] : bbox2[1];
  const maxLon = bbox1[2] > bbox2[2] ? bbox1[2] : bbox2[2];
  const maxLat = bbox1[3] > bbox2[3] ? bbox1[3] : bbox2[3];

  return [minLon, minLat, maxLon, maxLat];
}

/**
 * Convert zoom to scale
 * @param {number} zoom Zoom
 * @param {number} ppi Pixel per inch (Default: 96)
 * @param {256|512} tileSize Tile size (Default: 256)
 * @returns {number} Scale
 */
export function zoomToScale(zoom, ppi = 96, tileSize = 256) {
  return (ppi * (MAX_GM / tileSize / Math.pow(2, zoom))) / 0.0254;
}

/**
 * Convert scale to zoom
 * @param {number} scale Scale
 * @param {number} ppi Pixel per inch (Default: 96)
 * @param {256|512} tileSize Tile size (Default: 256)
 * @returns {number} zoom
 */
export function scaleToZoom(scale, ppi = 96, tileSize = 256) {
  return Math.log2(ppi * (MAX_GM / tileSize / scale / 0.0254));
}

/**
 * Convert scale to zoom
 * @param {number} scale Scale
 * @param {number} ppi Pixel per inch (Default: 96)
 * @param {256|512} tileSize Tile size (Default: 256)
 * @returns {number} zoom
 */
export function getTileFromPixelsZ(scale, ppi = 96, tileSize = 256) {
  return Math.log2(ppi * (MAX_GM / tileSize / scale / 0.0254));
}
