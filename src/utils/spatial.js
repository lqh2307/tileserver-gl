"use strict";

import { limitValue, max, min } from "./number.js";
import proj4 from "proj4";

/**
 * https://epsg.io/4756
 */
proj4.defs(
  "EPSG:4756",
  "+proj=longlat +ellps=WGS84 +towgs84=-191.90441429,-39.30318279,-111.45032835,-0.00928836,0.01975479,-0.00427372,0.252906278 +no_defs +type=crs",
);
/**
 * https://epsg.io/3405
 */
proj4.defs(
  "EPSG:3405",
  "+proj=utm +zone=48 +ellps=WGS84 +towgs84=-191.90441429,-39.30318279,-111.45032835,-0.00928836,0.01975479,-0.00427372,0.252906278 +units=m +no_defs +type=crs",
);
/**
 * https://epsg.io/3406
 */
proj4.defs(
  "EPSG:3406",
  "+proj=utm +zone=49 +ellps=WGS84 +towgs84=-191.90441429,-39.30318279,-111.45032835,-0.00928836,0.01975479,-0.00427372,0.252906278 +units=m +no_defs +type=crs",
);

export const TOTAL_DEGREES = 360;

/** Maximum longitude in degrees for EPSG:4326 normalization. */
export const MAX_LON = 180;
/** Maximum latitude in degrees for EPSG:4326 normalization. */
export const MAX_LAT = 90;

/**  */
const GEOGRAPHIC_REGEX = /\+proj=(?:longlat|latlong)\b/;
/** Radius of the Earth in meters for spherical calculations. */
const SPHERICAL_RADIUS = 6378137.0;
/** */
const TWO_PI = 2 * Math.PI;
/** Circumference of the Earth in meters for spherical calculations. */
const MAX_GM = TWO_PI * SPHERICAL_RADIUS;
/** Maximum latitude supported by Web Mercator (EPSG:3857) in degrees. */
const MAX_CAL_LAT = 85.051129;

/** Minimum zoom level for Web Mercator (EPSG:3857). */
export const MIN_ZOOM = 0;
/** Maximum zoom level for Web Mercator (EPSG:3857). */
export const MAX_ZOOM = 25;

/** Default tile size in pixels for Web Mercator (EPSG:3857). */
export const DEFAULT_TILE_SIZE = 512;

/** Default pixels per inch (PPI) for screen resolution. */
export const DEFAULT_PPI = 96;

/** Check whether an SRS uses geographic longitude/latitude coordinates. */
export function isGeographicSRS(srs) {
  if (GEOGRAPHIC_REGEX.test(srs)) {
    return true;
  }

  const definition = proj4.defs(srs);

  return (
    definition?.projName === "longlat" || definition?.projName === "latlong"
  );
}

/**
 * Convert coordinates from EPSG:4326 (lon, lat) to EPSG:3857 (x, y in meters)
 * @param {number} lon Longitude in degrees
 * @param {number} lat Latitude in degrees
 * @returns {[number, number]} Web Mercator x, y in meters
 */
export function lonLat4326ToXY3857(lon, lat) {
  return [
    limitValue(lon, -MAX_LON, MAX_LON) * (Math.PI / MAX_LON) * SPHERICAL_RADIUS,
    Math.log(
      Math.tan(
        (Math.PI * (limitValue(lat, -MAX_CAL_LAT, MAX_CAL_LAT) + MAX_LAT)) /
          TOTAL_DEGREES,
      ),
    ) * SPHERICAL_RADIUS,
  ];
}

/**
 * Convert coordinates from EPSG:3857 (x, y in meters) to EPSG:4326 (lon, lat in degrees)
 * @param {number} x X in meters (Web Mercator)
 * @param {number} y Y in meters (Web Mercator)
 * @returns {[number, number]} Longitude and latitude in degrees
 */
export function xy3857ToLonLat4326(x, y) {
  return [
    limitValue((x / SPHERICAL_RADIUS) * (MAX_LON / Math.PI), -MAX_LON, MAX_LON),
    limitValue(
      Math.atan(Math.sinh(y / SPHERICAL_RADIUS)) * (MAX_LON / Math.PI),
      -MAX_CAL_LAT,
      MAX_CAL_LAT,
    ),
  ];
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
  const maxTile = 1 << z;

  let x = (0.5 + limitValue(lon, -MAX_LON, MAX_LON) / TOTAL_DEGREES) * maxTile;
  let y =
    (0.5 -
      Math.log(
        Math.tan(
          (Math.PI * (limitValue(lat, -MAX_CAL_LAT, MAX_CAL_LAT) + MAX_LAT)) /
            TOTAL_DEGREES,
        ),
      ) /
        TWO_PI) *
    maxTile;

  if (scheme === "tms") {
    y = maxTile - y;
  }

  return [
    limitValue(Math.floor(x), 0, maxTile - 1),
    limitValue(Math.floor(y), 0, maxTile - 1),
    z,
  ];
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
export function getXYZFromPixelZ(pixelX, pixelY, z, scheme, tileSize) {
  tileSize = tileSize || DEFAULT_TILE_SIZE;

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
    TOTAL_DEGREES * (x / maxTile - 0.5),
    (TOTAL_DEGREES * Math.atan(Math.exp(Math.PI * (1 - (2 * y) / maxTile)))) /
      Math.PI -
      MAX_LAT,
  ];
}

/**
 * Get tile bounds from z/x/y (Default: XYZ) in EPSG:4326
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @param {number} z Zoom level
 * @param {"xyz"|"tms"} scheme Tile scheme
 * @returns {[number, number, number, number]} [minLon, minLat, maxLon, maxLat] in EPSG:4326
 */
export function getTileBounds4326(x, y, z, scheme) {
  const [minLon, maxLat] = getLonLatFromXYZ(x, y, z, "topLeft", scheme);
  const [maxLon, minLat] = getLonLatFromXYZ(x, y, z, "bottomRight", scheme);

  return [minLon, minLat, maxLon, maxLat];
}

/**
 * Get tile bounds from z/x/y (Default: XYZ) in EPSG:3857
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @param {number} z Zoom level
 * @param {"xyz"|"tms"} scheme Tile scheme
 * @returns {[number, number, number, number]} [minLon, minLat, maxLon, maxLat] in EPSG:3857
 */
export function getTileBounds3857(x, y, z, scheme) {
  const [minLon, maxLat] = getLonLatFromXYZ(x, y, z, "topLeft", scheme);
  const [maxLon, minLat] = getLonLatFromXYZ(x, y, z, "bottomRight", scheme);

  return [
    ...lonLat4326ToXY3857(minLon, minLat),
    ...lonLat4326ToXY3857(maxLon, maxLat),
  ];
}

/**
 * Calculate maxzoom
 * @param {[number, number, number, number]} bbox Bounding box in EPSG:4326
 * @param {number} width Width of image
 * @param {number} height Height of image
 * @param {256|512} tileSize Tile size (Default: 256)
 * @returns {number} Max zoom
 */
export function calculateMaxZoom(bbox, width, height, tileSize = 256) {
  const [minX, minY] = lonLat4326ToXY3857(bbox[0], bbox[1]);
  const [maxX, maxY] = lonLat4326ToXY3857(bbox[2], bbox[3]);
  const xRes = (maxX - minX) / width;
  const yRes = (maxY - minY) / height;

  return limitValue(
    Math.round(Math.log2(MAX_GM / tileSize / min(xRes, yRes))),
    MIN_ZOOM,
    MAX_ZOOM,
  );
}

/**
 * Create a reusable transform from WGS84 lon/lat to image pixels.
 * @param {{bounds?: number[], center?: number[], zoom?: number, scale?: number, size: {width: number, height: number}, tileSize?: number, ppi?: number}} option Transform options
 * @returns {{transform: (coordinate: {lng: number, lat: number}) => {x: number, y: number}}}
 */
export function createLonLat4326ToPixelTransform(option) {
  let topLeft;
  let bottomRight;

  if (option.center) {
    const tileSize = option.tileSize || DEFAULT_TILE_SIZE;
    const zoom = option.zoom ?? scaleToZoom(option.scale, option.ppi, tileSize);
    const resolution = MAX_GM / (tileSize * Math.pow(2, zoom));
    const widthM = option.size.width * resolution;
    const heightM = option.size.height * resolution;
    const center = lonLat4326ToXY3857(option.center[0], option.center[1]);

    topLeft = [center[0] - widthM / 2, center[1] + heightM / 2];
    bottomRight = [center[0] + widthM / 2, center[1] - heightM / 2];
  } else {
    topLeft = lonLat4326ToXY3857(option.bounds[0], option.bounds[3]);
    bottomRight = lonLat4326ToXY3857(option.bounds[2], option.bounds[1]);
  }

  return {
    transform: (coordinate) => {
      const point = lonLat4326ToXY3857(coordinate.lng, coordinate.lat);

      return {
        x:
          ((point[0] - topLeft[0]) / (bottomRight[0] - topLeft[0])) *
          option.size.width,
        y:
          ((topLeft[1] - point[1]) / (topLeft[1] - bottomRight[1])) *
          option.size.height,
      };
    },
  };
}

/** Convert WGS84 lon/lat to image pixels. */
export function lonLat4326ToPixel(option) {
  return createLonLat4326ToPixelTransform(option).transform({
    lng: option.lng,
    lat: option.lat,
  });
}

/**
 * Create a reusable transform from image pixels to WGS84 lon/lat.
 * @param {{bounds?: number[], center?: number[], zoom?: number, scale?: number, size: {width: number, height: number}, tileSize?: number, ppi?: number}} option Transform options
 * @returns {{transform: (point: {x: number, y: number}) => {lng: number, lat: number}}}
 */
export function createPixelToLonLat4326Transform(option) {
  let topLeft;
  let bottomRight;

  if (option.center) {
    const tileSize = option.tileSize || DEFAULT_TILE_SIZE;
    const zoom = option.zoom ?? scaleToZoom(option.scale, option.ppi, tileSize);
    const resolution = MAX_GM / (tileSize * Math.pow(2, zoom));
    const widthM = option.size.width * resolution;
    const heightM = option.size.height * resolution;
    const center = lonLat4326ToXY3857(option.center[0], option.center[1]);

    topLeft = [center[0] - widthM / 2, center[1] + heightM / 2];
    bottomRight = [center[0] + widthM / 2, center[1] - heightM / 2];
  } else {
    topLeft = lonLat4326ToXY3857(option.bounds[0], option.bounds[3]);
    bottomRight = lonLat4326ToXY3857(option.bounds[2], option.bounds[1]);
  }

  return {
    transform: (point) => {
      const coordinate = xy3857ToLonLat4326(
        topLeft[0] +
          (point.x / option.size.width) * (bottomRight[0] - topLeft[0]),
        topLeft[1] -
          (point.y / option.size.height) * (topLeft[1] - bottomRight[1]),
      );

      return {
        lng: coordinate[0],
        lat: coordinate[1],
      };
    },
  };
}

/** Convert image pixels to WGS84 lon/lat. */
export function pixelToLonLat4326(option) {
  return createPixelToLonLat4326Transform(option).transform({
    x: option.x,
    y: option.y,
  });
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
 * Calculate pixel size for bounds at a zoom level or map scale.
 * @param {{bounds: number[], zoom?: number, scale?: number, tileSize?: number, ppi?: number}} option Options
 * @returns {{width: number, height: number}}
 */
export function calculateSize(option) {
  const tileSize = option.tileSize || DEFAULT_TILE_SIZE;

  const zoom = option.zoom ?? scaleToZoom(option.scale, option.ppi, tileSize);

  const [minX, minY] = lonLat4326ToXY3857(option.bounds[0], option.bounds[1]);
  const [maxX, maxY] = lonLat4326ToXY3857(option.bounds[2], option.bounds[3]);
  const resolution = MAX_GM / (tileSize * Math.pow(2, zoom));

  return {
    width: Math.round((maxX - minX) / resolution),
    height: Math.round((maxY - minY) / resolution),
  };
}

/**
 * Calculate WGS84 bounds for an image size around a center point.
 * @param {{center: number[], size: {width: number, height: number}, zoom?: number, scale?: number, tileSize?: number, ppi?: number}} option Options
 * @returns {number[]}
 */
export function calculateBBox(option) {
  const tileSize = option.tileSize || DEFAULT_TILE_SIZE;

  const zoom = option.zoom ?? scaleToZoom(option.scale, option.ppi, tileSize);
  const resolution = MAX_GM / (tileSize * Math.pow(2, zoom));

  const widthM = option.size.width * resolution;
  const heightM = option.size.height * resolution;

  const center = lonLat4326ToXY3857(option.center[0], option.center[1]);

  const bottomLeft = xy3857ToLonLat4326(
    center[0] - widthM / 2,
    center[1] - heightM / 2,
  );
  const topRight = xy3857ToLonLat4326(
    center[0] + widthM / 2,
    center[1] + heightM / 2,
  );

  return [
    min(bottomLeft[0], topRight[0]),
    min(bottomLeft[1], topRight[1]),
    max(bottomLeft[0], topRight[0]),
    max(bottomLeft[1], topRight[1]),
  ];
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

  const minLon = Array.isArray(bbox) ? bbox[0] : bbox.minLon;
  const minLat = Array.isArray(bbox) ? bbox[1] : bbox.minLat;
  const maxLon = Array.isArray(bbox) ? bbox[2] : bbox.maxLon;
  const maxLat = Array.isArray(bbox) ? bbox[3] : bbox.maxLat;
  const lonRanges = lonStep
    ? splitStep(minLon, maxLon, lonStep)
    : [[minLon, maxLon]];
  const latRanges = latStep
    ? splitStep(minLat, maxLat, latStep)
    : [[minLat, maxLat]];

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
  return splitBBox(coverage.bbox, lonStep, latStep).map((bbox) => {
    return {
      bbox,
      zoom: coverage.zoom,
    };
  });
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
    for (const coverage of options.coverages) {
      let bbox = coverage.circle
        ? getBBoxFromCircle(coverage.circle.center, coverage.circle.radius)
        : coverage.bbox;

      if (options.limitedBBox) {
        const intersecBBox = getIntersectBBox(bbox, options.limitedBBox);
        if (intersecBBox) {
          bbox = intersecBBox;
        } else {
          continue;
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

      realBBox = realBBox ? getCoverBBox(realBBox, _bbox) : _bbox;

      const _total = (xMax - xMin + 1) * (yMax - yMin + 1);

      totalTile += _total;

      targetCoverages.push({
        zoom: coverage.zoom,
        bbox,
      });

      tileBounds.push({
        realBBox: _bbox,
        bbox,
        total: _total,
        z: coverage.zoom,
        x: [xMin, xMax],
        y: [yMin, yMax],
      });
    }
  } else {
    let bbox = options.bbox;

    if (options.limitedBBox) {
      bbox = getIntersectBBox(bbox, options.limitedBBox);

      if (!bbox) {
        return {
          targetCoverages,
          realBBox,
          total: totalTile,
          tileBounds,
        };
      }
    }

    for (let zoom = options.minZoom; zoom <= options.maxZoom; zoom++) {
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

      realBBox = realBBox ? getCoverBBox(realBBox, _bbox) : _bbox;

      const _total = (xMax - xMin + 1) * (yMax - yMin + 1);

      totalTile += _total;

      targetCoverages.push({
        zoom,
        bbox,
      });

      tileBounds.push({
        realBBox: _bbox,
        bbox,
        total: _total,
        z: zoom,
        x: [xMin, xMax],
        y: [yMin, yMax],
      });
    }
  }

  return {
    targetCoverages,
    realBBox,
    total: totalTile,
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
export function getBBoxFromPoints(points) {
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

    bbox[0] = limitValue(bbox[0], -MAX_LON, MAX_LON);
    bbox[2] = limitValue(bbox[2], -MAX_LON, MAX_LON);
    bbox[1] = limitValue(bbox[1], -MAX_CAL_LAT, MAX_CAL_LAT);
    bbox[3] = limitValue(bbox[3], -MAX_CAL_LAT, MAX_CAL_LAT);
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
    return [(bbox[0] + bbox[2]) / 2, (bbox[1] + bbox[3]) / 2];
  } else {
    return [(bbox[0] + bbox[2]) / 2, (bbox[1] + bbox[3]) / 2, z];
  }
}

/**
 * Get intersection of two bboxes.
 * When they do not intersect, returns [0, 0, 0, 0].
 * @param {number[]} bbox1 [minLon, minLat, maxLon, maxLat]
 * @param {number[]} bbox2 [minLon, minLat, maxLon, maxLat]
 * @returns {number[]} Intersection bbox
 */
export function getIntersectBBox(bbox1, bbox2) {
  const minLon = max(bbox1[0], bbox2[0]);
  const minLat = max(bbox1[1], bbox2[1]);
  const maxLon = min(bbox1[2], bbox2[2]);
  const maxLat = min(bbox1[3], bbox2[3]);

  if (minLon >= maxLon || minLat >= maxLat) {
    return;
  }

  return [minLon, minLat, maxLon, maxLat];
}

/**
 * Check if two bboxes intersect (works for any coordinate system).
 * @param {number[]} bbox1 [minLon, minLat, maxLon, maxLat]
 * @param {number[]} bbox2 [minLon, minLat, maxLon, maxLat]
 * @returns {boolean} True if they intersect
 */
export function isIntersectBBoxs(bbox1, bbox2) {
  if (
    max(bbox1[0], bbox1[2]) <= min(bbox2[0], bbox2[2]) ||
    min(bbox1[0], bbox1[2]) >= max(bbox2[0], bbox2[2]) ||
    max(bbox1[1], bbox1[3]) <= min(bbox2[1], bbox2[3]) ||
    min(bbox1[1], bbox1[3]) >= max(bbox2[1], bbox2[3])
  ) {
    return false;
  }

  return true;
}

/**
 * Check if a point is inside a bbox (inclusive).
 * @param {number[]} bbox Bounds in [minLon, minLat, maxLon, maxLat]
 * @param {{ lng: number, lat: number }} coordinate { lng, lat }
 * @returns {boolean} True if inside
 */
export function isIntersectBBoxPoint(bbox, coordinate) {
  return (
    coordinate.lng >= min(bbox[0], bbox[2]) &&
    coordinate.lng <= max(bbox[0], bbox[2]) &&
    coordinate.lat >= min(bbox[1], bbox[3]) &&
    coordinate.lat <= max(bbox[1], bbox[3])
  );
}

/**
 * Get bounding box cover
 * @param {[number, number, number, number]} bbox1 Bounding box 1 in the format [minLon, minLat, maxLon, maxLat]
 * @param {[number, number, number, number]} bbox2 Bounding box 2 in the format [minLon, minLat, maxLon, maxLat]
 * @returns {[number, number, number, number]} Cover bounding box in the format [minLon, minLat, maxLon, maxLat]
 */
export function getCoverBBox(bbox1, bbox2) {
  const aMinX = bbox1[0] < bbox1[2] ? bbox1[0] : bbox1[2];
  const aMaxX = bbox1[0] > bbox1[2] ? bbox1[0] : bbox1[2];
  const aMinY = bbox1[1] < bbox1[3] ? bbox1[1] : bbox1[3];
  const aMaxY = bbox1[1] > bbox1[3] ? bbox1[1] : bbox1[3];

  const bMinX = bbox2[0] < bbox2[2] ? bbox2[0] : bbox2[2];
  const bMaxX = bbox2[0] > bbox2[2] ? bbox2[0] : bbox2[2];
  const bMinY = bbox2[1] < bbox2[3] ? bbox2[1] : bbox2[3];
  const bMaxY = bbox2[1] > bbox2[3] ? bbox2[1] : bbox2[3];

  return [
    aMinX < bMinX ? aMinX : bMinX,
    aMinY < bMinY ? aMinY : bMinY,
    aMaxX > bMaxX ? aMaxX : bMaxX,
    aMaxY > bMaxY ? aMaxY : bMaxY,
  ];
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

/**
 * Transform a point between any two coordinate reference systems.
 * Accepts EPSG codes (e.g. `"EPSG:4326"`, `"EPSG:3857"`) or proj4 definition strings.
 * @param {{ dstSRS: string, srcSRS: string, point: number[] }} option Options for transformation
 * @returns {number[]} Transformed [x, y]
 */
export function transformPointSRS(option) {
  if (option.dstSRS === option.srcSRS) {
    return option.point;
  }

  return proj4(option.srcSRS, option.dstSRS, option.point);
}

/**
 * Transform a bounding box between coordinate reference systems.
 * All four corners are transformed and normalized back to `[minX, minY, maxX, maxY]`.
 * @param {{ dstSRS: string, srcSRS: string, bounds: number[] }} option Options for transformation
 * @returns {number[]} Transformed bbox
 */
export function transformBBoxSRS(option) {
  const { srcSRS, dstSRS, bounds } = option;

  if (dstSRS === srcSRS) {
    return bounds;
  }

  const transform = proj4(srcSRS, dstSRS).forward;

  let minX = Infinity;
  let minY = Infinity;
  let maxX = -Infinity;
  let maxY = -Infinity;

  // Reuse one initialized projection for every corner and build the
  // destination envelope without allocating intermediate point arrays.
  for (let xIndex = 0; xIndex < 2; xIndex++) {
    const x = bounds[xIndex * 2];

    for (let yIndex = 0; yIndex < 2; yIndex++) {
      const y = bounds[yIndex * 2 + 1];

      const point = transform([x, y]);

      minX = min(minX, point[0]);
      minY = min(minY, point[1]);
      maxX = max(maxX, point[0]);
      maxY = max(maxY, point[1]);
    }
  }

  return [minX, minY, maxX, maxY];
}
