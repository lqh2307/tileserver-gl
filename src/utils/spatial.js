"use strict";

import { calculateResolution } from "./image.js";
import { deepClone } from "./util.js";

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
    lon * (Math.PI / 180) * 6378137.0,
    Math.log(Math.tan(Math.PI / 4 + lat * (Math.PI / 360))) * 6378137.0,
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
 * @param {"xyz"|"tms"} scheme Tile scheme to output (Default: XYZ)
 * @param {256|512} tileSize Tile size
 * @returns {[number, number, number]} Tile indices [x, y, z]
 */
export function getXYZFromLonLatZ(lon, lat, z, scheme, tileSize = 256) {
  const size = tileSize * (1 << z);
  const bc = size / 360;
  const cc = size / 2 / Math.PI;
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
    (zc - cc * Math.log(Math.tan(Math.PI / 4 + lat * (Math.PI / 360)))) /
    tileSize
  );

  if (scheme === "tms") {
    y = maxTileIndex - y;
  }

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
 * Get longitude, latitude from z/x/y (Default: XYZ)
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @param {number} z Zoom level
 * @param {"center"|"topLeft"|"bottomRight"} position Tile position: "center", "topLeft", or "bottomRight"
 * @param {"xyz"|"tms"} scheme Tile scheme
 * @param {256|512} tileSize Tile size
 * @returns {[number, number]} [longitude, latitude] in EPSG:4326
 */
export function getLonLatFromXYZ(x, y, z, position, scheme, tileSize = 256) {
  const size = tileSize * (1 << z);
  const bc = size / 360;
  const cc = size / 2 / Math.PI;
  const zc = size / 2;

  let px = x * tileSize;
  let py = y * tileSize;

  if (position === "center") {
    px += tileSize / 2;
    py += tileSize / 2;
  } else if (position === "bottomRight") {
    px += tileSize;
    py += tileSize;
  }

  if (scheme === "tms") {
    py = size - py;
  }

  return [
    (px - zc) / bc,
    (180 / Math.PI) * (2 * Math.atan(Math.exp((zc - py) / cc)) - Math.PI / 2),
  ];
}

/**
 * Calculate zoom levels
 * @param {[number, number, number, number]} bbox Bounding box in EPSG:4326
 * @param {number} width Width of image
 * @param {number} height Height of image
 * @param {256|512} tileSize Tile size
 * @returns {Promise<{ minZoom: number, maxZoom: number }>} Zoom levels
 */
export async function calculateZoomLevels(bbox, width, height, tileSize = 256) {
  const [xRes, yRes] = await calculateResolution({
    bbox: bbox,
    width: width,
    height: height,
  });

  const res = xRes <= yRes ? xRes : yRes;

  let maxZoom = Math.round(
    Math.log2((2 * Math.PI * 6378137.0) / tileSize / res)
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
 * @param {number} tileScale Tile scale
 * @param {256|512} tileSize Tile size
 * @returns {{width: number, height: number}} Sizes
 */
export function calculateSizes(z, bbox, tileScale, tileSize) {
  const [minX, minY] = lonLat4326ToXY3857(bbox[0], bbox[1]);
  const [maxX, maxY] = lonLat4326ToXY3857(bbox[2], bbox[3]);

  const resolution = (2 * Math.PI * 6378137.0) / (tileSize * Math.pow(2, z));

  return {
    width: Math.round(tileScale * ((maxX - minX) / resolution)),
    height: Math.round(tileScale * ((maxY - minY) / resolution)),
  };
}

/**
 * Get grids for specific coverage with optional lat/lon steps (Keeps both head and tail residuals)
 * @param {{ zoom: number, bbox: [number, number, number, number] }} coverage
 * @param {number} lonStep Step for longitude
 * @param {number} latStep Step for latitude
 * @returns {{ zoom: number, bbox: [number, number, number, number] }[]}
 */
export function getGridsFromCoverage(coverage, lonStep, latStep) {
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

  const lonRanges = lonStep
    ? splitStep(coverage.bbox[0], coverage.bbox[2], lonStep)
    : [[coverage.bbox[0], coverage.bbox[2]]];
  const latRanges = latStep
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
 * Get tile bounds
 * @param {{ coverages: { zoom: number, bbox: [number, number, number, number], circle: { radius: number, center: [number, number] }}[], scheme: "xyz"|"tms", tileSize: 256|512, limitedBBox: [number, number, number, number], minZoom: number, maxZoom: number, bbox: [number, number, number, number] }} options Option object
 * @returns {{ targetCoverages: { zoom: number, bbox: [number, number, number, number] }[], realBBox: [number, number, number, number], bbox: [number, number, number, number], total: number, tileBounds: { realBBox: [number, number, number, number], total: number, z: number, x: [number, number], y: [number, number] }[] }}
 */
export function getTileBounds(options) {
  let totalTile = 0;
  let realBBox;
  const targetCoverages = [];
  let tileBounds = [];

  if (options.coverages) {
    tileBounds = options.coverages.map((coverage, idx) => {
      const bbox = coverage.circle
        ? getBBoxFromCircle(coverage.circle.center, coverage.circle.radius)
        : deepClone(coverage.bbox);

      if (options.limitedBBox) {
        if (bbox[0] < options.limitedBBox[0]) {
          bbox[0] = options.limitedBBox[0];
        }

        if (bbox[1] < options.limitedBBox[1]) {
          bbox[1] = options.limitedBBox[1];
        }

        if (bbox[2] > options.limitedBBox[2]) {
          bbox[2] = options.limitedBBox[2];
        }

        if (bbox[3] > options.limitedBBox[3]) {
          bbox[3] = options.limitedBBox[3];
        }
      }

      const [xMin, yMin, xMax, yMax] = getTilesFromBBox(
        bbox,
        coverage.zoom,
        options.scheme,
        options.tileSize
      );

      const _bbox = getBBoxFromTiles(
        xMin,
        yMin,
        xMax,
        yMax,
        coverage.zoom,
        options.scheme,
        options.tileSize
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
      const bbox = deepClone(options.bbox);

      if (options.limitedBBox) {
        if (bbox[0] < options.limitedBBox[0]) {
          bbox[0] = options.limitedBBox[0];
        }

        if (bbox[1] < options.limitedBBox[1]) {
          bbox[1] = options.limitedBBox[1];
        }

        if (bbox[2] > options.limitedBBox[2]) {
          bbox[2] = options.limitedBBox[2];
        }

        if (bbox[3] > options.limitedBBox[3]) {
          bbox[3] = options.limitedBBox[3];
        }
      }

      const [xMin, yMin, xMax, yMax] = getTilesFromBBox(
        bbox,
        zoom,
        options.scheme,
        options.tileSize
      );

      const _bbox = getBBoxFromTiles(
        xMin,
        yMin,
        xMax,
        yMax,
        zoom,
        options.scheme,
        options.tileSize
      );

      if (zoom === options.minZoom) {
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
 * @param {256|512} tileSize Tile size
 * @returns {[number, number, number, number]} Bounding box [lonMin, latMin, lonMax, latMax] in EPSG:4326
 */
export function getBBoxFromTiles(xMin, yMin, xMax, yMax, z, scheme, tileSize) {
  let [lonMin, latMax] = getLonLatFromXYZ(
    xMin,
    yMin,
    z,
    "topLeft",
    scheme,
    tileSize
  );
  let [lonMax, latMin] = getLonLatFromXYZ(
    xMax,
    yMax,
    z,
    "bottomRight",
    scheme,
    tileSize
  );

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
 * @param {256|512} tileSize Tile size
 * @returns {[number, number, number, number]} Tiles [minX, maxX, minY, maxY]
 */
export function getTilesFromBBox(bbox, z, scheme, tileSize) {
  let [xMin, yMin] = getXYZFromLonLatZ(bbox[0], bbox[3], z, scheme, tileSize);
  let [xMax, yMax] = getXYZFromLonLatZ(bbox[2], bbox[1], z, scheme, tileSize);

  if (xMin > xMax) {
    [xMin, xMax] = [xMax, xMin];
  }

  if (yMin > yMax) {
    [yMin, yMax] = [yMax, yMin];
  }

  return [xMin, yMin, xMax, yMax];
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
  const minLon = bbox1[0] >= bbox2[0] ? bbox1[0] : bbox2[0];
  const minLat = bbox1[1] >= bbox2[1] ? bbox1[1] : bbox2[1];
  const maxLon = bbox1[2] <= bbox2[2] ? bbox1[2] : bbox2[2];
  const maxLat = bbox1[3] <= bbox2[3] ? bbox1[3] : bbox2[3];

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
