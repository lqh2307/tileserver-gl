"use strict";

import { max, min } from "./number.js";

/**
 * Ensure a ring of points is closed by checking if the first and last points are the same.
 * If they are not the same, append the first point to the end of the array to close the ring.
 * @param {number[][]} ring Array of points [x, y]
 * @returns {number[][]} The original ring if it's already closed, or a new array with the first point appended if it was not closed
 */
export function makeValidCloseRing(ring) {
  const first = ring[0];
  const last = ring[ring.length - 1];

  if (first[0] === last[0] && first[1] === last[1]) {
    return ring;
  }

  const newRing = ring.slice();
  newRing.push(first);

  return newRing;
}

/**
 * Calculate the area of a polygon defined by an array of rings (arrays of points).
 * Uses the shoelace formula to calculate the area of the outer ring (first array of points) and ignores holes.
 * @param {number[][][]} coords Array of rings, where each ring is an array of points [x, y]
 * @returns {number} The area of the polygon
 */
export function getPolygonArea(coords) {
  const ring = makeValidCloseRing(coords[0]);

  if (ring.length < 4) {
    return 0;
  }

  let area = 0;

  for (let i = 0; i < ring.length - 1; i++) {
    area += ring[i][0] * ring[i + 1][1] - ring[i + 1][0] * ring[i][1];
  }

  return Math.abs(area) * 0.5;
}

/**
 * Calculate the centroid of a polygon defined by an array of rings (arrays of points).
 * Uses the formula for polygon centroids, which accounts for the shape of the polygon.
 * Only considers the outer ring (first array of points) and ignores holes.
 * @param {number[][][]} coords Array of rings, where each ring is an array of points [x, y]
 * @returns {number[]} The centroid [x, y] of the polygon
 */
export function getPolygonCentroid(coords) {
  let area = 0;
  let x = 0;
  let y = 0;

  const ring = makeValidCloseRing(coords[0]);

  for (let i = 0; i < ring.length - 1; i++) {
    const f = ring[i][0] * ring[i + 1][1] - ring[i + 1][0] * ring[i][1];

    x += (ring[i][0] + ring[i + 1][0]) * f;
    y += (ring[i][1] + ring[i + 1][1]) * f;

    area += f;
  }

  area *= 0.5;

  if (!area) {
    return ring[0];
  }

  return [x / (6 * area), y / (6 * area)];
}

const geometryBBoxes = new WeakMap();

/**
 * Calculate a geometry bounding box without allocating an intermediate list
 * of every coordinate. The WeakMap also avoids repeating this work while a
 * feature is evaluated by multiple filters or output builders.
 * @param {object} geometry GeoJSON geometry
 * @returns {number[]} [minX, minY, maxX, maxY]
 */
export function getGeometryBBox(geometry) {
  if (!geometry || typeof geometry !== "object") {
    return;
  }

  const cached = geometryBBoxes.get(geometry);
  if (cached) {
    return cached;
  }

  let minX = Infinity;
  let minY = Infinity;
  let maxX = -Infinity;
  let maxY = -Infinity;

  const visit = (value) => {
    if (!Array.isArray(value)) {
      return;
    }

    if (
      value.length >= 2 &&
      typeof value[0] === "number" &&
      typeof value[1] === "number"
    ) {
      minX = min(minX, value[0]);
      minY = min(minY, value[1]);
      maxX = max(maxX, value[0]);
      maxY = max(maxY, value[1]);

      return;
    }

    for (const child of value) {
      visit(child);
    }
  };

  if (geometry.type === "GeometryCollection") {
    for (const child of geometry.geometries ?? []) {
      const bbox = getGeometryBBox(child);
      if (bbox) {
        minX = min(minX, bbox[0]);
        minY = min(minY, bbox[1]);
        maxX = max(maxX, bbox[2]);
        maxY = max(maxY, bbox[3]);
      }
    }
  } else {
    visit(geometry.coordinates);
  }

  if (minX === Infinity) {
    return;
  }

  const bbox = [minX, minY, maxX, maxY];

  geometryBBoxes.set(geometry, bbox);

  return bbox;
}
