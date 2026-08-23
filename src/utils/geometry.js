"use strict";

import { degToRad, max, min, radToDeg } from "./number.js";

/** Ensure a ring is closed without mutating the input. */
export function makeValidCloseRing(ring) {
  if (!Array.isArray(ring) || ring.length < 2) {
    return ring ?? [];
  }

  const first = ring[0];
  const last = ring[ring.length - 1];
  if (first[0] === last[0] && first[1] === last[1]) {
    return ring;
  }

  return [...ring, first];
}

export function getDistance(a, b) {
  return Math.hypot(b[0] - a[0], b[1] - a[1]);
}

export function getPerimeter(ring, isClosed = false) {
  if (!Array.isArray(ring) || ring.length < 2) {
    return 0;
  }

  const points = isClosed ? makeValidCloseRing(ring) : ring;
  let total = 0;
  for (let index = 1; index < points.length; index++) {
    total += getDistance(points[index - 1], points[index]);
  }
  return total;
}

export function getBoundingBox(points) {
  if (!Array.isArray(points) || !points.length) {
    return;
  }

  let minX = points[0][0];
  let minY = points[0][1];
  let maxX = minX;
  let maxY = minY;
  for (let index = 1; index < points.length; index++) {
    const point = points[index];
    minX = min(minX, point[0]);
    minY = min(minY, point[1]);
    maxX = max(maxX, point[0]);
    maxY = max(maxY, point[1]);
  }
  return [minX, minY, maxX, maxY];
}

function pointInRing(point, ring) {
  let inside = false;
  for (
    let index = 0, previous = ring.length - 1;
    index < ring.length;
    previous = index++
  ) {
    const current = ring[index];
    const prior = ring[previous];
    if (
      current[1] > point[1] !== prior[1] > point[1] &&
      point[0] <
        ((prior[0] - current[0]) * (point[1] - current[1])) /
          (prior[1] - current[1]) +
          current[0]
    ) {
      inside = !inside;
    }
  }
  return inside;
}

export function isPointInPolygon(point, coords) {
  if (!Array.isArray(coords) || !coords[0]?.length) {
    return false;
  }
  if (!pointInRing(point, coords[0])) {
    return false;
  }
  for (let index = 1; index < coords.length; index++) {
    if (pointInRing(point, coords[index])) {
      return false;
    }
  }
  return true;
}

function squaredSegmentDistance(point, start, end) {
  let x = start[0];
  let y = start[1];
  const dx = end[0] - x;
  const dy = end[1] - y;
  if (dx !== 0 || dy !== 0) {
    const t = ((point[0] - x) * dx + (point[1] - y) * dy) / (dx * dx + dy * dy);
    if (t > 1) {
      x = end[0];
      y = end[1];
    } else if (t > 0) {
      x += dx * t;
      y += dy * t;
    }
  }
  const offsetX = point[0] - x;
  const offsetY = point[1] - y;
  return offsetX * offsetX + offsetY * offsetY;
}

export function simplifyPoints(points, tolerance) {
  if (!Array.isArray(points) || points.length <= 2 || tolerance <= 0) {
    return points;
  }

  const squaredTolerance = tolerance * tolerance;
  const result = [points[0]];
  const simplify = (first, last) => {
    let largest = squaredTolerance;
    let split = -1;
    for (let index = first + 1; index < last; index++) {
      const distance = squaredSegmentDistance(
        points[index],
        points[first],
        points[last],
      );
      if (distance > largest) {
        largest = distance;
        split = index;
      }
    }
    if (split !== -1) {
      if (split - first > 1) {
        simplify(first, split);
      }
      result.push(points[split]);
      if (last - split > 1) {
        simplify(split, last);
      }
    }
  };
  simplify(0, points.length - 1);
  result.push(points[points.length - 1]);
  return result;
}

export function isClockwise(ring) {
  let sum = 0;
  for (let index = 0; index < ring.length - 1; index++) {
    sum +=
      (ring[index + 1][0] - ring[index][0]) *
      (ring[index + 1][1] + ring[index][1]);
  }
  return sum > 0;
}

export function getPolygonArea(coords) {
  const ring = makeValidCloseRing(coords?.[0]);
  if (ring.length < 4) {
    return 0;
  }
  let area = 0;
  for (let index = 0; index < ring.length - 1; index++) {
    area +=
      ring[index][0] * ring[index + 1][1] - ring[index + 1][0] * ring[index][1];
  }
  return Math.abs(area) * 0.5;
}

export function getPolygonCentroid(coords) {
  const ring = makeValidCloseRing(coords?.[0]);
  if (!ring.length) {
    return;
  }
  let area = 0;
  let x = 0;
  let y = 0;
  for (let index = 0; index < ring.length - 1; index++) {
    const factor =
      ring[index][0] * ring[index + 1][1] - ring[index + 1][0] * ring[index][1];
    x += (ring[index][0] + ring[index + 1][0]) * factor;
    y += (ring[index][1] + ring[index + 1][1]) * factor;
    area += factor;
  }
  area *= 0.5;
  return area ? [x / (6 * area), y / (6 * area)] : ring[0];
}

const geometryBBoxes = new WeakMap();

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

const EARTH_RADIUS = 6371008.8;

export function getHaversineDistance(from, to, units = "meters") {
  const dLat = degToRad(to[1] - from[1]);
  const dLng = degToRad(to[0] - from[0]);
  const lat1 = degToRad(from[1]);
  const lat2 = degToRad(to[1]);
  const sinLat = Math.sin(dLat / 2);
  const sinLng = Math.sin(dLng / 2);
  const a = sinLat * sinLat + sinLng * sinLng * Math.cos(lat1) * Math.cos(lat2);
  const distance =
    EARTH_RADIUS * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return units === "km" || units === "kilometers" ? distance / 1000 : distance;
}

export function getBearing(from, to) {
  const lon1 = degToRad(from[0]);
  const lon2 = degToRad(to[0]);
  const lat1 = degToRad(from[1]);
  const lat2 = degToRad(to[1]);
  return radToDeg(
    Math.atan2(
      Math.sin(lon2 - lon1) * Math.cos(lat2),
      Math.cos(lat1) * Math.sin(lat2) -
        Math.sin(lat1) * Math.cos(lat2) * Math.cos(lon2 - lon1),
    ),
  );
}

export function getDestination(origin, distance, bearing) {
  const lon1 = degToRad(origin[0]);
  const lat1 = degToRad(origin[1]);
  const bearingRad = degToRad(bearing);
  const radians = distance / EARTH_RADIUS;
  const lat2 = Math.asin(
    Math.sin(lat1) * Math.cos(radians) +
      Math.cos(lat1) * Math.sin(radians) * Math.cos(bearingRad),
  );
  const lon2 =
    lon1 +
    Math.atan2(
      Math.sin(bearingRad) * Math.sin(radians) * Math.cos(lat1),
      Math.cos(radians) - Math.sin(lat1) * Math.sin(lat2),
    );
  return [radToDeg(lon2), radToDeg(lat2)];
}

export function getMidpoint(a, b) {
  return [(a[0] + b[0]) / 2, (a[1] + b[1]) / 2];
}

export function getLineIntersection(p1, p2, p3, p4) {
  const denom =
    (p4[1] - p3[1]) * (p2[0] - p1[0]) - (p4[0] - p3[0]) * (p2[1] - p1[1]);
  if (denom === 0) {
    return;
  }
  const ua =
    ((p4[0] - p3[0]) * (p1[1] - p3[1]) - (p4[1] - p3[1]) * (p1[0] - p3[0])) /
    denom;
  const ub =
    ((p2[0] - p1[0]) * (p1[1] - p3[1]) - (p2[1] - p1[1]) * (p1[0] - p3[0])) /
    denom;
  return ua >= 0 && ua <= 1 && ub >= 0 && ub <= 1
    ? [p1[0] + ua * (p2[0] - p1[0]), p1[1] + ua * (p2[1] - p1[1])]
    : undefined;
}

export function getNearestPointOnSegment(point, start, end) {
  const dx = end[0] - start[0];
  const dy = end[1] - start[1];
  if (dx === 0 && dy === 0) {
    return start;
  }
  const t =
    ((point[0] - start[0]) * dx + (point[1] - start[1]) * dy) /
    (dx * dx + dy * dy);
  const clamped = max(0, min(1, t));
  return [start[0] + clamped * dx, start[1] + clamped * dy];
}

export function isPointOnSegment(point, start, end, epsilon = 1e-9) {
  const cross =
    (point[1] - start[1]) * (end[0] - start[0]) -
    (point[0] - start[0]) * (end[1] - start[1]);
  if (Math.abs(cross) > epsilon) {
    return false;
  }
  const dot =
    (point[0] - start[0]) * (end[0] - start[0]) +
    (point[1] - start[1]) * (end[1] - start[1]);
  if (dot < 0) {
    return false;
  }
  const lengthSquared = (end[0] - start[0]) ** 2 + (end[1] - start[1]) ** 2;
  return dot <= lengthSquared + epsilon;
}

export function getConvexHull(points) {
  if (!Array.isArray(points) || points.length <= 2) {
    return points?.slice() ?? [];
  }
  const sorted = points.slice().sort((a, b) => {
    return a[0] === b[0] ? a[1] - b[1] : a[0] - b[0];
  });
  const cross = (origin, a, b) => {
    return (
      (a[0] - origin[0]) * (b[1] - origin[1]) -
      (a[1] - origin[1]) * (b[0] - origin[0])
    );
  };
  const lower = [];
  for (const point of sorted) {
    while (lower.length >= 2 && cross(lower.at(-2), lower.at(-1), point) <= 0) {
      lower.pop();
    }
    lower.push(point);
  }
  const upper = [];
  for (let index = sorted.length - 1; index >= 0; index--) {
    const point = sorted[index];
    while (upper.length >= 2 && cross(upper.at(-2), upper.at(-1), point) <= 0) {
      upper.pop();
    }
    upper.push(point);
  }
  lower.pop();
  upper.pop();
  return lower.concat(upper);
}

export function getCirclePolygon(center, radius, steps = 64) {
  const coordinates = [];
  const count = max(3, Math.floor(steps));
  for (let index = 0; index < count; index++) {
    coordinates.push(getDestination(center, radius, (index * 360) / count));
  }
  coordinates.push(coordinates[0]);
  return coordinates;
}

export function intersectLineWithLine(line1, line2) {
  const intersections = [];
  for (let first = 0; first < line1.length - 1; first++) {
    for (let second = 0; second < line2.length - 1; second++) {
      const point = getLineIntersection(
        line1[first],
        line1[first + 1],
        line2[second],
        line2[second + 1],
      );
      if (
        point &&
        !intersections.some((item) => {
          return getDistance(item, point) < 1e-7;
        })
      ) {
        intersections.push(point);
      }
    }
  }
  return intersections;
}

export function intersectLineWithPolygon(line, polygon) {
  const intersections = [];
  for (const ring of polygon) {
    for (const point of intersectLineWithLine(line, makeValidCloseRing(ring))) {
      if (
        !intersections.some((item) => {
          return getDistance(item, point) < 1e-7;
        })
      ) {
        intersections.push(point);
      }
    }
  }
  return intersections;
}

export function intersectPolygonWithPolygon(first, second) {
  const intersections = [];
  for (const firstRing of first) {
    for (const secondRing of second) {
      for (const point of intersectLineWithLine(
        makeValidCloseRing(firstRing),
        makeValidCloseRing(secondRing),
      )) {
        if (
          !intersections.some((item) => {
            return getDistance(item, point) < 1e-7;
          })
        ) {
          intersections.push(point);
        }
      }
    }
  }
  return intersections;
}

export function booleanIntersects(first, second) {
  if (first.type === "Point" && second.type === "Point") {
    return getDistance(first.coordinates, second.coordinates) < 1e-9;
  }
  if (first.type === "Point" && second.type === "LineString") {
    return second.coordinates.some((point, index) => {
      return (
        index > 0 &&
        isPointOnSegment(
          first.coordinates,
          second.coordinates[index - 1],
          point,
        )
      );
    });
  }
  if (first.type === "LineString" && second.type === "Point") {
    return booleanIntersects(second, first);
  }
  if (first.type === "Point" && second.type === "Polygon") {
    return isPointInPolygon(first.coordinates, second.coordinates);
  }
  if (first.type === "Polygon" && second.type === "Point") {
    return booleanIntersects(second, first);
  }
  if (first.type === "LineString" && second.type === "LineString") {
    return (
      intersectLineWithLine(first.coordinates, second.coordinates).length > 0
    );
  }
  if (first.type === "LineString" && second.type === "Polygon") {
    return (
      intersectLineWithPolygon(first.coordinates, second.coordinates).length >
        0 || isPointInPolygon(first.coordinates[0], second.coordinates)
    );
  }
  if (first.type === "Polygon" && second.type === "LineString") {
    return booleanIntersects(second, first);
  }
  if (first.type === "Polygon" && second.type === "Polygon") {
    return (
      intersectPolygonWithPolygon(first.coordinates, second.coordinates)
        .length > 0 ||
      isPointInPolygon(first.coordinates[0]?.[0], second.coordinates) ||
      isPointInPolygon(second.coordinates[0]?.[0], first.coordinates)
    );
  }
  return false;
}
