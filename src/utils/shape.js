"use strict";

export const DEFAULT_SHAPE_WIDTH = 200;
export const DEFAULT_SHAPE_HEIGHT = 200;
export const DEFAULT_TABLE_COL_WIDTH = 120;
export const DEFAULT_TABLE_ROW_HEIGHT = 75;
export const DEFAULT_TABLE_ROWS = 3;
export const DEFAULT_TABLE_COLS = 3;
export const DEFAULT_MOTION_DURATION_MS = 400;
export const NATURAL_MAP_ZOOM = 14;
export const MIN_STAGE_SCALE = 2 ** -14;
export const MAX_STAGE_SCALE = 2 ** 8;
export const STAGE_SCALE_FACTOR = 2 ** 0.4;
export const DEFAULT_CENTER_LNG = 105.82234262427113;
export const DEFAULT_CENTER_LAT = 21.056155258978833;

const LINE_DASHES = {
  none: [0, 0],
  solid: [5, 0],
  dashed: [10, 5],
  longDashed: [10, 10],
  dotted: [2, 5],
  dashedDot: [10, 5, 2, 5],
};

/** Clone a flat point array. */
export function clonePoints(points) {
  return points.slice();
}

/** Reverse point order in a flat `[x1, y1, ...]` array. */
export function reversePoints(points) {
  const result = new Array(points.length);
  let targetIndex = 0;

  for (let index = points.length - 2; index >= 0; index -= 2) {
    result[targetIndex++] = points[index];
    result[targetIndex++] = points[index + 1];
  }

  return result;
}

/** Pick points by point index rather than flat-array index. */
export function pickPoints(points, indices) {
  const result = new Array(indices.length * 2);
  let targetIndex = 0;

  for (const pointIndex of indices) {
    const index = pointIndex * 2;
    result[targetIndex++] = points[index];
    result[targetIndex++] = points[index + 1];
  }

  return result;
}

/** Sample a Bezier curve using the De Casteljau algorithm. */
export function createBezierCurvePoints(points, pointsPerSegment) {
  const count = points.length / 2;
  if (count < 3) {
    return points;
  }

  const stepsPerSegment = pointsPerSegment ?? 5 * points.length;
  const result = new Array((stepsPerSegment + 1) * 2);
  const xValues = new Array(count);
  const yValues = new Array(count);

  for (let index = 0; index <= stepsPerSegment; index++) {
    const ratio = index / stepsPerSegment;
    const inverseRatio = 1 - ratio;

    for (let pointIndex = 0; pointIndex < count; pointIndex++) {
      xValues[pointIndex] = points[pointIndex * 2];
      yValues[pointIndex] = points[pointIndex * 2 + 1];
    }

    for (let level = 1; level < count; level++) {
      for (let pointIndex = 0; pointIndex < count - level; pointIndex++) {
        xValues[pointIndex] =
          xValues[pointIndex] * inverseRatio + xValues[pointIndex + 1] * ratio;
        yValues[pointIndex] =
          yValues[pointIndex] * inverseRatio + yValues[pointIndex + 1] * ratio;
      }
    }

    result[index * 2] = xValues[0];
    result[index * 2 + 1] = yValues[0];
  }

  return result;
}

/** Sample a Catmull-Rom spline. */
export function createCatmullRomCurvePoints(points, pointsPerSegment) {
  const count = points.length / 2;
  if (count < 3) {
    return points;
  }

  const steps = pointsPerSegment || 10;
  const segmentCount = count - 1;
  const result = [];

  for (let index = 0; index < segmentCount; index++) {
    const p0x = index === 0 ? points[0] : points[(index - 1) * 2];
    const p0y = index === 0 ? points[1] : points[(index - 1) * 2 + 1];
    const p1x = points[index * 2];
    const p1y = points[index * 2 + 1];
    const p2x = points[(index + 1) * 2];
    const p2y = points[(index + 1) * 2 + 1];
    const p3x = index + 2 < count ? points[(index + 2) * 2] : p2x;
    const p3y = index + 2 < count ? points[(index + 2) * 2 + 1] : p2y;

    const cx = p1x;
    const bx = 0.5 * (-p0x + p2x);
    const ax = 0.5 * (2 * p0x - 5 * p1x + 4 * p2x - p3x);
    const dx = 0.5 * (-p0x + 3 * p1x - 3 * p2x + p3x);
    const cy = p1y;
    const by = 0.5 * (-p0y + p2y);
    const ay = 0.5 * (2 * p0y - 5 * p1y + 4 * p2y - p3y);
    const dy = 0.5 * (-p0y + 3 * p1y - 3 * p2y + p3y);
    const startStep = index === 0 ? 0 : 1;

    for (let step = startStep; step <= steps; step++) {
      const ratio = step / steps;
      const ratio2 = ratio * ratio;
      const ratio3 = ratio2 * ratio;

      result.push(
        cx + bx * ratio + ax * ratio2 + dx * ratio3,
        cy + by * ratio + ay * ratio2 + dy * ratio3,
      );
    }
  }

  return result;
}

/** Sample half-circle arcs between consecutive points. */
export function createHalfCirclePoints(points, pointsPerSegment, lower) {
  const count = points.length / 2;
  if (count < 2) {
    return points;
  }

  const steps = pointsPerSegment ?? 20;
  const result = [points[0], points[1]];

  for (let index = 0; index < count - 1; index++) {
    const flatIndex = index * 2;
    const centerX = (points[flatIndex] + points[flatIndex + 2]) * 0.5;
    const centerY = (points[flatIndex + 1] + points[flatIndex + 3]) * 0.5;
    const deltaX = points[flatIndex + 2] - points[flatIndex];
    const deltaY = points[flatIndex + 3] - points[flatIndex + 1];
    const radius = Math.hypot(deltaX, deltaY) * 0.5;
    const isLower = lower !== undefined ? lower : index % 2 === 1;
    const startAngle = Math.atan2(
      points[flatIndex + 1] - centerY,
      points[flatIndex] - centerX,
    );
    const angleStep = (isLower ? -1 : 1) * (Math.PI / steps);

    for (let step = 1; step < steps; step++) {
      const angle = startAngle + angleStep * step;
      result.push(
        centerX + Math.cos(angle) * radius,
        centerY + Math.sin(angle) * radius,
      );
    }

    result.push(points[flatIndex + 2], points[flatIndex + 3]);
  }

  return result;
}

/** Sample half-ellipse arcs from endpoint/midpoint triplets. */
export function createHalfEllipsePoints(points, pointsPerSegment, lower) {
  if (points.length < 4) {
    return points;
  }

  const steps = pointsPerSegment ?? 20;
  const result = [];
  let flatIndex = 0;
  let segmentIndex = 0;

  while (flatIndex < points.length - 2) {
    const x0 = points[flatIndex];
    const y0 = points[flatIndex + 1];
    const hasMiddle = points.length - flatIndex >= 6;
    const x2 = hasMiddle ? points[flatIndex + 4] : points[flatIndex + 2];
    const y2 = hasMiddle ? points[flatIndex + 5] : points[flatIndex + 3];
    const centerX = (x0 + x2) * 0.5;
    const centerY = (y0 + y2) * 0.5;
    const isLower = lower !== undefined ? lower : segmentIndex % 2 === 1;

    if (!hasMiddle) {
      const radius = Math.hypot(x0 - centerX, y0 - centerY);
      const startAngle = Math.atan2(y0 - centerY, x0 - centerX);
      const angleStep = (isLower ? -1 : 1) * (Math.PI / steps);

      for (let step = 0; step <= steps; step++) {
        const angle = startAngle + angleStep * step;
        result.push(
          centerX + radius * Math.cos(angle),
          centerY + radius * Math.sin(angle),
        );
      }
    } else {
      const x1 = points[flatIndex + 2];
      const y1 = points[flatIndex + 3];
      const vectorX = x0 - centerX;
      const vectorY = y0 - centerY;
      const middleX = x1 - centerX;
      const middleY = y1 - centerY;
      const majorRadius = Math.hypot(vectorX, vectorY);
      const majorAxisX = vectorX / majorRadius;
      const majorAxisY = vectorY / majorRadius;
      const minorAxisX = -majorAxisY;
      const minorAxisY = majorAxisX;
      let middleAngle =
        Math.atan2(middleY, middleX) - Math.atan2(vectorY, vectorX);

      if (middleAngle < 0) {
        middleAngle += Math.PI * 2;
      }
      if (middleAngle > Math.PI) {
        middleAngle = 2 * Math.PI - middleAngle;
      }

      const minorProjection = middleX * minorAxisX + middleY * minorAxisY;
      const minorRadius = Math.abs(minorProjection) / Math.sin(middleAngle);
      const direction = isLower ? -1 : 1;

      for (let step = 0; step <= steps; step++) {
        const ratio = (step * Math.PI) / steps;
        const localX = majorRadius * Math.cos(ratio);
        const localY = minorRadius * Math.sin(ratio) * direction;

        result.push(
          centerX + localX * majorAxisX + localY * minorAxisX,
          centerY + localX * majorAxisY + localY * minorAxisY,
        );
      }
    }

    flatIndex += hasMiddle ? 4 : 2;
    segmentIndex += 1;
  }

  return result;
}

/** Clone line objects and their point arrays. */
export function cloneLines(items) {
  return items.map((item) => {
    return {
      ...item,
      points: item.points?.slice(),
    };
  });
}

/** Return a dash pattern for a named line style. */
export function createLineDash(style) {
  return LINE_DASHES[style];
}

/** Create a canvas-compatible font style. */
export function createFontStyle(italic, bold) {
  if (italic && bold) {
    return "italic bold";
  }
  if (italic) {
    return "italic";
  }
  if (bold) {
    return "bold";
  }

  return "normal";
}
