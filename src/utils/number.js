"use strict";

const INTEGER_REGEX = /-?\d+/;
const FLOAT_REGEX = /-?\d+(\.\d+)?/;

/**
 * Clamp a number within min/max bounds.
 * @param {number} value Input value
 * @param {number} min Minimum bound
 * @param {number} max Maximum bound
 * @returns {number} Clamped value
 * @example
 * limitValue(12, 0, 10); // 10
 * limitValue(-2, 0, 10); // 0
 * limitValue(5, 0, 10); // 5
 */
export function limitValue(value, min, max) {
  if (min !== undefined && value < min) {
    value = min;
  }

  if (max !== undefined && value > max) {
    value = max;
  }

  return value;
}

/**
 * Get the maximum value in an array.
 * @param {number[]} values Input values
 * @returns {number} Max value, or undefined if empty
 * @example
 * maxValue([1, 7, 3]); // 7
 * maxValue([]); // undefined
 */
export function maxValue(values) {
  if (values?.length) {
    let value = values[0];

    for (let i = 1; i < values.length; i++) {
      if (value < values[i]) {
        value = values[i];
      }
    }

    return value;
  }
}

/**
 * Get the minimum value in an array.
 * @param {number[]} values Input values
 * @returns {number} Min value, or undefined if empty
 * @example
 * minValue([1, 7, 3]); // 1
 * minValue([]); // undefined
 */
export function minValue(values) {
  if (values?.length) {
    let value = values[0];

    for (let i = 1; i < values.length; i++) {
      if (value > values[i]) {
        value = values[i];
      }
    }

    return value;
  }
}

/**
 * Extract a number from a string.
 * @param {string} strNumber Input string
 * @param {boolean} isFloat If true, matches floats; otherwise integers
 * @param {number} defaultNumber Default if no match (default: 0)
 * @returns {number} Parsed value or default
 * @example
 * fixNumber("width: 42px"); // 42
 * fixNumber("scale: 1.25", true); // 1.25
 * fixNumber("none", true, 10); // 10
 */
export function fixNumber(strNumber, isFloat, defaultNumber) {
  const match = strNumber?.match(isFloat ? FLOAT_REGEX : INTEGER_REGEX);

  return match ? Number(match[0]) : (defaultNumber ?? 0);
}

/**
 * Normalize angle to the -180..180 range.
 * @param {number} deg Angle in degrees
 * @returns {number} Normalized angle
 * @example
 * normalize180(270); // -90
 * normalize180(-270); // 90
 */
export function normalize180(deg) {
  let d = deg % 360;
  if (d > 180) {
    d -= 360;
  } else if (d < -180) {
    d += 360;
  }

  return d;
}

/**
 * Normalize angle to the -360..360 range.
 * @param {number} deg Angle in degrees
 * @returns {number} Normalized angle
 * @example
 * normalize360(270); // 270
 * normalize360(-90); // 270
 */
export function normalize360(deg) {
  let d = deg % 360;
  if (d < 0) {
    d += 360;
  }

  return d;
}

/**
 * Convert an angle from degrees to radians.
 * @param {number} angle Angle in degrees
 * @returns {number} Angle in radians
 * @example
 * degToRad(180); // Math.PI
 */
export function degToRad(angle) {
  return (angle / 180) * Math.PI;
}

/**
 * Convert an angle from radians to degrees.
 * @param {number} angle Angle in radians
 * @returns {number} Angle in degrees
 * @example
 * radToDeg(Math.PI); // 180
 */
export function radToDeg(angle) {
  return (180 * angle) / Math.PI;
}

/**
 * Convert decimal degrees to a DMS object.
 * @param {number} deg Decimal degrees
 * @returns {{degree: number, minute: number, second: number}} DMS object (normalized to -180..180)
 * @example
 * convertDEGToDMS(105.5); // { degree: 105, minute: 30, second: 0 }
 * convertDEGToDMS(-181); // { degree: 179, minute: 0, second: 0 }
 */
export function convertDEGToDMS(deg) {
  const normalized = normalize180(deg % 360);

  const absolute = normalized > 0 ? normalized : -normalized;
  let degree = Math.floor(absolute);
  const minuteNotTruncated = (absolute - degree) * 60;
  let minute = Math.floor(minuteNotTruncated);
  let second = Math.round((minuteNotTruncated - minute) * 60);

  if (second === 60) {
    minute += 1;

    second = 0;
  }

  if (minute === 60) {
    degree += 1;

    minute = 0;
  }

  return {
    degree: normalized >= 0 ? degree : -degree,
    minute,
    second,
  };
}

/**
 * Convert decimal degrees to a formatted DMS string.
 * @param {number} deg Decimal degrees
 * @returns {string} Formatted DMS string
 * @example
 * convertDEGToDMSString(105.5); // "105° 30' 0\""
 * convertDEGToDMSString(-181); // "179° 0' 0\""
 */
export function convertDEGToDMSString(deg) {
  const normalized = normalize180(deg % 360);

  const absolute = normalized > 0 ? normalized : -normalized;
  let degree = Math.floor(absolute);
  const minuteNotTruncated = (absolute - degree) * 60;
  let minute = Math.floor(minuteNotTruncated);
  let second = Math.round((minuteNotTruncated - minute) * 60);

  if (second === 60) {
    minute += 1;

    second = 0;
  }

  if (minute === 60) {
    degree += 1;

    minute = 0;
  }

  return `${normalized >= 0 ? degree : -degree}° ${minute}' ${second}"`;
}

/**
 * Convert a DMS object to decimal degrees.
 * @param {{degree: number, minute: number, second: number}} dms DMS object
 * @returns {number} Decimal degrees (normalized to -180..180)
 * @example
 * convertDMSToDEG({ degree: 105, minute: 30, second: 0 }); // 105.5
 * convertDMSToDEG({ degree: -105, minute: 30, second: 0 }); // -105.5
 */
export function convertDMSToDEG(dms) {
  const absDeg = dms.degree > 0 ? dms.degree : -dms.degree;
  const decimal = absDeg + dms.minute / 60 + dms.second / 3600;
  const signed = dms.degree >= 0 ? decimal : -decimal;

  return normalize180(signed % 360);
}

/**
 * Create an array of numbers from start to end with a given step.
 * @param {number} start Start value
 * @param {number} end End value
 * @param {number} pointsPerSegment Number of points to generate (exclude start and end)
 * @returns {number[]} Array of numbers
 * @example
 * createRangeNumber(0, 10, 1); // [0, 5, 10]
 * createRangeNumber(0, 10); // [0, 10]
 */
export function createRangeNumber(start, end, pointsPerSegment) {
  const segmentCount = (pointsPerSegment ?? 0) + 1;
  const step = (end - start) / segmentCount;

  const points = new Array(segmentCount + 1);

  for (let i = 0; i <= segmentCount; i++) {
    points[i] = start + i * step;
  }

  return points;
}

/**
 * Return the maximum of two numbers, treating undefined as less than any number.
 * @param {number} a First number
 * @param {number} b Second number
 * @returns {number} Maximum of a and b, or the defined number if one is undefined
 * @example
 * max(3, 7); // 7
 * max(undefined, 7); // 7
 */
export function max(a, b) {
  if (a === undefined) {
    return b;
  }

  if (b === undefined) {
    return a;
  }

  return a > b ? a : b;
}

/**
 * Return the minimum of two numbers, treating undefined as greater than any number.
 * @param {number} a First number
 * @param {number} b Second number
 * @returns {number} Minimum of a and b, or the defined number if one is undefined
 * @example
 * min(3, 7); // 3
 * min(undefined, 7); // 7
 */
export function min(a, b) {
  if (a === undefined) {
    return b;
  }

  if (b === undefined) {
    return a;
  }

  return a < b ? a : b;
}

const DEFAULT_TOLERANCE = 1e-7;

/**
 * Round a decimal number to the specified number of fraction digits.
 * Integers are returned unchanged.
 * @param {number} value Input value
 * @param {number} digits Number of fraction digits to keep
 * @returns {number} Rounded number
 * @example
 * roundDecimal(4.135, 2); // 4.14
 * roundDecimal(4, 2); // 4
 */
export function roundDecimal(value, digits) {
  if (Number.isInteger(value)) {
    return value;
  }

  const factor = 10 ** max(0, Math.floor(digits ?? 0));

  return Math.round((value + Number.EPSILON) * factor) / factor;
}

/**
 * Round a number to the nearest multiple of the provided divisor.
 * @param {number} value Input value
 * @param {number} divisor Multiple step to round to
 * @returns {number} Nearest multiple of divisor
 * @example
 * roundToMultiple(4.13, 0.25); // 4.25
 * roundToMultiple(4.1, 0.25); // 4
 */
export function roundToMultiple(value, divisor) {
  if (!divisor) {
    return value;
  }

  const absDivisor = Math.abs(divisor);
  const rounded = Math.round(value / absDivisor) * absDivisor;
  const divisorDecimalLength = absDivisor.toString().split(".")[1]?.length ?? 0;

  return roundDecimal(rounded, divisorDecimalLength);
}

/**
 * Compare two numbers for near-equality within a specified tolerance.
 * @param {number} a First number
 * @param {number} b Second number
 * @param {number} tolerance Tolerance for comparison
 * @returns {boolean} True if the numbers are equal within the tolerance, false otherwise
 * @example
 * isSameNumber(0.1 + 0.2, 0.3); // true
 * isSameNumber(1, 1.1, 0.01); // false
 */
export function isSameNumber(a, b, tolerance) {
  if (a === undefined || b === undefined) {
    return a === b;
  }

  return Math.abs(a - b) < (tolerance ?? DEFAULT_TOLERANCE);
}
