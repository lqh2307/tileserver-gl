"use strict";

import { MAX_LON, TOTAL_DEGREES } from "./spatial.js";

const INTEGER_REGEX = /-?\d+/;
const FLOAT_REGEX = /-?\d+(\.\d+)?/;
const DMS_NUMBER_REGEX = /[-+]?\d+(?:\.\d+)?/g;
const DMS_HEMISPHERE_REGEX = /[NSEW]/i;
const NUMBER_PATTERN = /[-+]?(?:\d*\.\d+|\d+\.?)(?:[eE][-+]?\d+)?/g;

export const DEFAULT_TOLERANCE = 1e-7;

/**
 * Convert a string-like value into a list of finite numbers.
 * @param {unknown} value Input containing numbers
 * @param {number[]} def Fallback when no number can be parsed
 * @param {number} digit Maximum fractional digits retained without rounding
 * @returns {number[]}
 */
export function parseNumberList(value, def, digit = 3) {
  const matches = String(value ?? "").match(NUMBER_PATTERN);
  if (!matches?.length) {
    return def;
  }

  return matches.map((match) => {
    return parseNumber(match, undefined, digit);
  });
}

/**
 * Safely extract a finite number from a string-like value.
 * @param {unknown} value Input value
 * @param {number} def Fallback when conversion is invalid
 * @param {number} digit Maximum fractional digits retained without rounding
 * @returns {number}
 */
export function parseNumber(value, def, digit = 3) {
  const matches = String(value ?? "").match(NUMBER_PATTERN);
  if (!matches?.length) {
    return def;
  }

  const factor = 10 ** digit;
  const parsed = Number(matches.join(""));

  return Number.isFinite(parsed) ? Math.trunc(parsed * factor) / factor : def;
}

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
 * maxs([1, 7, 3]); // 7
 * maxs([]); // undefined
 */
export function maxs(values) {
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
 * mins([1, 7, 3]); // 1
 * mins([]); // undefined
 */
export function mins(values) {
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
  let d = deg % TOTAL_DEGREES;
  if (d > MAX_LON) {
    d -= TOTAL_DEGREES;
  } else if (d < -MAX_LON) {
    d += TOTAL_DEGREES;
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
  let d = deg % TOTAL_DEGREES;
  if (d < 0) {
    d += TOTAL_DEGREES;
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
  return (angle / MAX_LON) * Math.PI;
}

/**
 * Convert an angle from radians to degrees.
 * @param {number} angle Angle in radians
 * @returns {number} Angle in degrees
 * @example
 * radToDeg(Math.PI); // 180
 */
export function radToDeg(angle) {
  return (MAX_LON * angle) / Math.PI;
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
  const normalized = normalize180(deg % TOTAL_DEGREES);

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
 * Convert decimal degrees to a DMS object with an optional hemisphere.
 * @param {number} deg Decimal degrees
 * @param {boolean} isLon True for longitude, false for latitude; omit for signed degree
 * @returns {{degree: number, minute: number, second: number, hemisphere?: "N"|"S"|"E"|"W"}}
 */
export function convertDEGToDMSH(deg, isLon) {
  const normalized = normalize180(deg % TOTAL_DEGREES);
  const dms = convertDEGToDMS(normalized);

  if (isLon === undefined) {
    return dms;
  }

  return {
    ...dms,
    degree: Math.abs(dms.degree),
    hemisphere: isLon
      ? normalized >= 0
        ? "E"
        : "W"
      : normalized >= 0
        ? "N"
        : "S",
  };
}

/**
 * Format decimal degrees as DD, DDM, DMS, or DMSH.
 * @param {number} value Coordinate value
 * @param {boolean} isLatitude True for latitude
 * @param {"DD"|"DDM"|"DMS"|"DMSH"} format Output format
 * @returns {string}
 */
export function convertDEGToDMSHString(value, isLatitude, format) {
  if (format === "DD") {
    return `${String(parseNumber(value))}\u00b0`;
  }

  const absolute = Math.abs(value);
  let degree = Math.floor(absolute);

  if (format === "DDM") {
    let decimalMinute = Math.round((absolute - degree) * 60 * 1000) / 1000;

    if (decimalMinute === 60) {
      decimalMinute = 0;
      degree += 1;
    }

    return `${value < 0 ? "-" : ""}${degree}\u00b0${String(parseNumber(decimalMinute))}'`;
  }

  const dms = convertDEGToDMSH(
    value,
    format === "DMSH" ? !isLatitude : undefined,
  );

  return `${format !== "DMSH" && dms.degree < 0 ? "-" : ""}${Math.abs(dms.degree)}\u00b0${dms.minute ? `${dms.minute}'` : ""}${dms.second ? `${dms.second}\"` : ""}${dms.hemisphere ?? ""}`;
}

/**
 * Convert a DMSH string to decimal degrees.
 * @param {string} dmshString DMSH string
 * @returns {number}
 */
export function convertDMSHStringToDEG(dmshString) {
  const values =
    dmshString?.match(DMS_NUMBER_REGEX)?.map((value) => {
      return Number(value);
    }) ?? [];

  if (!values.length) {
    return 0;
  }

  const [degree, minute = 0, second = 0] = values;

  return convertDMSHToDEG({
    degree,
    minute,
    second,
    hemisphere: dmshString.match(DMS_HEMISPHERE_REGEX)?.[0]?.toUpperCase(),
  });
}

/**
 * Convert a DMSH object to decimal degrees.
 * @param {{degree: number, minute: number, second: number, hemisphere?: "N"|"S"|"E"|"W"}} dms DMSH object
 * @returns {number}
 */
export function convertDMSHToDEG(dms) {
  const absDeg = Math.abs(dms.degree);
  const decimal = absDeg + dms.minute / 60 + dms.second / 3600;
  const signed = dms.hemisphere
    ? dms.hemisphere === "W" || dms.hemisphere === "S"
      ? -decimal
      : decimal
    : dms.degree >= 0
      ? decimal
      : -decimal;

  return normalize180(signed % TOTAL_DEGREES);
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
  const normalized = normalize180(deg % TOTAL_DEGREES);

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

  return normalize180(signed % TOTAL_DEGREES);
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
  if (typeof start !== "object" || start === null) {
    const segmentCount = (pointsPerSegment ?? 0) + 1;
    const step = (end - start) / segmentCount;
    const points = new Array(segmentCount + 1);

    for (let i = 0; i <= segmentCount; i++) {
      points[i] = start + i * step;
    }

    return points;
  }

  const option = start;
  const rangeStart = option.start;
  const rangeEnd = option.end;
  const interiorPointCount = option.pointsPerSegment ?? 0;

  if (rangeEnd < rangeStart) {
    return [];
  }

  if (rangeStart === rangeEnd) {
    return option.excludeStart || option.excludeEnd ? [] : [rangeStart];
  }

  if (option.step !== undefined) {
    if (option.step <= 0) {
      return [];
    }

    const origin = option.origin ?? 0;
    const values = option.excludeStart ? [] : [rangeStart];

    for (
      let value = Math.ceil((rangeStart - origin) / option.step) * option.step;
      value <= rangeEnd - origin + option.step * DEFAULT_TOLERANCE;
      value += option.step
    ) {
      const roundedValue = roundDecimal(
        origin + roundToMultiple(value, option.step),
        12,
      );

      if (roundedValue > rangeStart && roundedValue < rangeEnd) {
        values.push(roundedValue);
      }
    }

    if (!option.excludeEnd) {
      values.push(rangeEnd);
    }

    return values;
  }

  const segmentCount = interiorPointCount + 1;
  const step = (rangeEnd - rangeStart) / segmentCount;
  const points = option.excludeStart ? [] : [rangeStart];

  for (let i = 1; i < segmentCount; i++) {
    points.push(rangeStart + i * step);
  }

  if (!option.excludeEnd) {
    points.push(rangeEnd);
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
