"use strict";

/**
 * Limit value
 * @param {number} value Value
 * @param {number} min Min
 * @param {number} max Max
 * @returns {number}
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
