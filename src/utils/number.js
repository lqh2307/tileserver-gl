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

/**
 * Max value
 * @param {number[]} values Values
 * @returns {number}
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
 * Min value
 * @param {number[]} values Values
 * @returns {number}
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
