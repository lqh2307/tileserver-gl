"use strict";

/**
 * Check if the error is a "Not Found" error
 * @param {Error} error Error object
 * @returns {boolean} True if the error is a "Not Found" error
 */
export function isErrorNotFound(error) {
  return error?.message?.includes("Not Found");
}
