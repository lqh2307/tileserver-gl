"use strict";

/**
 * Create a debounced function and a matching cancel function.
 * @param {Function} func Function to debounce
 * @param {number} delay Delay in milliseconds
 * @returns {[Function, Function]}
 */
export function debounce(func, delay) {
  let timeoutId;

  const debounced = (...args) => {
    clearTimeout(timeoutId);

    timeoutId = setTimeout(() => {
      return func(...args);
    }, delay);
  };

  const cancel = () => {
    clearTimeout(timeoutId);
  };

  return [debounced, cancel];
}
