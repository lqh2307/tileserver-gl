"use strict";

const inFlightTasks = new Map();

/**
 * Share one in-flight promise among callers using the same key.
 * The entry is removed after completion and never acts as a value cache.
 * @param {string} key Task key
 * @param {() => Promise<any>} task Task factory
 * @returns {Promise<any>} Shared task result
 */
export function runSingleFlight(key, task) {
  const currentTask = inFlightTasks.get(key);
  if (currentTask) {
    return currentTask;
  }

  let taskPromise;
  taskPromise = Promise.resolve()
    .then(task)
    .finally(() => {
      if (inFlightTasks.get(key) === taskPromise) {
        inFlightTasks.delete(key);
      }
    });

  inFlightTasks.set(key, taskPromise);

  return taskPromise;
}
