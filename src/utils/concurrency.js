"use strict";

/**
 * Run all tasks with concurrency limit
 * @param {AsyncGenerator<function(): Promise<void>>} generator Async generator that yields tasks
 * @param {number} [limit=Infinity] Concurrency limit
 * @param {{ export: boolean }} item Item object
 * @returns {Promise<void>} Response
 */
export async function runAllWithLimit(generator, limit = Infinity, item) {
  const executing = new Set();
  const errors = [];

  for await (const task of generator) {
    if (item && !item.export) {
      break;
    }

    let taskPromise;

    taskPromise = Promise.resolve()
      .then(task)
      .catch((error) => {
        errors.push(error);
      })
      .finally(() => {
        executing.delete(taskPromise);
      });

    executing.add(taskPromise);

    if (executing.size >= limit) {
      await Promise.race(executing);
    }
  }

  await Promise.all(executing);

  if (errors.length) {
    throw new AggregateError(errors, `${errors.length} task(s) failed`);
  }
}
