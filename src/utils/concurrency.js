"use strict";

/**
 * Run all tasks with concurrency limit
 * @param {AsyncGenerator<function(): Promise<void>>} generator Async generator that yields tasks
 * @param {number} limit Concurrency limit. If limit < 1, it will be treated as Infinity
 * @param {{ export: boolean }} item Item object
 * @returns {Promise<{void}>} Response
 */
export async function runAllWithLimit(generator, limit, item) {
  const concurrency = limit >= 1 ? limit : Infinity;

  const executing = new Set();

  for await (const task of generator) {
    if (item && !item.export) {
      break;
    }

    const p = Promise.resolve().then(task);

    executing.add(p);

    p.finally(() => executing.delete(p));

    if (executing.size >= concurrency) {
      await Promise.race(executing);
    }
  }

  await Promise.allSettled(executing);
}
