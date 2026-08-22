"use strict";

export const TASK_TYPE_KEYS = Object.freeze({
  sprite: "sprites",
  font: "fonts",
  style: "styles",
  geojson: "geojsons",
  data: "datas",
});

export const TASK_TYPES = new Set(Object.keys(TASK_TYPE_KEYS));

/**
 * Get configured IDs for one sync type.
 * @param {object} repository Seed or cleanup configuration
 * @param {string} type Sync type
 * @param {string} [id] Optional exact resource ID
 * @returns {string[]} Matching IDs
 */
export function getTaskIds(repository, type, id) {
  const items = repository?.[TASK_TYPE_KEYS[type]];
  if (!items) {
    return [];
  }

  if (id) {
    return id in items ? [id] : [];
  }

  return Object.keys(items);
}

/**
 * Expand a selector into unique resource-level sync targets.
 * @param {{ type?: string, id?: string }} selector Task selector
 * @param {object} seedConfig Seed configuration
 * @param {object} cleanUpConfig Cleanup configuration
 * @returns {{ type: string, id: string }[]} Sync targets
 */
export function getTaskTargets(selector, seedConfig, cleanUpConfig) {
  if (selector.id && !selector.type) {
    return [];
  }

  const types = selector.type
    ? TASK_TYPES.has(selector.type)
      ? [selector.type]
      : []
    : Object.keys(TASK_TYPE_KEYS);
  const targets = [];

  for (const type of types) {
    const ids = new Set([
      ...getTaskIds(cleanUpConfig, type, selector.id),
      ...getTaskIds(seedConfig, type, selector.id),
    ]);

    for (const id of ids) {
      targets.push({
        type,
        id,
      });
    }
  }

  return targets;
}

/**
 * Create the registry key for a resource-level sync task.
 * @param {{ type: string, id: string }} target Task target
 * @returns {string} Registry key
 */
export function getTaskKey(target) {
  return `${target.type}:${target.id}`;
}

/**
 * Check whether a resource-level task matches a cancel selector.
 * @param {{ type: string, id: string }} target Task target
 * @param {{ type?: string, id?: string }} selector Cancel selector
 * @returns {boolean} True when matched
 */
export function isTaskTargetMatched(target, selector) {
  return (
    (!selector.type || target.type === selector.type) &&
    (!selector.id || target.id === selector.id)
  );
}
