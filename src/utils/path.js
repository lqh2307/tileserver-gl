"use strict";

import { fileURLToPath } from "node:url";
import path from "node:path";

/** Absolute path to the project root, independent of process.cwd(). */
const PROJECT_ROOT = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "..",
  "..",
);

/**
 * Resolve a path inside the project installation.
 * @param {...string} segments Path segments relative to the project root
 * @returns {string}
 */
export function resolveProjectPath(...segments) {
  return path.resolve(PROJECT_ROOT, ...segments);
}
