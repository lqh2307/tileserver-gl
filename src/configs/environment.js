"use strict";

import path from "node:path";

const LOG_LEVELS = new Set([
  "fatal",
  "error",
  "warn",
  "info",
  "debug",
  "trace",
  "silent",
]);

/**
 * Return a non-empty string or a fallback value.
 * @param {unknown} value Candidate value
 * @param {string} fallback Fallback value
 * @returns {string}
 */
export function getEnvironmentString(value, fallback) {
  if (typeof value === "string" && value.trim()) {
    return value.trim();
  }

  return fallback;
}

/**
 * Return a supported Pino log level or a fallback.
 * @param {unknown} value Candidate log level
 * @param {string} fallback Fallback log level
 * @returns {string}
 */
export function getEnvironmentLogLevel(value, fallback = "debug") {
  const level = getEnvironmentString(value, fallback).toLowerCase();

  return LOG_LEVELS.has(level) ? level : fallback;
}

/**
 * Parse a boolean configuration value.
 * @param {unknown} value Candidate value
 * @param {boolean} fallback Fallback value
 * @returns {boolean}
 */
export function getEnvironmentBoolean(value, fallback) {
  if (typeof value === "boolean") {
    return value;
  }

  if (typeof value !== "string") {
    return fallback;
  }

  switch (value.trim().toLowerCase()) {
    case "true":
    case "1":
    case "yes":
    case "on": {
      return true;
    }

    case "false":
    case "0":
    case "no":
    case "off": {
      return false;
    }

    default: {
      return fallback;
    }
  }
}

/**
 * Parse a bounded integer configuration value.
 * @param {unknown} value Candidate value
 * @param {number} fallback Fallback value
 * @param {{ min?: number, max?: number }} bounds Allowed bounds
 * @returns {number}
 */
export function getEnvironmentInteger(value, fallback, { min = 0, max } = {}) {
  if (typeof value !== "number" && typeof value !== "string") {
    return fallback;
  }

  const result = typeof value === "number" ? value : Number(value.trim());

  if (
    !Number.isInteger(result) ||
    result < min ||
    (max !== undefined && result > max)
  ) {
    return fallback;
  }

  return result;
}

/**
 * Resolve DATA_DIR once for the primary process and workers.
 * @param {unknown} value Configured data directory
 * @returns {string}
 */
export function resolveDataDir(value = process.env.DATA_DIR) {
  return path.resolve(getEnvironmentString(value, "data"));
}

/**
 * Normalize runtime environment values after the application config is loaded.
 * @param {Record<string, any>} configOptions Config options
 * @param {number} cpuCount Available CPU count
 * @returns {void}
 */
export function configureRuntimeEnvironment(configOptions = {}, cpuCount = 1) {
  const processFallback = getEnvironmentInteger(configOptions.process, 1, {
    min: 1,
  });
  const threadFallback = getEnvironmentInteger(configOptions.thread, cpuCount, {
    min: 1,
    max: 1024,
  });
  const portFallback = getEnvironmentInteger(configOptions.listenPort, 8080, {
    min: 1,
    max: 65535,
  });

  process.env.SERVICE_NAME = getEnvironmentString(
    process.env.SERVICE_NAME,
    "tile-server",
  );
  process.env.RESTART_AFTER_CONFIG_CHANGE = getEnvironmentBoolean(
    process.env.RESTART_AFTER_CONFIG_CHANGE,
    true,
  )
    ? "true"
    : "false";
  process.env.LOG_LEVEL = getEnvironmentLogLevel(process.env.LOG_LEVEL);

  process.env.NUM_OF_PROCESS = String(
    getEnvironmentInteger(process.env.NUM_OF_PROCESS, processFallback, {
      min: 1,
    }),
  );

  const configuredThreadCount =
    getEnvironmentString(
      process.env.NUM_OF_THREAD,
      getEnvironmentString(process.env.UV_THREADPOOL_SIZE, ""),
    ) || undefined;

  process.env.UV_THREADPOOL_SIZE = String(
    getEnvironmentInteger(configuredThreadCount, threadFallback, {
      min: 1,
      max: 1024,
    }),
  );

  process.env.POSTGRESQL_BASE_URI = getEnvironmentString(
    process.env.POSTGRESQL_BASE_URI,
    getEnvironmentString(
      configOptions.postgreSQLBaseURI,
      "postgresql://localhost:5432",
    ),
  );
  process.env.SERVE_FRONT_PAGE = getEnvironmentBoolean(
    configOptions.serveFrontPage,
    true,
  )
    ? "true"
    : "false";
  process.env.SERVE_SWAGGER = getEnvironmentBoolean(
    configOptions.serveSwagger,
    true,
  )
    ? "true"
    : "false";
  process.env.LISTEN_PORT = String(
    getEnvironmentInteger(process.env.LISTEN_PORT, portFallback, {
      min: 1,
      max: 65535,
    }),
  );
}
