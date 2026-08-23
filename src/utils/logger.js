"use strict";

import pretty from "pino-pretty";
import pino from "pino";

let logger;

if (!logger) {
  logger = pino(
    {
      level: process.env.LOG_LEVEL || "info",
      base: {
        pid: process.pid,
      },
      timestamp: pino.stdTimeFunctions.isoTime,
    },
    process.env.LOG_PRETTY !== "false"
      ? pretty({
          colorize: true,
          translateTime: "SYS:standard",
          ignore: "hostname",
          sync: false,
        })
      : pino.destination({
          sync: false,
        }),
  );
}

/**
 * Print log using pino with custom format
 * @param {"fatal"|"error"|"warn"|"info"|"debug"|"trace"} level Log level
 * @param {string} msg Message
 * @returns {void}
 */
export function printLog(level, msg) {
  logger[level](msg);
}

/**
 * Check whether a log level is enabled before building expensive messages.
 * @param {"fatal"|"error"|"warn"|"info"|"debug"|"trace"} level Log level
 * @returns {boolean} True when enabled
 */
export function isLogLevelEnabled(level) {
  return logger.isLevelEnabled(level);
}

/**
 * Get duration in seconds
 * @param {number} startTime Start time in milliseconds
 * @returns {number} Duration in seconds
 */
export function getDuration(startTime) {
  return (Date.now() - startTime) / 1000;
}
