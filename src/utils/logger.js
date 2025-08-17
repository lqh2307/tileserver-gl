"use strict";

import pretty from "pino-pretty";
import pino from "pino";

let logger;

if (!logger) {
  logger = pino(
    {
      level: "info",
      base: {
        pid: process.pid,
      },
      timestamp: pino.stdTimeFunctions.isoTime,
    },
    pretty({
      colorize: true,
      translateTime: "SYS:standard",
      ignore: "hostname",
      sync: true,
    })
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
