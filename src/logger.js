"use strict";

import FileStreamRotator from "file-stream-rotator";
import pino from "pino";

let logger;

/* Init pino logger */
if (logger === undefined) {
  logger = pino(
    {
      level: "info",
      base: { pid: process.pid },
      formatters: {
        level(label) {
          return { level: label };
        },
      },
      timestamp: pino.stdTimeFunctions.isoTime,
    },
    pino.multistream([
      {
        stream: process.stdout,
      },
      {
        stream: FileStreamRotator.getStream({
          filename: `${process.env.DATA_DIR}/logs/%DATE%.log`,
          frequency: "daily",
          date_format: "YYYY-MM-DD",
        }),
      },
    ])
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
