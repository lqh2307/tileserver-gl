"use strict";

import FileStreamRotator from "file-stream-rotator";
import pretty from "pino-pretty";
import pino from "pino";

let logger;

/* Init pino logger */
if (logger === undefined) {
  const consoleStream = pretty({
    colorize: true,
    translateTime: "SYS:standard",
    ignore: "hostname",
  });

  if ((process.env.LOGGING_TO_FILE || "true") === "true") {
    const logFileStream = FileStreamRotator.getStream({
      filename: `${process.env.DATA_DIR}/logs/%DATE%.log`,
      frequency: "daily",
      date_format: "YYYY-MM-DD",
    });

    consoleStream.pipe(logFileStream);
  }

  logger = pino(
    {
      level: "info",
      base: {
        pid: process.pid,
      },
      formatters: {
        level(label) {
          return {
            level: label,
          };
        },
      },
      timestamp: pino.stdTimeFunctions.isoTime,
    },
    consoleStream
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
