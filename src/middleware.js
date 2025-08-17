"use strict";

import { setMetrics } from "./prometheus.js";
import { printLog } from "./utils/index.js";

/**
 * Logger middleware
 * @returns {void}
 */
export function loggerMiddleware() {
  return async (req, res, next) => {
    const start = process.hrtime();

    res.on("finish", () => {
      const diff = process.hrtime(start);
      const method = req.method || "-";
      const protocol = req.protocol || "-";
      const path = req.originalUrl || "-";
      const statusCode = res.statusCode || "-";
      const contentLength = res.get("content-length") || "-";
      const duration = diff[0] * 1e3 + diff[1] / 1e6;
      const origin = req.headers["origin"] || req.headers["referer"] || "-";
      const ip = req.ip || "-";
      const userID = req.headers["userid"] || "-";
      const userAgent = req.headers["user-agent"] || "-";

      printLog(
        "info",
        `${method} ${protocol} ${path} ${statusCode} ${duration} ${contentLength} ${origin} ${ip} ${userAgent}`
      );

      setMetrics(
        method,
        protocol,
        path,
        statusCode,
        origin,
        ip,
        userID,
        userAgent,
        duration
      );
    });

    next();
  };
}
