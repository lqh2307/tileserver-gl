"use strict";

import { isLogLevelEnabled, setMetrics, printLog } from "../utils/index.js";
import express from "express";

const jsonParser = express.json({
  limit: "500mb",
});

const METHODS = new Set(["POST", "PUT", "PATCH", "DELETE"]);

/**
 * Parse and limit JSON bodies for methods that can carry a JSON payload.
 * @returns {express.RequestHandler} Middleware
 */
export function jsonBodyMiddleware() {
  return (req, res, next) => {
    if (METHODS.has(req.method)) {
      jsonParser(req, res, next);
    } else {
      next();
    }
  };
}

/**
 * Logger middleware
 * @returns {void}
 */
export function loggerMiddleware() {
  return (req, res, next) => {
    const start = process.hrtime.bigint();

    res.once("finish", () => {
      const duration = Number(process.hrtime.bigint() - start) / 1e6;
      const method = req.method;
      const routePath = req.route?.path;
      const route =
        typeof routePath === "string"
          ? `${req.baseUrl || ""}${routePath}`
          : routePath instanceof RegExp
            ? routePath.toString()
            : "unmatched";
      const statusCode = res.statusCode;
      const contentLength = res.get("content-length") || "-";
      const level =
        statusCode >= 500 ? "error" : statusCode >= 400 ? "warn" : "info";

      if (isLogLevelEnabled(level)) {
        const ips = req.ips;
        const clientIp = ips[0] || req.socket.remoteAddress || "-";
        const proxies = ips.length > 1 ? ips.join(",") : "-";

        printLog(
          level,
          `${method} ${req.originalUrl} ${statusCode} ${duration} ${contentLength} ${clientIp} ${proxies} ${req.headers["user-agent"] || "-"} ${req.headers.referer || "-"}`,
        );
      }

      setMetrics(method, route, statusCode, duration);
    });

    next();
  };
}
