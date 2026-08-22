"use strict";

import { printLog, setMetrics } from "../utils/index.js";
import express from "express";

const jsonParser = express.json({
  limit: "500mb",
});

/**
 * Parse and limit JSON bodies for POST requests.
 * @returns {express.RequestHandler} Middleware
 */
export function jsonBodyMiddleware() {
  return (req, res, next) => {
    if (req.method === "POST") {
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
      const isTileRoute = route.includes(":z") && route.includes(":x");

      if (!isTileRoute || statusCode >= 400) {
        const contentLength = res.get("content-length") || "-";

        printLog(
          statusCode >= 500 ? "error" : statusCode >= 400 ? "warn" : "info",
          `${method} ${req.originalUrl} ${statusCode} ${duration} ${contentLength}`,
        );
      }

      setMetrics(method, route, statusCode, duration);
    });

    next();
  };
}
