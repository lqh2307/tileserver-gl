"use strict";

import { constants as zlibConstants, createGzip } from "node:zlib";
import express from "express";
import {
  shouldCompressResponse,
  isLogLevelEnabled,
  setMetrics,
  printLog,
} from "../utils/index.js";

const jsonParser = express.json({
  limit: "500mb",
});

const METHODS = new Set(["POST", "PUT", "PATCH", "DELETE"]);

/**
 * Return whether a content type benefits from gzip transfer compression.
 * @param {unknown} value Content-Type header
 * @returns {boolean}
 */
function isCompressibleContentType(value) {
  if (typeof value !== "string") {
    return false;
  }

  const contentType = value.split(";", 1)[0].trim().toLowerCase();

  return (
    contentType.startsWith("text/") ||
    contentType.includes("json") ||
    contentType.includes("javascript") ||
    contentType.includes("xml") ||
    contentType === "application/x-protobuf" ||
    contentType === "application/vnd.mapbox-vector-tile"
  );
}

/**
 * Compress eligible HTTP responses when enabled.
 * Existing Content-Encoding headers and already-compressed media are left
 * untouched. The middleware streams the response through gzip and removes a
 * stale Content-Length header before the compressed bytes are sent.
 * @returns {import("express").RequestHandler}
 */
export function responseCompressionMiddleware() {
  return (req, res, next) => {
    if (req.method === "HEAD" || !shouldCompressResponse(req)) {
      next();

      return;
    }

    res.vary("Accept-Encoding");

    if (req.acceptsEncodings("gzip") !== "gzip") {
      next();

      return;
    }

    const writeHead = res.writeHead.bind(res);
    const write = res.write.bind(res);
    const end = res.end.bind(res);
    const flushHeaders = res.flushHeaders?.bind(res);

    let gzip;
    let initialized = false;
    let emittingGzipDrain = false;

    const initialize = () => {
      if (initialized) {
        return;
      }

      initialized = true;

      const statusCode = res.statusCode;
      const contentEncoding = res.getHeader("content-encoding");
      const contentType = res.getHeader("content-type");
      const cacheControl = String(res.getHeader("cache-control") ?? "");

      if (
        res.headersSent ||
        contentEncoding !== undefined ||
        statusCode < 200 ||
        statusCode === 204 ||
        statusCode === 206 ||
        statusCode === 304 ||
        /(?:^|,)\s*no-transform\s*(?:,|$)/i.test(cacheControl) ||
        !isCompressibleContentType(contentType)
      ) {
        return;
      }

      res.removeHeader("content-length");
      res.setHeader("content-encoding", "gzip");

      gzip = createGzip({
        level: zlibConstants.Z_BEST_SPEED,
      });

      gzip.on("data", (chunk) => {
        if (!write(chunk)) {
          gzip.pause();
        }
      });

      gzip.on("drain", () => {
        emittingGzipDrain = true;

        res.emit("drain");

        emittingGzipDrain = false;
      });

      gzip.on("error", (error) => {
        res.destroy(error);
      });

      res.once("close", () => {
        if (!gzip.destroyed) {
          gzip.destroy();
        }
      });

      res.on("drain", () => {
        if (!emittingGzipDrain && gzip.isPaused()) {
          gzip.resume();
        }
      });
    };

    res.writeHead = (...args) => {
      const headerArg =
        typeof args[1] === "object"
          ? args[1]
          : typeof args[2] === "object"
            ? args[2]
            : undefined;

      if (headerArg) {
        for (const [name, value] of Object.entries(headerArg)) {
          res.setHeader(name, value);
        }
      }

      initialize();

      return typeof args[1] === "string"
        ? writeHead(args[0], args[1])
        : writeHead(args[0]);
    };

    res.write = (chunk, encoding, callback) => {
      initialize();

      return gzip
        ? gzip.write(chunk, encoding, callback)
        : write(chunk, encoding, callback);
    };

    res.end = (chunk, encoding, callback) => {
      if (typeof chunk === "function") {
        callback = chunk;

        chunk = undefined;
        encoding = undefined;
      } else if (typeof encoding === "function") {
        callback = encoding;

        encoding = undefined;
      }

      initialize();

      if (!gzip) {
        return end(chunk, encoding, callback);
      }

      gzip.once("end", () => {
        end(callback);
      });

      if (chunk === undefined) {
        gzip.end();
      } else {
        gzip.end(chunk, encoding);
      }

      return res;
    };

    if (flushHeaders) {
      res.flushHeaders = () => {
        initialize();

        return flushHeaders();
      };
    }

    next();
  };
}

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
