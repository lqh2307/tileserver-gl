"use strict";

import { DEFAULT_CACHE_TIMEOUT } from "../defaults/default.js";
import { StatusCodes } from "http-status-codes";
import { createCache } from "cache-manager";
import { getFileCreated } from "./file.js";
import axios, { isCancel } from "axios";
import { printLog } from "./logger.js";
import { min } from "./number.js";
import https from "node:https";
import http from "node:http";

const LOCAL_SCHEMES = new Set([
  "mbtiles://",
  "pmtiles://",
  "xyz://",
  "pg://",
  "geojson://",
]);

export const HTTP_SCHEMES = ["https://", "http://"];

const MAX_ERROR_RESPONSE_LENGTH = 2000;
const MAX_HTTP_SOCKETS = 256;
const MAX_HTTP_FREE_SOCKETS = 64;

const httpAgent = new http.Agent({
  keepAlive: true,
  maxSockets: MAX_HTTP_SOCKETS,
  maxFreeSockets: MAX_HTTP_FREE_SOCKETS,
});
const httpsAgent = new https.Agent({
  keepAlive: true,
  maxSockets: MAX_HTTP_SOCKETS,
  maxFreeSockets: MAX_HTTP_FREE_SOCKETS,
});

const lastModifiedCaches = createCache({
  ttl: DEFAULT_CACHE_TIMEOUT,
});

/** Read a case-insensitive parameter from a query/body object. */
export function getParameter(parameters, name, fallback) {
  const key = Object.keys(parameters ?? {}).find((item) => {
    return item.toLowerCase() === String(name).toLowerCase();
  });

  if (key === undefined) {
    return fallback;
  }

  const value = parameters[key];

  return Array.isArray(value) ? value.join(",") : value;
}

async function getCachedLastModified(fileOrFolderPath) {
  return await lastModifiedCaches.wrap(fileOrFolderPath, async () => {
    return new Date(await getFileCreated(fileOrFolderPath)).toUTCString();
  });
}

/**
 * Check if a string is a URL (blob, data, http, or https).
 * @param {string} data Input string
 * @returns {boolean} True if the string is a URL, false otherwise
 */
export function isURL(data) {
  return (
    data.startsWith("blob:") ||
    data.startsWith("data:") ||
    HTTP_SCHEMES.some((scheme) => {
      return data.startsWith(scheme);
    })
  );
}

/**
 * Abort an HTTP request and optionally create a new AbortController.
 * @param {AbortController} controller Controller to abort
 * @param {boolean} create If true, create and return a new controller
 * @returns {AbortController} New controller when `create` is true
 */
export function abortRequest(controller, create) {
  if (controller) {
    controller.abort();
  }

  if (create) {
    return new AbortController();
  }
}

/**
 * Request to URL
 * @param {string} url URL to request
 * @param {{ method: axios.Method, timeout: number, body: object, responseType: axios.ResponseType, keepAlive: boolean, headers: object, decompress: boolean, signal: AbortSignal }} options Options
 * @returns {Promise<axios.AxiosResponse>}
 */
export async function requestToURL(url, options) {
  try {
    return await axios({
      method: options.method,
      url,
      timeout: options.timeout,
      responseType: options.responseType,
      headers: options.headers,
      data: options.body,
      signal: options.signal,
      decompress: options.decompress,
      validateStatus: (status) => {
        return (
          StatusCodes.OK <= status &&
          status < StatusCodes.MULTIPLE_CHOICES &&
          status !== StatusCodes.NO_CONTENT
        );
      },
      httpAgent: options.keepAlive === false ? undefined : httpAgent,
      httpsAgent: options.keepAlive === false ? undefined : httpsAgent,
    });
  } catch (error) {
    if (isCancel(error)) {
      throw error;
    }

    if (error.response) {
      error.statusCode = error.response.status;
      error.responseData = error.response.data;

      let responseMessage;
      if (Buffer.isBuffer(error.response.data)) {
        responseMessage = error.response.data.toString("utf8");
      } else if (typeof error.response.data === "string") {
        responseMessage = error.response.data;
      } else if (error.response.data !== undefined) {
        try {
          responseMessage = JSON.stringify(error.response.data);
        } catch {
          responseMessage = `${error.response.data}`;
        }
      }

      if (responseMessage?.length > MAX_ERROR_RESPONSE_LENGTH) {
        responseMessage = `${responseMessage.slice(0, MAX_ERROR_RESPONSE_LENGTH)}...`;
      }

      const statusMessage =
        error.statusCode === StatusCodes.NO_CONTENT
          ? "Not Found"
          : error.response.statusText || "Request failed";

      error.message = `Status code: ${error.statusCode} - ${statusMessage}${responseMessage ? ` - Response: ${responseMessage}` : ""}`;
    } else if (error.request) {
      error.message = "No response received";
    }

    throw error;
  }
}

/**
 * Get data from a URL
 * @param {string} url URL to get data
 * @param {{ method: axios.Method, timeout: number, body: object, responseType: axios.ResponseType, keepAlive: boolean, headers: object, decompress: boolean, maxTry: number }} options Options
 * @returns {Promise<any>}
 */
export async function getDataFromURL(url, options) {
  if (options.maxTry > 0) {
    for (let attempt = 1; attempt <= options.maxTry; attempt++) {
      try {
        const response = await requestToURL(url, options);

        return response.data;
      } catch (error) {
        if (
          error.statusCode >= StatusCodes.BAD_REQUEST &&
          error.statusCode < StatusCodes.INTERNAL_SERVER_ERROR
        ) {
          throw error;
        }

        const remainingAttempts = options.maxTry - attempt;
        if (remainingAttempts > 0) {
          printLog(
            "warn",
            `${error}. ${remainingAttempts} try remaining with backoff...`,
          );
        } else {
          throw error;
        }
      }
    }
  } else {
    const response = await requestToURL(url, options);

    return response.data;
  }
}

/**
 * Check URL is local?
 * @param {string} url URL to check
 * @returns {boolean}
 */
export function isLocalURL(url) {
  if (typeof url !== "string") {
    return false;
  }

  const schemeEnd = url.indexOf("://");

  return schemeEnd !== -1 && LOCAL_SCHEMES.has(url.slice(0, schemeEnd + 3));
}

/**
 * Get the public path prefix supplied by a reverse proxy.
 * `x-prefix` is preferred and `x-forwarded-prefix` is also supported.
 * @param {Request} req Request object
 * @returns {string} Normalized public prefix without a trailing slash
 */
export function getRequestPrefix(req) {
  const prefix =
    req.headers["x-prefix"] || req.headers["x-forwarded-prefix"] || "";

  if (!prefix.startsWith("/") || prefix.startsWith("//")) {
    return "";
  }

  return prefix.endsWith("/") ? prefix.slice(0, -1) : prefix;
}

/**
 * Get the public request host. The explicit `referer` query parameter takes
 * precedence over reverse-proxy headers.
 * @param {Request} req Request object
 * @returns {string}
 */
export function getRequestHost(req) {
  if (req.query.referer) {
    return req.query.referer;
  }

  const protocol = req.headers["x-forwarded-proto"] || req.protocol || "";
  const host = req.headers["x-forwarded-host"] || req.headers.host || "";

  return `${protocol}://${host}${getRequestPrefix(req)}`;
}

/**
 * Check whether a local file has changed since the client's last request.
 * If a request also includes If-None-Match, Express evaluates that validator
 * after creating the response body, as required by HTTP's validator priority.
 * A forwarded resource may not have been written to the cache yet, so a
 * missing file deliberately does not prevent its response.
 * @param {Request} req Express request
 * @param {Response} res Express response
 * @param {string} fileOrFolderPath File or folder path to inspect
 * @returns {Promise<boolean>}
 */
export async function isFileNotModified(req, res, fileOrFolderPath) {
  try {
    const lastModified = await getCachedLastModified(fileOrFolderPath);

    res.set({
      "cache-control": "public, max-age=0",
      "last-modified": lastModified,
    });

    const ifModifiedSince = req.get("if-modified-since");
    if (!ifModifiedSince || req.get("if-none-match")) {
      return false;
    }

    return Date.parse(lastModified) <= Date.parse(ifModifiedSince);
  } catch (error) {
    if (error.message === "Not Found") {
      return false;
    }

    throw error;
  }
}
