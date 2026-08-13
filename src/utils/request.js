"use strict";

import { StatusCodes } from "http-status-codes";
import { createCache } from "cache-manager";
import { getFileCreated } from "./file.js";
import axios, { isCancel } from "axios";
import { printLog } from "./logger.js";
import https from "node:https";
import http from "node:http";

export const HTTP_SCHEMES = ["https://", "http://"];

/* Cache in RAM */
const lastModifiedCaches = createCache({
  ttl: 300000, // 5 mins
});

/**
 * Check if a string is a URL (blob, data, http, or https).
 * @param {string} data Input string
 * @returns {boolean} True if the string is a URL, false otherwise
 */
export function isURL(data) {
  if (
    data.startsWith("blob:") ||
    data.startsWith("data:") ||
    data.startsWith("http")
  ) {
    return true;
  }

  return false;
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
      url: url,
      timeout: options.timeout,
      responseType: options.responseType,
      headers: options.headers,
      data: options.body,
      signal: options.signal,
      decompress: options.decompress,
      validateStatus: (status) =>
        StatusCodes.OK <= status &&
        status < StatusCodes.MULTIPLE_CHOICES &&
        status !== StatusCodes.NO_CONTENT,
      httpAgent: new http.Agent({
        keepAlive: options.keepAlive,
      }),
      httpsAgent: new https.Agent({
        keepAlive: options.keepAlive,
      }),
    });
  } catch (error) {
    if (isCancel(error)) {
      throw error;
    }

    if (error.response) {
      error.statusCode = error.response.status;
      error.message = `Status code: ${error.response.status} - ${error.statusCode === StatusCodes.NO_CONTENT ? "Not Found" : error.response.statusText}`;
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
          error.statusCode &&
          (error.statusCode === StatusCodes.NO_CONTENT ||
            error.statusCode === StatusCodes.NOT_FOUND)
        ) {
          throw error;
        }

        const remainingAttempts = options.maxTry - attempt;
        if (remainingAttempts > 0) {
          printLog("warn", `${error}. ${remainingAttempts} try remaining...`);
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

  return ["mbtiles://", "pmtiles://", "xyz://", "pg://", "geojson://"].some(
    (scheme) => url.startsWith(scheme),
  );
}

/**
 * Get request host
 * @param {Request} req Request object
 * @returns {string}
 */
export function getRequestHost(req) {
  // const protocol = req.headers["x-forwarded-proto"] || req.protocol || "";
  // const host = req.headers["x-forwarded-host"] || req.headers["host"] || "";
  // const prefix = req.headers["x-forwarded-prefix"] || "";

  return `${req.headers["x-forwarded-proto"] || req.protocol || ""}://${req.headers["x-forwarded-host"] || req.headers["host"] || ""}${req.headers["x-forwarded-prefix"] || ""}`;
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
    const lastModified = await lastModifiedCaches.wrap(
      fileOrFolderPath,
      async () =>
        new Date(await getFileCreated(fileOrFolderPath)).toUTCString(),
    );

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
