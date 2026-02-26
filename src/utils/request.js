"use strict";

import { StatusCodes } from "http-status-codes";
import { printLog } from "./logger.js";
import https from "node:https";
import http from "node:http";
import axios from "axios";

export const HTTP_SCHEMES = ["https://", "http://"];

/**
 * Request to URL
 * @param {string} url URL to request
 * @param {{ method: axios.Method, timeout: number, body: object, responseType: axios.ResponseType, keepAlive: boolean, headers: object, decompress: boolean }} options Options
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
    if (error.response) {
      error.statusCode = error.response.status;
      error.message = `Status code: ${error.response.status} - ${error.statusCode === StatusCodes.NO_CONTENT ? "Not Found" : error.response.statusText}`;
    } else if (error.request) {
      error.message = `No response received: ${error.message}`;
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
  const protocol = req.headers["x-forwarded-proto"] || req.protocol;
  const host = req.headers["x-forwarded-host"] || req.headers["host"];
  const prefix = req.headers["x-forwarded-prefix"] || "";

  return `${protocol}://${host}${prefix}`;
}
