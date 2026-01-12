"use strict";

import { rename, mkdir, rm } from "node:fs/promises";
import { detectFormatAndHeaders } from "./util.js";
import { StatusCodes } from "http-status-codes";
import { createWriteStream } from "node:fs";
import https from "node:https";
import http from "node:http";
import path from "node:path";
import axios from "axios";

/**
 * Request to URL
 * @param {{ url: string, method: axios.Method, timeout: number, body: object, responseType: axios.ResponseType, keepAlive: boolean, headers: object }} options Options
 * @returns {Promise<axios.AxiosResponse>}
 */
export async function requestToURL(options) {
  try {
    return await axios({
      method: options.method,
      url: options.url,
      timeout: options.timeout,
      responseType: options.responseType,
      headers: options.headers,
      data: options.body,
      validateStatus: (status) => {
        return (
          StatusCodes.OK <= status &&
          status < StatusCodes.MULTIPLE_CHOICES &&
          status !== StatusCodes.NO_CONTENT
        );
      },
      httpAgent: new http.Agent({
        keepAlive: options.keepAlive,
      }),
      httpsAgent: new https.Agent({
        keepAlive: options.keepAlive,
      }),
    });
  } catch (error) {
    if (error.response) {
      error.message = `Status code: ${error.response.status} - ${error.response.statusText}`;
      error.statusCode = error.response.status;
    } else if (error.request) {
      error.message = "No response received";
    }

    throw error;
  }
}

/**
 * Get data tile from a URL
 * @param {string} url The URL to fetch data tile from
 * @param {object} headers Headers
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<object>}
 */
export async function getDataTileFromURL(url, headers, timeout) {
  try {
    const response = await requestToURL({
      url: url,
      method: "GET",
      timeout: timeout,
      responseType: "arraybuffer",
      headers: headers,
    });

    return {
      data: response.data,
      headers: detectFormatAndHeaders(response.data).headers,
    };
  } catch (error) {
    if (error.statusCode) {
      if (
        error.statusCode === StatusCodes.NO_CONTENT ||
        error.statusCode === StatusCodes.NOT_FOUND
      ) {
        throw new Error("Tile does not exist");
      } else {
        throw new Error(`Failed to get data tile from "${url}": ${error}`);
      }
    } else {
      throw new Error(`Failed to get data tile from "${url}": ${error}`);
    }
  }
}

/**
 * Get data file from a URL
 * @param {string} url The URL to fetch data from
 * @param {object} headers Headers
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<Buffer>}
 */
export async function getDataFileFromURL(url, headers, timeout) {
  try {
    const response = await requestToURL({
      url: url,
      method: "GET",
      timeout: timeout,
      responseType: "arraybuffer",
      headers: headers,
    });

    return response.data;
  } catch (error) {
    if (error.statusCode) {
      if (
        error.statusCode === StatusCodes.NO_CONTENT ||
        error.statusCode === StatusCodes.NOT_FOUND
      ) {
        throw new Error("File does not exist");
      } else {
        throw new Error(`Failed to get file from "${url}": ${error}`);
      }
    } else {
      throw new Error(`Failed to get file from "${url}": ${error}`);
    }
  }
}

/**
 * Download file with stream
 * @param {string} url The URL to download the file from
 * @param {string} filePath The path where the file will be saved
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<void>}
 */
export async function downloadFileWithStream(url, filePath, timeout) {
  try {
    await mkdir(path.dirname(filePath), {
      recursive: true,
    });

    const response = await await requestToURL({
      url: url,
      method: "GET",
      timeout: timeout,
      responseType: "stream",
    });

    const tempFilePath = `${filePath}.tmp`;

    const writer = createWriteStream(tempFilePath);

    response.data.pipe(writer);

    return await new Promise((resolve, reject) => {
      writer
        .on("finish", async () => {
          await rename(tempFilePath, filePath);

          resolve();
        })
        .on("error", async (error) => {
          await rm(tempFilePath, {
            force: true,
          });

          reject(error);
        });
    });
  } catch (error) {
    if (error.statusCode) {
      if (
        error.statusCode === StatusCodes.NO_CONTENT ||
        error.statusCode === StatusCodes.NOT_FOUND
      ) {
        throw new Error("File does not exist");
      } else {
        throw new Error(
          `Failed to download file "${filePath}" - From "${url}": ${error}`,
        );
      }
    } else {
      throw new Error(
        `Failed to download file "${filePath}" - From "${url}": ${error}`,
      );
    }
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
