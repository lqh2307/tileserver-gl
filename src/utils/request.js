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
 * Get data from URL
 * @param {string} url URL to fetch data from
 * @param {number} timeout Timeout in milliseconds
 * @param {"arraybuffer"|"json"|"text"|"stream"|"blob"|"document"|"formdata"} responseType Response type
 * @param {boolean} keepAlive Whether to keep the connection alive
 * @param {object} headers Headers
 * @returns {Promise<axios.AxiosResponse>}
 */
export async function getDataFromURL(
  url,
  timeout,
  responseType,
  keepAlive,
  headers
) {
  try {
    return await axios({
      method: "GET",
      url: url,
      timeout: timeout,
      responseType: responseType,
      headers: headers,
      validateStatus: (status) => {
        return status === StatusCodes.OK;
      },
      httpAgent: new http.Agent({
        keepAlive: keepAlive,
      }),
      httpsAgent: new https.Agent({
        keepAlive: keepAlive,
      }),
    });
  } catch (error) {
    if (error.response) {
      error.message = `Status code: ${error.response.status} - ${error.response.statusText}`;
      error.statusCode = error.response.status;
    }

    throw error;
  }
}

/**
 * Post data to URL
 * @param {string} url URL to post data
 * @param {number} timeout Timeout in milliseconds
 * @param {object} body Body
 * @param {"arraybuffer"|"json"|"text"|"stream"|"blob"|"document"|"formdata"} responseType Response type
 * @param {boolean} keepAlive Whether to keep the connection alive
 * @param {object} headers Headers
 * @returns {Promise<axios.AxiosResponse>}
 */
export async function postDataToURL(
  url,
  timeout,
  body,
  responseType,
  keepAlive,
  headers
) {
  try {
    return await axios({
      method: "POST",
      url: url,
      timeout: timeout,
      responseType: responseType,
      headers: headers,
      data: body,
      validateStatus: (status) => {
        return status === StatusCodes.OK;
      },
      httpAgent: new http.Agent({
        keepAlive: keepAlive,
      }),
      httpsAgent: new https.Agent({
        keepAlive: keepAlive,
      }),
    });
  } catch (error) {
    if (error.response) {
      error.message = `Status code: ${error.response.status} - ${error.response.statusText}`;
      error.statusCode = error.response.status;
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
    const response = await getDataFromURL(
      url,
      timeout,
      "arraybuffer",
      false,
      headers
    );

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
    const response = await getDataFromURL(
      url,
      timeout,
      "arraybuffer",
      false,
      headers
    );

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

    const response = await getDataFromURL(url, timeout, "stream");

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
          `Failed to download file "${filePath}" - From "${url}": ${error}`
        );
      }
    } else {
      throw new Error(
        `Failed to download file "${filePath}" - From "${url}": ${error}`
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
    (scheme) => url.startsWith(scheme)
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
