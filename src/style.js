"use strict";

import { validateStyleMin } from "@maplibre/maplibre-gl-style-spec";
import { StatusCodes } from "http-status-codes";
import fsPromise from "node:fs/promises";
import { printLog } from "./logger.js";
import { config } from "./config.js";
import {
  removeFileWithLock,
  createFileWithLock,
  getDataFromURL,
  isLocalTileURL,
  retry,
} from "./utils.js";

/**
 * Remove style data file with lock
 * @param {string} filePath File path to remove style data file
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<void>}
 */
export async function removeStyleFile(filePath, timeout) {
  await removeFileWithLock(filePath, timeout);
}

/**
 * Download style file
 * @param {string} url The URL to download the file from
 * @param {string} filePath File path
 * @param {number} maxTry Number of retry attempts on failure
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<void>}
 */
export async function downloadStyleFile(url, filePath, maxTry, timeout) {
  await retry(async () => {
    try {
      // Get data from URL
      const response = await getDataFromURL(url, timeout, "arraybuffer");

      // Store data to file
      await cacheStyleFile(filePath, response.data);
    } catch (error) {
      if (error.statusCode !== undefined) {
        printLog(
          "error",
          `Failed to download style file "${filePath}" - From "${url}": ${error}`
        );

        if (
          error.statusCode === StatusCodes.NO_CONTENT ||
          error.statusCode === StatusCodes.NOT_FOUND
        ) {
          return;
        } else {
          throw error;
        }
      } else {
        throw error;
      }
    }
  }, maxTry);
}

/**
 * Cache style file
 * @param {string} filePath File path
 * @param {Buffer} data Tile data buffer
 * @returns {Promise<void>}
 */
export async function cacheStyleFile(filePath, data) {
  await createFileWithLock(
    filePath,
    data,
    300000 // 5 mins
  );
}

/**
 * Get styleJSON from a URL
 * @param {string} url The URL to fetch data from
 * @param {number} timeout Timeout in milliseconds
 * @param {boolean} isParse
 * @returns {Promise<object|Buffer>}
 */
export async function getStyleJSONFromURL(url, timeout, isParse) {
  try {
    const response = await getDataFromURL(
      url,
      timeout,
      isParse === true ? "json" : "arraybuffer"
    );

    return response.data;
  } catch (error) {
    if (error.statusCode !== undefined) {
      if (
        error.statusCode === StatusCodes.NO_CONTENT ||
        error.statusCode === StatusCodes.NOT_FOUND
      ) {
        throw new Error("Style does not exist");
      } else {
        throw new Error(`Failed to get style from "${url}": ${error}`);
      }
    } else {
      throw new Error(`Failed to get style from "${url}": ${error}`);
    }
  }
}

/**
 * Validate style
 * @param {Object} styleJSON StyleJSON
 * @returns {Promise<void>}
 */
export async function validateStyle(styleJSON) {
  /* Validate style */
  const validationErrors = validateStyleMin(styleJSON);
  if (validationErrors.length > 0) {
    throw new Error(
      validationErrors
        .map((validationError) => `\n\t${validationError.message}`)
        .join()
    );
  }

  /* Validate fonts */
  if (styleJSON.glyphs !== undefined) {
    if (
      styleJSON.glyphs.startsWith("fonts://") === false &&
      styleJSON.glyphs.startsWith("https://") === false &&
      styleJSON.glyphs.startsWith("http://") === false
    ) {
      throw new Error("Invalid fonts url");
    }
  }

  /* Validate sprite */
  if (styleJSON.sprite !== undefined) {
    if (styleJSON.sprite.startsWith("sprites://") === true) {
      const spriteID = styleJSON.sprite.slice(
        10,
        styleJSON.sprite.lastIndexOf("/")
      );

      if (config.repo.sprites[spriteID] === undefined) {
        throw new Error(`Sprite "${spriteID}" is not found`);
      }
    } else if (
      styleJSON.sprite.startsWith("https://") === false &&
      styleJSON.sprite.startsWith("http://") === false
    ) {
      throw new Error("Invalid sprite url");
    }
  }

  /* Validate sources */
  await Promise.all(
    Object.keys(styleJSON.sources).map(async (id) => {
      const source = styleJSON.sources[id];

      if (source.url !== undefined) {
        if (isLocalTileURL(source.url) === true) {
          const sourceID = source.url.split("/")[2];

          if (config.repo.datas[sourceID] === undefined) {
            throw new Error(
              `Source "${id}" is not found data source "${sourceID}"`
            );
          }
        } else if (
          source.url.startsWith("https://") === false &&
          source.url.startsWith("http://") === false
        ) {
          throw new Error(`Source "${id}" is invalid data url "${source.url}"`);
        }
      }

      if (source.urls !== undefined) {
        if (source.urls.length === 0) {
          throw new Error(`Source "${id}" is invalid data urls`);
        }

        source.urls.forEach((url) => {
          if (isLocalTileURL(url) === true) {
            const sourceID = url.split("/")[2];

            if (config.repo.datas[sourceID] === undefined) {
              throw new Error(
                `Source "${id}" is not found data source "${sourceID}"`
              );
            }
          } else if (
            url.startsWith("https://") === false &&
            url.startsWith("http://") === false
          ) {
            throw new Error(`Source "${id}" is invalid data url "${url}"`);
          }
        });
      }

      if (source.tiles !== undefined) {
        if (source.tiles.length === 0) {
          throw new Error(`Source "${id}" is invalid tile urls`);
        }

        source.tiles.forEach((tile) => {
          if (isLocalTileURL(tile) === true) {
            const sourceID = tile.split("/")[2];

            if (config.repo.datas[sourceID] === undefined) {
              throw new Error(
                `Source "${id}" is not found data source "${sourceID}"`
              );
            }
          } else if (
            tile.startsWith("https://") === false &&
            tile.startsWith("http://") === false
          ) {
            throw new Error(`Source "${id}" is invalid tile url "${tile}"`);
          }
        });
      }
    })
  );
}

/**
 * Get style
 * @param {string} filePath
 * @param {boolean} isParse
 * @returns {Promise<object|Buffer>}
 */
export async function getStyle(filePath, isParse) {
  try {
    const data = await fsPromise.readFile(filePath);
    if (!data) {
      throw new Error("Style does not exist");
    }

    if (isParse === true) {
      return JSON.parse(data);
    } else {
      return data;
    }
  } catch (error) {
    if (error.code === "ENOENT") {
      throw new Error("Style does not exist");
    } else {
      throw error;
    }
  }
}

/**
 * Get created of style
 * @param {string} filePath The path of the file
 * @returns {Promise<number>}
 */
export async function getStyleCreated(filePath) {
  try {
    const stats = await fsPromise.stat(filePath);

    return stats.ctimeMs;
  } catch (error) {
    if (error.code === "ENOENT") {
      throw new Error("Style created does not exist");
    } else {
      throw error;
    }
  }
}

/**
 * Get the size of Style
 * @param {string} filePath The path of the file
 * @returns {Promise<number>}
 */
export async function getStyleSize(filePath) {
  const stat = await fsPromise.stat(filePath);

  return stat.size;
}
