"use strict";

import { validateStyleMin } from "@maplibre/maplibre-gl-style-spec";
import { readFile, stat } from "node:fs/promises";
import { StatusCodes } from "http-status-codes";
import { printLog } from "./logger.js";
import { config } from "./config.js";
import {
  removeFileWithLock,
  createFileWithLock,
  getDataFromURL,
  isLocalURL,
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
      if (error.statusCode) {
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
    30000 // 30 secs
  );
}

/**
 * Validate style
 * @param {object} styleJSON StyleJSON
 * @returns {Promise<void>}
 */
export async function validateStyle(styleJSON) {
  /* Validate style */
  const validationErrors = validateStyleMin(styleJSON);
  if (validationErrors.length) {
    throw new Error(
      validationErrors
        .map((validationError) => `\n\t${validationError.message}`)
        .join()
    );
  }

  /* Validate font */
  if (styleJSON.glyphs !== undefined) {
    if (
      !["fonts://", "https://", "http://"].some((scheme) =>
        styleJSON.glyphs.startsWith(scheme)
      )
    ) {
      throw new Error(`Invalid font url "${styleJSON.glyphs}"`);
    }
  }

  /* Validate sprite */
  if (styleJSON.sprite !== undefined) {
    if (styleJSON.sprite.startsWith("sprites://")) {
      const spriteID = styleJSON.sprite.split("/")[2];

      if (!config.sprites[spriteID]) {
        throw new Error(`Sprite "${spriteID}" is not found`);
      }
    } else if (
      !["https://", "http://"].some((scheme) =>
        styleJSON.sprite.startsWith(scheme)
      )
    ) {
      throw new Error(`Invalid sprite url "${styleJSON.sprite}"`);
    }
  }

  /* Validate sources */
  await Promise.all(
    Object.keys(styleJSON.sources).map(async (id) => {
      const source = styleJSON.sources[id];

      if (source.data !== undefined) {
        if (isLocalURL(source.data)) {
          const parts = source.data.split("/");

          if (!config.geojsons[parts[2]]) {
            throw new Error(
              `Source "${id}" is not found data source "${parts[2]}"`
            );
          }

          if (!config.geojsons[parts[2]][parts[3]]) {
            throw new Error(
              `Source "${id}" is not found data source "${parts[3]}"`
            );
          }
        }
      }

      if (source.url !== undefined) {
        if (isLocalURL(source.url)) {
          const sourceID = source.url.split("/")[2];

          if (!config.datas[sourceID]) {
            throw new Error(
              `Source "${id}" is not found data source "${sourceID}"`
            );
          }
        } else if (
          !["https://", "http://", "data:"].some((scheme) =>
            source.url.startsWith(scheme)
          )
        ) {
          throw new Error(`Source "${id}" is invalid data url "${source.url}"`);
        }
      }

      if (source.urls !== undefined) {
        if (source.urls.length === 0) {
          throw new Error(`Source "${id}" is invalid data urls`);
        }

        source.urls.forEach((url) => {
          if (isLocalURL(url)) {
            const sourceID = url.split("/")[2];

            if (!config.datas[sourceID]) {
              throw new Error(
                `Source "${id}" is not found data source "${sourceID}"`
              );
            }
          } else if (
            !["https://", "http://"].some((scheme) => url.startsWith(scheme))
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
          if (isLocalURL(tile)) {
            const sourceID = tile.split("/")[2];

            if (!config.datas[sourceID]) {
              throw new Error(
                `Source "${id}" is not found data source "${sourceID}"`
              );
            }
          } else if (
            !["https://", "http://"].some((scheme) => tile.startsWith(scheme))
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
 * @returns {Promise<Buffer>}
 */
export async function getStyle(filePath) {
  try {
    return await readFile(filePath);
  } catch (error) {
    if (error.code === "ENOENT") {
      throw new Error("JSON does not exist");
    } else {
      throw error;
    }
  }
}

/**
 * Get rendered styleJSON
 * @param {string} filePath
 * @returns {Promise<object|Buffer>}
 */
export async function getRenderedStyleJSON(filePath) {
  try {
    const styleJSON = JSON.parse(await readFile(filePath));

    await Promise.all(
      Object.keys(styleJSON.sources).map(async (id) => {
        const source = styleJSON.sources[id];

        if (source.tiles !== undefined) {
          const tiles = new Set(
            source.tiles.map((tile) => {
              if (isLocalURL(tile)) {
                const sourceID = tile.split("/")[2];
                const sourceData = config.datas[sourceID];

                tile = `${sourceData.sourceType}://${sourceID}/{z}/{x}/{y}.${sourceData.tileJSON.format}`;
              }

              return tile;
            })
          );

          source.tiles = Array.from(tiles);
        }

        if (source.urls !== undefined) {
          const otherUrls = [];

          source.urls.forEach((url) => {
            if (isLocalURL(url)) {
              const sourceID = url.split("/")[2];
              const sourceData = config.datas[sourceID];

              const tile = `${sourceData.sourceType}://${sourceID}/{z}/{x}/{y}.${sourceData.tileJSON.format}`;

              if (source.tiles !== undefined) {
                if (!source.tiles.includes(tile)) {
                  source.tiles.push(tile);
                }
              } else {
                source.tiles = [tile];
              }
            } else {
              if (!otherUrls.includes(url)) {
                otherUrls.push(url);
              }
            }
          });

          if (otherUrls.length === 0) {
            delete source.urls;
          } else {
            source.urls = otherUrls;
          }
        }

        if (source.url !== undefined) {
          if (isLocalURL(source.url)) {
            const sourceID = source.url.split("/")[2];
            const sourceData = config.datas[sourceID];

            const tile = `${sourceData.sourceType}://${sourceID}/{z}/{x}/{y}.${sourceData.tileJSON.format}`;

            if (source.tiles !== undefined) {
              if (!source.tiles.includes(tile)) {
                source.tiles.push(tile);
              }
            } else {
              source.tiles = [tile];
            }

            delete source.url;
          }
        }

        if (
          source.url === undefined &&
          source.urls === undefined &&
          source.tiles !== undefined
        ) {
          if (source.tiles.length === 1) {
            if (isLocalURL(source.tiles[0])) {
              const sourceID = source.tiles[0].split("/")[2];
              const sourceData = config.datas[sourceID];

              styleJSON.sources[id] = {
                ...sourceData.tileJSON,
                ...source,
                tiles: [source.tiles[0]],
              };
            }
          }
        }
      })
    );

    return styleJSON;
  } catch (error) {
    if (error.code === "ENOENT") {
      throw new Error("JSON does not exist");
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
    const stats = await stat(filePath);

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
  const stats = await stat(filePath);

  return stats.size;
}
