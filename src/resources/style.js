"use strict";

import { validateStyleMin } from "@maplibre/maplibre-gl-style-spec";
import { config } from "../configs/index.js";
import { readFile } from "node:fs/promises";
import { createCache } from "cache-manager";
import {
  removeFileWithLock,
  createFileWithLock,
  calculateMD5OfFile,
  getDataFromURL,
  getFileCreated,
  HTTP_SCHEMES,
  getFileSize,
  isLocalURL,
  printLog,
} from "../utils/index.js";

/* Cache in RAM */
const renderedStyleJSONCaches = createCache({
  ttl: 300000, // 5 mins
});

/*********************************** Style *************************************/

/**
 * Remove style data file with lock
 * @param {string} filePath Style file path to remove
 * @returns {Promise<void>}
 */
export async function removeStyleFile(filePath) {
  await removeFileWithLock(
    filePath,
    30000, // 30 seconds
  );
}

/**
 * Store style file
 * @param {string} filePath Style file path to store
 * @param {Buffer} data Tile data buffer
 * @returns {Promise<void>}
 */
export async function storeStyleFile(filePath, data) {
  await createFileWithLock(
    filePath,
    data,
    30000, // 30 seconds
  );
}

/**
 * Get StyleJSON buffer
 * @param {string} filePath Style file path to get
 * @returns {Promise<Buffer>}
 */
export async function getStyle(filePath) {
  try {
    return await readFile(filePath);
  } catch (error) {
    if (error.code === "ENOENT") {
      throw new Error("Not Found");
    }

    throw error;
  }
}

/**
 * Create rendered StyleJSON
 * @param {string} filePath Style file path to create rendered
 * @returns {Promise<object>}
 */
export async function createRenderedStyleJSON(filePath) {
  const styleJSON = JSON.parse(await readFile(filePath));

  await Promise.all(
    Object.keys(styleJSON.sources).map(async (id) => {
      const source = styleJSON.sources[id];

      if (source.tiles) {
        const tiles = new Set(
          source.tiles.map((tile) => {
            if (isLocalURL(tile)) {
              const sourceID = tile.split("/")[2];
              const sourceData = config.datas[sourceID];

              tile = `${sourceData.sourceType}://${sourceID}/{z}/{x}/{y}.${sourceData.tileJSON.format}`;
            }

            return tile;
          }),
        );

        source.tiles = Array.from(tiles);
      }

      if (source.urls) {
        const otherUrls = [];

        source.urls.forEach((url) => {
          if (isLocalURL(url)) {
            const sourceID = url.split("/")[2];
            const sourceData = config.datas[sourceID];

            const tile = `${sourceData.sourceType}://${sourceID}/{z}/{x}/{y}.${sourceData.tileJSON.format}`;

            if (source.tiles) {
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

        if (!otherUrls.length) {
          delete source.urls;
        } else {
          source.urls = otherUrls;
        }
      }

      if (source.url) {
        if (isLocalURL(source.url)) {
          const sourceID = source.url.split("/")[2];
          const sourceData = config.datas[sourceID];

          const tile = `${sourceData.sourceType}://${sourceID}/{z}/{x}/{y}.${sourceData.tileJSON.format}`;

          if (source.tiles) {
            if (!source.tiles.includes(tile)) {
              source.tiles.push(tile);
            }
          } else {
            source.tiles = [tile];
          }

          delete source.url;
        }
      }

      if (!source.url && !source.urls && source.tiles) {
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
    }),
  );

  return styleJSON;
}

/**
 * Get rendered StyleJSON
 * @param {string} filePath Style file path to render
 * @returns {Promise<object>}
 */
export async function getRenderedStyleJSON(filePath) {
  return await renderedStyleJSONCaches.wrap(filePath, async () => {
    try {
      return await createRenderedStyleJSON(filePath);
    } catch (error) {
      if (error.code === "ENOENT") {
        throw new Error("Not Found");
      }

      throw error;
    }
  });
}

/**
 * Get created time of Style file
 * @param {string} filePath Style file path to get
 * @returns {Promise<number>}
 */
export async function getStyleCreated(filePath) {
  return await getFileCreated(filePath);
}

/**
 * Get MD5 of Style
 * @param {string} filePath Style file path to get
 * @returns {Promise<string>}
 */
export async function getStyleMD5(filePath) {
  return await calculateMD5OfFile(filePath);
}

/**
 * Get the size of Style file
 * @param {string} filePath Style file path to get
 * @returns {Promise<number>}
 */
export async function getStyleSize(filePath) {
  return await getFileSize(filePath);
}

/**
 * Validate StyleJSON
 * @param {object|string} data StyleJSON or Style file path
 * @returns {Promise<void>}
 */
export async function validateStyle(data) {
  const styleJSON =
    typeof data === "object" ? data : JSON.parse(await readFile(data));

  /* Validate style */
  const validationErrors = validateStyleMin(styleJSON);
  if (validationErrors.length) {
    throw new Error(
      validationErrors
        .map((validationError) => `\n\t${validationError.message}`)
        .join(),
    );
  }

  /* Validate font */
  if (styleJSON.glyphs !== undefined) {
    if (styleJSON.glyphs.startsWith("fonts://")) {
    } else if (
      !HTTP_SCHEMES.some((scheme) => styleJSON.glyphs.startsWith(scheme))
    ) {
      throw new Error(`Invalid font url: "${styleJSON.glyphs}"`);
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
      !HTTP_SCHEMES.some((scheme) => styleJSON.sprite.startsWith(scheme))
    ) {
      throw new Error(`Invalid sprite url: "${styleJSON.sprite}"`);
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
              `Source "${id}" is not found data source "${parts[2]}"`,
            );
          }

          if (!config.geojsons[parts[2]][parts[3]]) {
            throw new Error(
              `Source "${id}" is not found data source "${parts[3]}"`,
            );
          }
        }
      }

      if (source.url !== undefined) {
        if (isLocalURL(source.url)) {
          const sourceID = source.url.split("/")[2];

          if (!config.datas[sourceID]) {
            throw new Error(
              `Source "${id}" is not found data source "${sourceID}"`,
            );
          }
        } else if (source.url.startsWith("data:")) {
        } else if (
          !HTTP_SCHEMES.some((scheme) => source.url.startsWith(scheme))
        ) {
          throw new Error(
            `Source "${id}" is invalid data url: "${source.url}"`,
          );
        }
      }

      if (source.urls !== undefined) {
        if (!source.urls.length) {
          throw new Error(
            `Source "${id}" is invalid data urls: "${source.urls}"`,
          );
        }

        source.urls.forEach((url) => {
          if (isLocalURL(url)) {
            const sourceID = url.split("/")[2];

            if (!config.datas[sourceID]) {
              throw new Error(
                `Source "${id}" is not found data source "${sourceID}"`,
              );
            }
          } else if (!HTTP_SCHEMES.some((scheme) => url.startsWith(scheme))) {
            throw new Error(`Source "${id}" is invalid data url: "${url}"`);
          }
        });
      }

      if (source.tiles !== undefined) {
        if (!source.tiles.length) {
          throw new Error(
            `Source "${id}" is invalid tile urls: "${source.tiles}"`,
          );
        }

        source.tiles.forEach((tile) => {
          if (isLocalURL(tile)) {
            const sourceID = tile.split("/")[2];

            if (!config.datas[sourceID]) {
              throw new Error(
                `Source "${id}" is not found data source "${sourceID}"`,
              );
            }
          } else if (!HTTP_SCHEMES.some((scheme) => tile.startsWith(scheme))) {
            throw new Error(`Source "${id}" is invalid tile url: "${tile}"`);
          }
        });
      }
    }),
  );
}

/**
 * Get and cache data StyleJSON
 * @param {string} id StyleJSON id
 * @returns {Promise<Buffer>}
 */
export async function getAndCacheDataStyleJSON(id) {
  const item = config.styles[id];
  if (!item) {
    throw new Error(`Style id "${id}" does not exist`);
  }

  try {
    return await getStyle(item.path);
  } catch (error) {
    if (item.sourceURL && error.message.includes("Not Found")) {
      printLog(
        "info",
        `Forwarding style id "${id}" - To "${item.sourceURL}"...`,
      );

      const styleJSON = await getDataFromURL(item.sourceURL, {
        method: "GET",
        responseType: "arraybuffer",
        timeout: 30000, // 30 seconds
        headers: item.headers,
        decompress: true,
      });

      if (item.storeCache) {
        printLog("info", `Caching style id "${id}" - File "${item.path}"...`);

        storeStyleFile(item.path, styleJSON).catch((error) =>
          printLog(
            "error",
            `Failed to cache style id "${id}" - File "${item.path}": ${error}`,
          ),
        );
      }

      return styleJSON;
    }

    throw error;
  }
}
