"use strict";

import { readFile, stat } from "node:fs/promises";
import { StatusCodes } from "http-status-codes";
import { config } from "../configs/index.js";
import {
  removeFileWithLock,
  createFileWithLock,
  getDataFileFromURL,
  getImageMetadata,
  getJSONSchema,
  validateJSON,
  requestToURL,
  getFileSize,
  findFiles,
  printLog,
  retry,
} from "../utils/index.js";

/**
 * Remove sprite file with lock
 * @param {string} filePath Sprite file path to remove
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<void>}
 */
export async function removeSpriteFile(filePath, timeout) {
  await removeFileWithLock(filePath, timeout);
}

/**
 * Store sprite file
 * @param {string} filePath Sprite file path to store
 * @param {Buffer} data Sprite buffer
 * @returns {Promise<void>}
 */
export async function storeSpriteFile(filePath, data) {
  await createFileWithLock(
    filePath,
    data,
    30000, // 30 seconds
  );
}

/**
 * Download sprite file
 * @param {string} url The URL to download the file from
 * @param {string} filePath Sprite file path to store
 * @param {number} maxTry Number of retry attempts on failure
 * @param {number} timeout Timeout in milliseconds
 * @param {object} headers Headers
 * @returns {Promise<void>}
 */
export async function downloadSpriteFile(
  url,
  filePath,
  maxTry,
  timeout,
  headers,
) {
  await retry(async () => {
    try {
      // Get data from URL
      const response = await requestToURL({
        url: url,
        method: "GET",
        timeout: timeout,
        responseType: "arraybuffer",
        headers: headers,
      });

      // Store data to file
      await storeSpriteFile(filePath, response.data);
    } catch (error) {
      if (error.statusCode) {
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
 * Get created time of sprite file
 * @param {string} filePath Sprite file path to get
 * @returns {Promise<number>}
 */
export async function getSpriteCreated(filePath) {
  try {
    const stats = await stat(filePath);

    return stats.ctimeMs;
  } catch (error) {
    if (error.code === "ENOENT") {
      throw new Error("Sprite created does not exist");
    } else {
      throw error;
    }
  }
}

/**
 * Get sprite buffer
 * @param {string} filePath Sprite file path to get
 * @returns {Promise<Buffer>}
 */
export async function getSprite(filePath) {
  try {
    return await readFile(filePath);
  } catch (error) {
    if (error.code === "ENOENT") {
      throw new Error("Sprite does not exist");
    } else {
      throw error;
    }
  }
}

/**
 * Get the size of Sprite folder path
 * @param {string} spriteDirPath Sprite dir path to get
 * @returns {Promise<number>}
 */
export async function getSpriteSize(spriteDirPath) {
  const fileNames = await findFiles(
    spriteDirPath,
    /^sprite(@\d+x)?\.(json|png)$/,
    false,
    true,
  );

  let size = 0;

  for (const fileName of fileNames) {
    size += await getFileSize(fileName);
  }

  return size;
}

/**
 * Validate sprite
 * @param {string} spriteDirPath Sprite dir path to validate
 * @returns {Promise<void>}
 */
export async function validateSprite(spriteDirPath) {
  const [jsonSpriteFileNames, pngSpriteNames] = await Promise.all([
    findFiles(spriteDirPath, /^sprite(@\d+x)?\.json$/, false, true),
    findFiles(spriteDirPath, /^sprite(@\d+x)?\.png$/, false, true),
  ]);

  if (jsonSpriteFileNames.length !== pngSpriteNames.length) {
    throw new Error("Missing some JSON or PNG files");
  }

  const fileNameWoExts = jsonSpriteFileNames.map(
    (jsonSpriteFileName) => jsonSpriteFileName.split(".")[0],
  );

  await Promise.all(
    fileNameWoExts.map(async (fileNameWoExt) => {
      /* Validate JSON sprite */
      validateJSON(
        await getJSONSchema("sprite"),
        JSON.parse(await readFile(`${fileNameWoExt}.json`, "utf8")),
      );

      /* Validate PNG sprite */
      const pngMetadata = await getImageMetadata(`${fileNameWoExt}.png`);

      if (pngMetadata.format !== "png") {
        throw new Error("Invalid PNG file");
      }
    }),
  );
}

/**
 * Get and cache data Sprite
 * @param {string} id Sprite id
 * @param {string} fileName Sprite file name
 * @returns {Promise<object>}
 */
export async function getAndCacheDataSprite(id, fileName) {
  const item = config.sprites[id];
  if (!item) {
    throw new Error("Sprite source does not exist");
  }

  const filePath = `${item.path}/${fileName}`;

  try {
    return await getSprite(filePath);
  } catch (error) {
    if (item.sourceURL && error.message === "Sprite does not exist") {
      const targetURL = item.sourceURL.replace("{name}", `${fileName}`);

      printLog(
        "info",
        `Forwarding sprite "${id}" - Filename "${fileName}" - To "${targetURL}"...`,
      );

      /* Get sprite */
      const sprite = await getDataFileFromURL(
        targetURL,
        item.headers,
        30000, // 30 seconds
      );

      /* Cache */
      if (item.storeCache) {
        printLog("info", `Caching sprite "${id}" - Filename "${fileName}"...`);

        storeSpriteFile(filePath, sprite).catch((error) =>
          printLog(
            "error",
            `Failed to cache sprite "${id}" - Filename "${fileName}": ${error}`,
          ),
        );
      }

      return sprite;
    } else {
      throw error;
    }
  }
}
