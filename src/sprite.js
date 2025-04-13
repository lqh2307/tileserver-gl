"use strict";

import { StatusCodes } from "http-status-codes";
import fsPromise from "node:fs/promises";
import { printLog } from "./logger.js";
import {
  getPNGImageMetadata,
  removeFileWithLock,
  createFileWithLock,
  getDataFromURL,
  getJSONSchema,
  validateJSON,
  findFiles,
  retry,
} from "./utils.js";

/**
 * Remove sprite file with lock
 * @param {string} filePath File path to remove sprite file
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<void>}
 */
export async function removeSpriteFile(filePath, timeout) {
  await removeFileWithLock(filePath, timeout);
}

/**
 * Cache sprite file
 * @param {string} sourcePath Sprite folder path
 * @param {string} fileName Sprite file name
 * @param {Buffer} data Sprite buffer
 * @returns {Promise<void>}
 */
export async function cacheSpriteFile(sourcePath, fileName, data) {
  await createFileWithLock(
    `${sourcePath}/${fileName}`,
    data,
    300000 // 5 mins
  );
}

/**
 * Download sprite file
 * @param {string} url The URL to download the file from
 * @param {string} id Font ID
 * @param {string} fileName Sprite file name
 * @param {number} maxTry Number of retry attempts on failure
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<void>}
 */
export async function downloadSpriteFile(url, id, fileName, maxTry, timeout) {
  await retry(async () => {
    try {
      // Get data from URL
      const response = await getDataFromURL(url, timeout, "arraybuffer");

      // Store data to file
      await cacheSpriteFile(
        `${process.env.DATA_DIR}/caches/sprites/${id}`,
        fileName,
        response.data
      );
    } catch (error) {
      printLog(
        "error",
        `Failed to download sprite file "${fileName}" - From "${url}": ${error}`
      );

      if (error.statusCode !== undefined) {
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
 * Get created of sprite
 * @param {string} filePath The path of the file
 * @returns {Promise<number>}
 */
export async function getSpriteCreated(filePath) {
  try {
    const stats = await fsPromise.stat(filePath);

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
 * Validate sprite
 * @param {string} spriteDirPath Sprite dir path
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
    (jsonSpriteFileName) => jsonSpriteFileName.split(".")[0]
  );

  await Promise.all(
    fileNameWoExts.map(async (fileNameWoExt) => {
      /* Validate JSON sprite */
      validateJSON(
        await getJSONSchema("sprite"),
        JSON.parse(await fsPromise.readFile(`${fileNameWoExt}.json`, "utf8"))
      );

      /* Validate PNG sprite */
      const pngMetadata = await getPNGImageMetadata(`${fileNameWoExt}.png`);

      if (pngMetadata.format !== "png") {
        throw new Error("Invalid PNG file");
      }
    })
  );
}

/**
 * Get sprite
 * @param {string} id Sprite ID
 * @param {string} fileName Sprite file name
 * @returns {Promise<Buffer>}
 */
export async function getSprite(id, fileName) {
  return await fsPromise.readFile(
    `${process.env.DATA_DIR}/sprites/${id}/${fileName}`
  );
}

/**
 * Get the size of Sprite folder path
 * @param {string} spriteDirPath Sprite dir path
 * @returns {Promise<number>}
 */
export async function getSpriteSize(spriteDirPath) {
  const fileNames = await findFiles(
    spriteDirPath,
    /^sprite(@\d+x)?\.(json|png)$/,
    false,
    true
  );

  let size = 0;

  for (const fileName of fileNames) {
    const stat = await fsPromise.stat(fileName);

    size += stat.size;
  }

  return size;
}

/**
 * Get sprite from a URL
 * @param {string} url The URL to fetch data from
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<Object>}
 */
export async function getSpriteFromURL(url, timeout) {
  try {
    const response = await getDataFromURL(url, timeout, "arraybuffer");

    return response.data;
  } catch (error) {
    if (error.statusCode !== undefined) {
      if (
        error.statusCode === StatusCodes.NO_CONTENT ||
        error.statusCode === StatusCodes.NOT_FOUND
      ) {
        throw new Error("Sprite does not exist");
      } else {
        throw new Error(`Failed to get sprite from "${url}": ${error}`);
      }
    } else {
      throw new Error(`Failed to get sprite from "${url}": ${error}`);
    }
  }
}
