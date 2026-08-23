"use strict";

import { DEFAULT_QUERY_TIMEOUT } from "../defaults/index.js";
import { config } from "../configs/index.js";
import { readFile } from "node:fs/promises";
import path from "node:path";
import {
  calculateMD5OfFiles,
  removeFileWithLock,
  createFileWithLock,
  getImageMetadata,
  isErrorNotFound,
  getDataFromURL,
  getFileCreated,
  getJSONSchema,
  validateJSON,
  getFileSize,
  findFiles,
  printLog,
} from "../utils/index.js";

/** Supported file formats in a MapLibre sprite bundle. @type {Set<string>} */
export const SPRITE_FORMATS = new Set(["json", "png"]);

const ALL_SPRITE_FILE_REGEX = /^sprite(@\d+x)?\.(json|png)$/;
const JSON_SPRITE_FILE_REGEX = /^sprite(@\d+x)?\.json$/;
const PNG_SPRITE_FILE_REGEX = /^sprite(@\d+x)?\.png$/;

/*********************************** Sprite *************************************/

/**
 * Remove sprite file with lock
 * @param {string} filePath Sprite file path to remove
 * @returns {Promise<void>}
 */
export async function removeSpriteFile(filePath) {
  await removeFileWithLock(filePath, DEFAULT_QUERY_TIMEOUT);
}

/**
 * Store sprite file
 * @param {string} filePath Sprite file path to store
 * @param {Buffer} data Sprite buffer
 * @returns {Promise<void>}
 */
export async function storeSpriteFile(filePath, data) {
  await createFileWithLock(filePath, data, DEFAULT_QUERY_TIMEOUT);
}

/**
 * Get created time of sprite file
 * @param {string} spriteDirPath Sprite dir path to get
 * @returns {Promise<number>}
 */
export function getSpriteCreated(spriteDirPath) {
  return getFileCreated(spriteDirPath);
}

/**
 * Get MD5 of sprite
 * @param {string} spriteDirPath Sprite dir path to get
 * @returns {Promise<string>}
 */
export function getSpriteMD5(spriteDirPath) {
  return calculateMD5OfFiles([
    path.join(spriteDirPath, "sprite.json"),
    path.join(spriteDirPath, "sprite.png"),
    path.join(spriteDirPath, "sprite@2x.json"),
    path.join(spriteDirPath, "sprite@2x.png"),
  ]);
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
      throw new Error("Not Found");
    }

    throw error;
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
    ALL_SPRITE_FILE_REGEX,
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
    findFiles(spriteDirPath, JSON_SPRITE_FILE_REGEX, false, true),
    findFiles(spriteDirPath, PNG_SPRITE_FILE_REGEX, false, true),
  ]);

  if (jsonSpriteFileNames.length !== pngSpriteNames.length) {
    throw new Error("Missing some JSON or PNG files");
  }

  const fileNameWoExts = jsonSpriteFileNames.map((jsonSpriteFileName) => {
    return path.join(
      path.dirname(jsonSpriteFileName),
      path.basename(jsonSpriteFileName, ".json"),
    );
  });

  await Promise.all(
    fileNameWoExts.map(async (fileNameWoExt) => {
      /* Validate JSON sprite */
      validateJSON(
        await getJSONSchema("sprite"),
        JSON.parse(
          await readFile(
            path.join(
              path.dirname(fileNameWoExt),
              `${path.basename(fileNameWoExt)}.json`,
            ),
            "utf8",
          ),
        ),
      );

      /* Validate PNG sprite */
      const pngMetadata = await getImageMetadata(
        path.join(
          path.dirname(fileNameWoExt),
          `${path.basename(fileNameWoExt)}.png`,
        ),
      );

      if (pngMetadata.format !== "png") {
        throw new Error(`Invalid PNG file: ${fileNameWoExt}.png`);
      }
    }),
  );
}

/**
 * Get and cache data Sprite
 * @param {string} id Sprite id
 * @param {string} fileName Sprite file name
 * @returns {Promise<Buffer>}
 */
export async function getAndCacheDataSprite(id, fileName) {
  const item = config.sprites[id];
  if (!item) {
    throw new Error(`Sprite id "${id}" does not exist`);
  }

  const filePath = path.join(item.path, fileName);

  try {
    return await getSprite(filePath);
  } catch (error) {
    if (item.sourceURL && isErrorNotFound(error)) {
      const targetURL = item.sourceURL.replace("{name}", `${fileName}`);

      printLog(
        "info",
        `Forwarding sprite id "${id}" - Filename "${fileName}" - To "${targetURL}"...`,
      );

      /* Get sprite */
      const sprite = await getDataFromURL(targetURL, {
        method: "GET",
        responseType: "arraybuffer",
        timeout: DEFAULT_QUERY_TIMEOUT,
        headers: item.headers,
        decompress: fileName.endsWith(".json") ? true : false,
      });

      /* Cache */
      if (item.storeCache) {
        printLog(
          "info",
          `Caching sprite id "${id}" - Filename "${fileName}"...`,
        );

        storeSpriteFile(filePath, sprite).catch((error) => {
          return printLog(
            "error",
            `Failed to cache sprite id "${id}" - Filename "${fileName}": ${error}`,
          );
        });
      }

      return sprite;
    }

    throw error;
  }
}
