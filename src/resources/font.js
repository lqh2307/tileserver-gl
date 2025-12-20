"use strict";

import { readFile, stat } from "node:fs/promises";
import { StatusCodes } from "http-status-codes";
import { config } from "../configs/index.js";
import protobuf from "protocol-buffers";
import cluster from "cluster";
import {
  removeFileWithLock,
  createFileWithLock,
  getDataFileFromURL,
  getDataFromURL,
  findFiles,
  printLog,
  retry,
} from "../utils/index.js";

let glyphsProto;

if (!cluster.isPrimary) {
  readFile("public/protos/glyphs.proto")
    .then((data) => {
      glyphsProto = protobuf(data);
    })
    .catch((error) => {
      printLog(
        "error",
        `Failed to load proto "public/protos/glyphs.proto": ${error}`,
      );
    });
}

/**
 * Remove font file with lock
 * @param {string} filePath Font file path to remove
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<void>}
 */
export async function removeFontFile(filePath, timeout) {
  await removeFileWithLock(filePath, timeout);
}

/**
 * Cache font file
 * @param {string} filePath Font file path to store
 * @param {Buffer} data Font buffer
 * @returns {Promise<void>}
 */
export async function cacheFontFile(filePath, data) {
  await createFileWithLock(
    filePath,
    data,
    30000, // 30 seconds
  );
}

/**
 * Download font file
 * @param {string} url The URL to download the file from
 * @param {string} filePath Font file path to store
 * @param {number} maxTry Number of retry attempts on failure
 * @param {number} timeout Timeout in milliseconds
 * @param {object} headers Headers
 * @returns {Promise<void>}
 */
export async function downloadFontFile(
  url,
  filePath,
  maxTry,
  timeout,
  headers,
) {
  await retry(async () => {
    try {
      // Get data from URL
      const response = await getDataFromURL(
        url,
        timeout,
        "arraybuffer",
        false,
        headers,
      );

      // Store data to file
      await cacheFontFile(filePath, response.data);
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
 * Get created time of font file
 * @param {string} filePath Font file path to get
 * @returns {Promise<number>}
 */
export async function getFontCreated(filePath) {
  try {
    const stats = await stat(filePath);

    return stats.ctimeMs;
  } catch (error) {
    if (error.code === "ENOENT") {
      throw new Error("Font created does not exist");
    } else {
      throw error;
    }
  }
}

/**
 * Get font buffer
 * @param {string} filePath Font file path to get
 * @returns {Promise<Buffer>}
 */
export async function getFont(filePath) {
  try {
    return await readFile(filePath);
  } catch (error) {
    if (error.code === "ENOENT") {
      throw new Error("Font does not exist");
    } else {
      throw error;
    }
  }
}

/**
 * Get fallback font pbf
 * @param {string} fontName Font name
 * @param {string} fileName Font file name
 * @returns {Promise<Buffer>}
 */
export async function getFallbackFont(fontName, fileName) {
  let fallbackFont = "Open Sans";
  fontName = fontName.toLowerCase();

  if (fontName.indexOf("extrabold")) {
    fallbackFont += " Extrabold";
  } else if (fontName.indexOf("semibold")) {
    fallbackFont += " Semibold";
  } else if (fontName.indexOf("bold")) {
    fallbackFont += " Bold";
  } else if (fontName.indexOf("medium")) {
    fallbackFont += " Medium";
  } else if (fontName.indexOf("light")) {
    fallbackFont += " Light";
  }

  if (fontName.indexOf("italic")) {
    fallbackFont += " Italic";
  }

  if (fallbackFont === "Open Sans") {
    fallbackFont += " Regular";
  }

  return await readFile(`public/resources/fonts/${fallbackFont}/${fileName}`);
}

/**
 * Merge PBF font datas
 * @param {Buffer[]} pbfBuffers PBF font buffers
 * @returns {Buffer}
 */
export function mergePBFFontDatas(pbfBuffers) {
  let result;
  const coverage = {};

  for (const buffer of pbfBuffers) {
    const decoded = glyphsProto.glyphs.decode(buffer);
    const glyphs = decoded.stacks[0].glyphs;

    if (result === undefined) {
      for (const glyph of glyphs) {
        coverage[glyph.id] = true;
      }

      result = decoded;
    } else {
      for (const glyph of glyphs) {
        if (coverage[glyph.id] === undefined) {
          result.stacks[0].glyphs.push(glyph);

          coverage[glyph.id] = true;
        }
      }

      result.stacks[0].name += "," + decoded.stacks[0].name;
    }
  }

  result.stacks[0].glyphs.sort((a, b) => a.id - b.id);

  return glyphsProto.glyphs.encode(result);
}

/**
 * Validate PBF font
 * @param {string} pbfDirPath PBF font dir path to validate
 * @returns {Promise<void>}
 */
export async function validatePBFFont(pbfDirPath) {
  const pbfFileNames = await findFiles(pbfDirPath, /^\d{1,5}-\d{1,5}\.pbf$/);

  if (!pbfFileNames.length) {
    throw new Error("Missing some PBF files");
  }
}

/**
 * Get the size of PBF font folder path
 * @param {string} pbfDirPath PBF font dir path to get
 * @returns {Promise<number>}
 */
export async function getPBFFontSize(pbfDirPath) {
  const fileNames = await findFiles(
    pbfDirPath,
    /^\d{1,5}-\d{1,5}\.pbf$/,
    false,
    true,
  );

  let size = 0;

  for (const fileName of fileNames) {
    const stats = await stat(fileName);

    size += stats.size;
  }

  return size;
}

/**
 * Get and cache data Fonts
 * @param {string} ids Font ids
 * @param {string} fileName Font file name
 * @returns {Promise<object>}
 */
export async function getAndCacheDataFonts(ids, fileName) {
  /* Get font datas */
  const buffers = await Promise.all(
    ids.split(",").map(async (id) => {
      const item = config.fonts[id];

      try {
        if (!item) {
          throw new Error("Font does not exist");
        }

        return await getFont(`${item.path}/${fileName}`);
      } catch (error) {
        try {
          if (item?.sourceURL && error.message === "Font does not exist") {
            const targetURL = item.sourceURL.replace("{range}.pbf", fileName);

            printLog(
              "info",
              `Forwarding font "${id}" - Filename "${fileName}" - To "${targetURL}"...`,
            );

            /* Get font */
            const font = await getDataFileFromURL(
              targetURL,
              item.headers,
              30000, // 30 seconds
            );

            /* Cache */
            if (item.storeCache) {
              printLog(
                "info",
                `Caching font "${id}" - Filename "${fileName}"...`,
              );

              cacheFontFile(`${item.path}/${fileName}`, font).catch((error) =>
                printLog(
                  "error",
                  `Failed to cache font "${id}" - Filename "${fileName}": ${error}`,
                ),
              );
            }

            return font;
          } else {
            throw error;
          }
        } catch (error) {
          printLog(
            "warn",
            `Failed to get font "${id}": ${error}. Using fallback font "Open Sans"...`,
          );

          return await getFallbackFont(id, fileName);
        }
      }
    }),
  );

  /* Merge font datas */
  return mergePBFFontDatas(buffers);
}
