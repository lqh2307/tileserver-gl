"use strict";

import { DEFAULT_QUERY_TIMEOUT } from "../defaults/index.js";
import { config } from "../configs/index.js";
import { readFile } from "node:fs/promises";
import protobuf from "protocol-buffers";
import cluster from "node:cluster";
import path from "node:path";
import {
  calculateMD5OfFiles,
  removeFileWithLock,
  createFileWithLock,
  isErrorNotFound,
  getDataFromURL,
  getFileCreated,
  getFileSize,
  unzipAsync,
  findFiles,
  printLog,
} from "../utils/index.js";

const PBF_RANGE_FILE_REGEX = /^\d{1,5}-\d{1,5}\.pbf$/;

let glyphsProto;

if (!cluster.isPrimary) {
  readFile(path.join("public", "protos", "glyphs.proto"))
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

/*********************************** Font *************************************/

/**
 * Remove font file with lock
 * @param {string} filePath Font file path to remove
 * @returns {Promise<void>}
 */
export async function removeFontFile(filePath) {
  await removeFileWithLock(filePath, DEFAULT_QUERY_TIMEOUT);
}

/**
 * Store font file
 * @param {string} filePath Font file path to store
 * @param {Buffer} data Font buffer
 * @returns {Promise<void>}
 */
export async function storeFontFile(filePath, data) {
  await createFileWithLock(filePath, data, DEFAULT_QUERY_TIMEOUT);
}

/**
 * Get created time of font file
 * @param {string} pbfDirPath PBF font dir path to get
 * @returns {Promise<number>}
 */
export function getFontCreated(pbfDirPath) {
  return getFileCreated(pbfDirPath);
}

/**
 * Get MD5 of font
 * @param {string} pbfDirPath PBF font dir path to get
 * @returns {Promise<string>}
 */
export function getFontMD5(pbfDirPath) {
  return calculateMD5OfFiles(
    Array.from(
      {
        length: 256,
      },
      (_, idx) => {
        const rangeStart = idx * 256;
        const rangeEnd = rangeStart + 255;

        return path.join(pbfDirPath, `${rangeStart}-${rangeEnd}.pbf`);
      },
    ),
  );
}

/**
 * Get font buffer
 * @param {string} filePath Font file path to get
 * @returns {Promise<Buffer>}
 */
export async function getFont(filePath) {
  try {
    const data = await readFile(filePath);

    /* Unzip */
    if (
      (data[0] === 0x78 && data[1] === 0x9c) ||
      (data[0] === 0x1f && data[1] === 0x8b)
    ) {
      return await unzipAsync(data);
    }

    return data;
  } catch (error) {
    if (error.code === "ENOENT") {
      throw new Error("Not Found");
    }

    throw error;
  }
}

/**
 * Get fallback font pbf
 * @param {string} fontName Font name
 * @param {string} fileName Font file name
 * @returns {Promise<Buffer>}
 */
export function getFallbackFont(fontName, fileName) {
  let fallbackFont = "Open Sans";
  fontName = fontName.toLowerCase();

  if (fontName.includes("extrabold")) {
    fallbackFont += " Extrabold";
  } else if (fontName.includes("semibold")) {
    fallbackFont += " Semibold";
  } else if (fontName.includes("bold")) {
    fallbackFont += " Bold";
  } else if (fontName.includes("medium")) {
    fallbackFont += " Medium";
  } else if (fontName.includes("light")) {
    fallbackFont += " Light";
  }

  if (fontName.includes("italic")) {
    fallbackFont += " Italic";
  }

  if (fallbackFont === "Open Sans") {
    fallbackFont += " Regular";
  }

  return readFile(
    path.join("public", "resources", "fonts", fallbackFont, fileName),
  );
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

  result.stacks[0].glyphs.sort((a, b) => {
    return a.id - b.id;
  });

  return glyphsProto.glyphs.encode(result);
}

/**
 * Validate PBF font
 * @param {string} pbfDirPath PBF font dir path to validate
 * @returns {Promise<void>}
 */
export async function validatePBFFont(pbfDirPath) {
  const pbfFileNames = await findFiles(pbfDirPath, PBF_RANGE_FILE_REGEX);

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
    PBF_RANGE_FILE_REGEX,
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
 * Get and cache data Fonts
 * @param {string} ids Font ids
 * @param {string} fileName Font file name
 * @returns {Promise<Buffer>}
 */
export async function getAndCacheDataFonts(ids, fileName) {
  /* Get font datas */
  const buffers = await Promise.all(
    ids.split(",").map(async (id) => {
      const item = config.fonts[id];
      if (!item) {
        printLog(
          "warn",
          `Font id "${id}" does not exist. Using fallback font "Open Sans"...`,
        );

        return await getFallbackFont(id, fileName);
      }

      const filePath = path.join(item.path, fileName);

      try {
        return await getFont(filePath);
      } catch (error) {
        try {
          if (item.sourceURL && isErrorNotFound(error)) {
            const targetURL = item.sourceURL.replace("{range}.pbf", fileName);

            printLog(
              "info",
              `Forwarding font id "${id}" - Filename "${fileName}" - To "${targetURL}"...`,
            );

            /* Get font */
            const font = await getDataFromURL(targetURL, {
              method: "GET",
              responseType: "arraybuffer",
              timeout: DEFAULT_QUERY_TIMEOUT,
              headers: item.headers,
              decompress: true,
            });

            /* Cache */
            if (item.storeCache) {
              printLog(
                "info",
                `Caching font id "${id}" - Filename "${fileName}"...`,
              );

              storeFontFile(filePath, font).catch((error) => {
                return printLog(
                  "error",
                  `Failed to cache font id "${id}" - Filename "${fileName}": ${error}`,
                );
              });
            }

            return font;
          }

          throw error;
        } catch (error) {
          printLog(
            "warn",
            `Failed to get font id "${id}": ${error}. Using fallback font "Open Sans"...`,
          );

          return await getFallbackFont(id, fileName);
        }
      }
    }),
  );

  /* Merge font datas */
  return mergePBFFontDatas(buffers);
}
