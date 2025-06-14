"use strict";

import { readFile, stat } from "node:fs/promises";
import { StatusCodes } from "http-status-codes";
import protobuf from "protocol-buffers";
import { printLog } from "./logger.js";
import cluster from "cluster";
import {
  removeFileWithLock,
  createFileWithLock,
  getDataFromURL,
  findFiles,
  retry,
} from "./utils.js";

let glyphsProto;

if (!cluster.isPrimary) {
  readFile("public/protos/glyphs.proto")
    .then((data) => {
      glyphsProto = protobuf(data);
    })
    .catch((error) => {
      printLog(
        "error",
        `Failed to load proto "public/protos/glyphs.proto": ${error}`
      );
    });
}

/**
 * Remove font file with lock
 * @param {string} filePath File path to remove font file
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<void>}
 */
export async function removeFontFile(filePath, timeout) {
  await removeFileWithLock(filePath, timeout);
}

/**
 * Cache font file
 * @param {string} sourcePath Font folder path
 * @param {string} fileName Font filename
 * @param {Buffer} data Font buffer
 * @returns {Promise<void>}
 */
export async function cacheFontFile(sourcePath, fileName, data) {
  await createFileWithLock(
    `${sourcePath}/${fileName}`,
    data,
    30000 // 30 secs
  );
}

/**
 * Download font file
 * @param {string} url The URL to download the file from
 * @param {string} id Font ID
 * @param {string} fileName Font filename
 * @param {number} maxTry Number of retry attempts on failure
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<void>}
 */
export async function downloadFontFile(url, id, fileName, maxTry, timeout) {
  await retry(async () => {
    try {
      // Get data from URL
      const response = await getDataFromURL(url, timeout, "arraybuffer");

      // Store data to file
      await cacheFontFile(
        `${process.env.DATA_DIR}/caches/fonts/${id}`,
        fileName,
        response.data
      );
    } catch (error) {
      printLog(
        "error",
        `Failed to download font "${fileName}" - From "${url}": ${error}`
      );

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
 * Get created of font
 * @param {string} filePath The path of the file
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
 * Validate font
 * @param {string} pbfDirPath PBF font dir path
 * @returns {Promise<void>}
 */
export async function validateFont(pbfDirPath) {
  const pbfFileNames = await findFiles(pbfDirPath, /^\d{1,5}-\d{1,5}\.pbf$/);

  if (pbfFileNames.length === 0) {
    throw new Error("Missing some PBF files");
  }
}

/**
 * Get font pbf
 * @param {string} dirPath Font dir path
 * @param {string} fileName Font file name
 * @returns {Promise<Buffer>}
 */
export async function getFont(dirPath, fileName) {
  try {
    return await readFile(`${dirPath}/${fileName}`);
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
  let fallbackFont = "Open Sans Regular";

  if (fontName.indexOf("Extrabold Italic")) {
    fallbackFont = "Open Sans Extrabold Italic";
  } else if (fontName.indexOf("Semibold Italic")) {
    fallbackFont = "Open Sans Semibold Italic";
  } else if (fontName.indexOf("Bold Italic")) {
    fallbackFont = "Open Sans Bold Italic";
  } else if (fontName.indexOf("Medium Italic")) {
    fallbackFont = "Open Sans Medium Italic";
  } else if (fontName.indexOf("Light Italic")) {
    fallbackFont = "Open Sans Light Italic";
  } else if (fontName.indexOf("Extrabold")) {
    fallbackFont = "Open Sans Extrabold";
  } else if (fontName.indexOf("Semibold")) {
    fallbackFont = "Open Sans Semibold";
  } else if (fontName.indexOf("Bold")) {
    fallbackFont = "Open Sans Bold";
  } else if (fontName.indexOf("Light")) {
    fallbackFont = "Open Sans Light";
  } else if (fontName.indexOf("Medium")) {
    fallbackFont = "Open Sans Medium";
  } else if (fontName.indexOf("Italic")) {
    fallbackFont = "Open Sans Italic";
  }

  return await readFile(`public/resources/fonts/${fallbackFont}/${fileName}`);
}

/**
 * Merge font datas
 * @param {Buffer[]} buffers Font buffers
 * @returns {Buffer}
 */
export function mergeFontDatas(buffers) {
  let result;
  const coverage = {};

  for (const buffer of buffers) {
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
 * Get the size of Font folder path
 * @param {string} pbfDirPath Font dir path
 * @returns {Promise<number>}
 */
export async function getFontSize(pbfDirPath) {
  const fileNames = await findFiles(
    pbfDirPath,
    /^\d{1,5}-\d{1,5}\.pbf$/,
    false,
    true
  );

  let size = 0;

  for (const fileName of fileNames) {
    const stats = await stat(fileName);

    size += stats.size;
  }

  return size;
}
