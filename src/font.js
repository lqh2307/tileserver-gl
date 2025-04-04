"use strict";

import { StatusCodes } from "http-status-codes";
import fsPromise from "node:fs/promises";
import protobuf from "protocol-buffers";
import { printLog } from "./logger.js";
import { config } from "./config.js";
import fs from "node:fs";
import {
  removeFileWithLock,
  createFileWithLock,
  getDataFromURL,
  findFiles,
  retry,
} from "./utils.js";

const glyphsProto = protobuf(fs.readFileSync("public/protos/glyphs.proto"));

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
 * @param {string} range Fontstack range
 * @param {Buffer} data Font buffer
 * @returns {Promise<void>}
 */
export async function cacheFontFile(sourcePath, range, data) {
  await createFileWithLock(
    `${sourcePath}/${range}.pbf`,
    data,
    300000 // 5 mins
  );
}

/**
 * Download font file
 * @param {string} url The URL to download the file from
 * @param {string} id Font ID
 * @param {string} range Fontstack range
 * @param {number} maxTry Number of retry attempts on failure
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<void>}
 */
export async function downloadFontFile(url, id, range, maxTry, timeout) {
  await retry(async () => {
    try {
      // Get data from URL
      const response = await getDataFromURL(url, timeout, "arraybuffer");

      // Store data to file
      await cacheFontFile(
        `${process.env.DATA_DIR}/caches/fonts/${id}`,
        range,
        response.data
      );
    } catch (error) {
      printLog(
        "error",
        `Failed to download font range "${range}" - From "${url}": ${error}`
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
 * Get created of font
 * @param {string} filePath The path of the file
 * @returns {Promise<number>}
 */
export async function getFontCreated(filePath) {
  try {
    const stats = await fsPromise.stat(filePath);

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
  const pbfFileNames = await findFiles(
    pbfDirPath,
    /^\d{1,5}-\d{1,5}\.pbf$/,
    false,
    false
  );

  if (pbfFileNames.length === 0) {
    throw new Error("Missing some PBF files");
  }
}

/**
 * Get fonts pbf
 * @param {string} ids Font IDs
 * @param {string} fileName Font file name
 * @returns {Promise<Buffer>}
 */
export async function getFonts(ids, fileName) {
  /* Get font datas */
  const buffers = await Promise.all(
    ids.split(",").map(async (font) => {
      try {
        /* Check font is exist? */
        if (config.fonts[font] === undefined) {
          throw new Error("Font does not exist");
        }

        return await fsPromise.readFile(
          `${process.env.DATA_DIR}/fonts/${font}/${fileName}`
        );
      } catch (error) {
        printLog(
          "warn",
          `Failed to get font "${font}": ${error}. Using fallback font "Open Sans Regular"...`
        );

        return await fsPromise.readFile(
          `public/resources/fonts/Open Sans Regular/${fileName}`
        );
      }
    })
  );

  /* Merge font datas */
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

      result.stacks[0].name += ", " + decoded.stacks[0].name;
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
    const stat = await fsPromise.stat(fileName);

    size += stat.size;
  }

  return size;
}
