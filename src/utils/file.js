"use strict";

import { delay, detectContentTypeFromFormat } from "./util.js";
import { createReadStream, createWriteStream } from "node:fs";
import archiver from "archiver";
import path from "node:path";
import crypto from "crypto";
import zlib from "zlib";
import util from "util";
import {
  writeFile,
  readdir,
  rename,
  mkdir,
  stat,
  open,
  rm,
} from "node:fs/promises";

/**
 * Calculate MD5 hash of a buffer
 * @param {Buffer} buffer The data buffer
 * @returns {string} The MD5 hash
 */
export function calculateMD5(buffer) {
  return crypto.createHash("md5").update(buffer).digest("hex");
}

/**
 * Calculate MD5 hash of a file
 * @param {string} filePath The data file path
 * @returns {Promise<string>} The MD5 hash
 */
export async function calculateMD5OfFile(filePath) {
  try {
    return new Promise((resolve, reject) => {
      const hash = crypto.createHash("md5");

      createReadStream(filePath)
        .on("error", reject)
        .on("data", (chunk) => hash.update(chunk))
        .on("end", () => resolve(hash.digest("hex")));
    });
  } catch (error) {
    if (error.code === "ENOENT") {
      throw new Error("File does not exist");
    } else {
      throw error;
    }
  }
}

/**
 * Create base64 string from buffer
 * @param {Buffer} buffer Input data
 * @param {format} format Data format
 * @returns {string} Base64 string
 */
export function createBase64(buffer, format) {
  return `data:${detectContentTypeFromFormat(format)};base64,${buffer.toString(
    "base64"
  )}`;
}

/**
 * Create folders
 * @param {string[]} dirPaths Create folders
 * @returns {Promise<void>}
 */
export async function createFolders(dirPaths) {
  await Promise.all(
    dirPaths.map((dirPath) =>
      mkdir(dirPath, {
        recursive: true,
      })
    )
  );
}

/**
 * Remove files or folders
 * @param {string[]} dirPaths File or folder paths
 * @returns {Promise<void>}
 */
export async function removeFolders(dirPaths) {
  await Promise.all(
    dirPaths.map((dirPath) =>
      rm(dirPath, {
        force: true,
        recursive: true,
      })
    )
  );
}

/**
 * Recursively removes empty folders in a directory
 * @param {string} folderPath The root directory to check for empty folders
 * @param {RegExp} regex The regex to match files
 * @returns {Promise<void>}
 */
export async function removeEmptyFolders(folderPath, regex) {
  const entries = await readdir(folderPath, {
    withFileTypes: true,
  });

  let hasMatchingFile = false;

  await Promise.all(
    entries.map(async (entry) => {
      const fullPath = `${folderPath}/${entry.name}`;

      if (entry.isFile() && (regex === undefined || regex.test(entry.name))) {
        hasMatchingFile = true;
      } else if (entry.isDirectory()) {
        await removeEmptyFolders(fullPath, regex);

        const subEntries = await readdir(fullPath).catch(() => []);
        if (subEntries.length) {
          hasMatchingFile = true;
        }
      }
    })
  );

  if (!hasMatchingFile) {
    await rm(folderPath, {
      recursive: true,
      force: true,
    });
  }
}

/**
 * Recursively removes old locks
 * @returns {Promise<void>}
 */
export async function removeOldLocks() {
  const fileNames = await findFiles(
    `${process.env.DATA_DIR}`,
    /^.*\.lock$/,
    true,
    true
  );

  await removeFolders(fileNames);
}

/**
 * Check file or folder is exist?
 * @param {string} fileOrDirPath File or directory path
 * @param {string} isDir Is Folder?
 * @returns {Promise<boolean>}
 */
export async function isExistFile(fileOrDirPath, isDir) {
  try {
    const stats = await stat(fileOrDirPath);

    return isDir ? stats.isDirectory() : stats.isFile();
  } catch (error) {
    if (error.code === "ENOENT") {
      return false;
    } else {
      throw error;
    }
  }
}

/**
 * Find matching files or folders in a directory
 * @param {string} filePath Directory path
 * @param {RegExp} regex The regex to match file or folder names
 * @param {boolean} recurse Whether to search recursively in subdirectories
 * @param {boolean} includeDirPath Whether to include full directory path in results
 * @param {boolean} isDir If true, search for directories; otherwise, search for files
 * @returns {Promise<string[]>} Array of matching file or folder paths
 */
export async function findFiles(
  filePath,
  regex,
  recurse,
  includeDirPath,
  isDir
) {
  const entries = await readdir(filePath, {
    withFileTypes: true,
  });

  const results = [];

  for (const entry of entries) {
    const fullPath = `${filePath}/${entry.name}`;
    const isDirectory = entry.isDirectory();

    if (isDir) {
      if (isDirectory && regex.test(entry.name)) {
        results.push(includeDirPath ? fullPath : entry.name);
      }
    } else {
      if (!isDirectory && regex.test(entry.name)) {
        results.push(includeDirPath ? fullPath : entry.name);
      }
    }

    if (isDirectory && recurse) {
      const subEntries = await findFiles(
        fullPath,
        regex,
        recurse,
        includeDirPath,
        isDir
      );

      if (includeDirPath) {
        subEntries.forEach((sub) => results.push(`${fullPath}/${sub}`));
      } else {
        subEntries.forEach((sub) => results.push(`${entry.name}/${sub}`));
      }
    }
  }

  return results;
}

/**
 * Compress data using gzip algorithm asynchronously
 * @param {Buffer|string} input The data to compress
 * @param {zlib.ZlibOptions} options Optional zlib compression options
 * @returns {Promise<Buffer>} A Promise that resolves to the compressed data as a Buffer
 */
export const gzipAsync = util.promisify(zlib.gzip);

/**
 * Decompress gzip-compressed data asynchronously
 * @param {Buffer|string} input The compressed data to decompress
 * @param {zlib.ZlibOptions} options Optional zlib decompression options
 * @returns {Promise<Buffer>} A Promise that resolves to the decompressed data as a Buffer
 */
export const unzipAsync = util.promisify(zlib.unzip);

/**
 * Decompress deflate-compressed data asynchronously
 * @param {Buffer|string} input The compressed data to decompress
 * @param {zlib.ZlibOptions} options Optional zlib decompression options
 * @returns {Promise<Buffer>} A Promise that resolves to the decompressed data as a Buffer
 */
export const inflateAsync = util.promisify(zlib.inflate);

/**
 * Compress zip folder
 * @param {string} iDirPath The input dir path
 * @param {string} oFilePath The output file path
 * @returns {Promise<void>}
 */
export async function zipFolder(iDirPath, oFilePath) {
  await mkdir(path.dirname(oFilePath), {
    recursive: true,
  });

  return new Promise((resolve, reject) => {
    const output = createWriteStream(oFilePath);
    const archive = archiver("zip", {
      zlib: {
        level: 9,
      },
    });

    output.on("close", resolve);

    archive.on("error", reject);

    archive.pipe(output);

    archive.directory(iDirPath);

    archive.finalize();
  });
}

/**
 * Create file with lock
 * @param {string} filePath File path to store
 * @param {Buffer} data Data buffer
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<void>}
 */
export async function createFileWithLock(filePath, data, timeout) {
  const startTime = Date.now();

  const lockFilePath = `${filePath}.lock`;
  let lockFileHandle;

  while (Date.now() - startTime <= timeout) {
    try {
      lockFileHandle = await open(lockFilePath, "wx");

      const tempFilePath = `${filePath}.tmp`;

      try {
        await mkdir(path.dirname(filePath), {
          recursive: true,
        });

        await writeFile(tempFilePath, data);

        await rename(tempFilePath, filePath);
      } catch (error) {
        await rm(tempFilePath, {
          force: true,
        });

        throw error;
      }

      await lockFileHandle.close();

      return await rm(lockFilePath, {
        force: true,
      });
    } catch (error) {
      if (error.code === "ENOENT") {
        await mkdir(path.dirname(filePath), {
          recursive: true,
        });

        continue;
      } else if (error.code === "EEXIST") {
        await delay(25);
      } else {
        if (lockFileHandle) {
          await lockFileHandle.close();

          await rm(lockFilePath, {
            force: true,
          });
        }

        throw error;
      }
    }
  }

  throw new Error(`Timeout to access lock file`);
}

/**
 * Remove file with lock
 * @param {string} filePath File path to remove
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<void>}
 */
export async function removeFileWithLock(filePath, timeout) {
  const startTime = Date.now();

  const lockFilePath = `${filePath}.lock`;
  let lockFileHandle;

  while (Date.now() - startTime <= timeout) {
    try {
      lockFileHandle = await open(lockFilePath, "wx");

      await rm(filePath, {
        force: true,
      });

      await lockFileHandle.close();

      return await rm(lockFilePath, {
        force: true,
      });
    } catch (error) {
      if (error.code === "ENOENT") {
        return;
      } else if (error.code === "EEXIST") {
        await delay(25);
      } else {
        if (lockFileHandle) {
          await lockFileHandle.close();

          await rm(lockFilePath, {
            force: true,
          });
        }

        throw error;
      }
    }
  }

  throw new Error(`Timeout to access lock file`);
}
