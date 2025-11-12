"use strict";

import { readFile } from "node:fs/promises";
import { printLog } from "./logger.js";
import { spawn } from "child_process";
import { Mutex } from "async-mutex";
import mime from "mime";

/**
 * Delay function to wait for a specified time
 * @param {number} ms Time to wait in milliseconds
 * @returns {Promise<void>}
 */
export async function delay(ms) {
  if (ms >= 0) {
    await new Promise((resolve) => setTimeout(resolve, ms));
  }
}

/**
 * Attempt do function multiple times
 * @param {function} fn The function to attempt
 * @param {number} maxTry Number of retry attempts on failure
 * @param {number} after Delay in milliseconds between each retry
 * @returns {Promise<void>}
 */
export async function retry(fn, maxTry, after = 0) {
  for (let attempt = 1; attempt <= maxTry; attempt++) {
    try {
      return await fn();
    } catch (error) {
      const remainingAttempts = maxTry - attempt;
      if (remainingAttempts > 0) {
        printLog(
          "warn",
          `${error}. ${remainingAttempts} try remaining - After ${after} ms...`,
        );

        await delay(after);
      } else {
        throw error;
      }
    }
  }
}

/**
 * Deep clone an object using JSON serialization
 * @param {object} obj The object to clone
 * @returns {object} The deep-cloned object
 */
export function deepClone(obj) {
  if (obj !== undefined) {
    return JSON.parse(JSON.stringify(obj));
  }
}

/**
 * Get version of server
 * @returns {Promise<string>}
 */
export async function getVersion() {
  return JSON.parse(await readFile("package.json", "utf8")).version;
}

/**
 * Run an external command and optionally stream output via callback
 * @param {string} command The command to run
 * @param {number} interval Interval in milliseconds to call callback
 * @param {(partialOutput: string) => void} callback Function to call every interval with output so far
 * @returns {Promise<string>} The command's full stdout
 */
export async function runCommand(command, interval, callback) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, {
      shell: true,
    });

    let stdout = "";
    let stderr = "";

    child.stdout.on("data", (data) => {
      stdout += data.toString();
    });

    child.stderr.on("data", (data) => {
      stderr += data.toString();
    });

    const intervalID =
      interval > 0 && callback
        ? setInterval(() => callback(stdout), interval)
        : undefined;

    child.on("close", (code) => {
      if (intervalID !== undefined) {
        clearInterval(intervalID);

        callback(stdout);
      }

      if (code === 0) {
        resolve(stdout);
      } else {
        reject(stderr || `Process exited with code: ${code}`);
      }
    });

    child.on("error", (err) => {
      if (intervalID !== undefined) {
        clearInterval(intervalID);
      }

      reject(err.message);
    });
  });
}

/**
 * Handle tiles concurrency
 * @param {number} concurrency Concurrency
 * @param {(z: number, x: number, y: number, tasks: { activeTasks: number, completeTasks: number }) => void} renderFunc Render function
 * @param {{ realBBox: [number, number, number, number], total: number, z: number, x: [number, number], y: [number, number] }[]} tileBounds Tile bounds
 * @param {{ export: boolean }} item Item object
 * @returns {Promise<{void}>} Response
 */
export async function handleTilesConcurrency(
  concurrency,
  renderFunc,
  tileBounds,
  item,
) {
  const mutex = new Mutex();

  const tasks = {
    activeTasks: 0,
    completeTasks: 0,
  };

  for (const { z, x, y } of tileBounds) {
    for (let xCount = x[0]; xCount <= x[1]; xCount++) {
      for (let yCount = y[0]; yCount <= y[1]; yCount++) {
        if (item && !item.export) {
          return;
        }

        /* Wait slot for a task */
        while (tasks.activeTasks >= concurrency) {
          await delay(25);
        }

        /* Acquire mutex */
        await mutex.runExclusive(() => {
          tasks.activeTasks++;
          tasks.completeTasks++;
        });

        /* Run a task */
        renderFunc(z, xCount, yCount, tasks).finally(() =>
          /* Release mutex */
          mutex.runExclusive(() => {
            tasks.activeTasks--;
          }),
        );
      }
    }
  }

  /* Wait all tasks done */
  while (tasks.activeTasks > 0) {
    await delay(25);
  }
}

/**
 * Handle concurrency
 * @param {number} concurrency Concurrency
 * @param {(idx: number, value: any[], tasks: { activeTasks: number, completeTasks: number }) => void} handleFunc Handle function
 * @param {any[]} values Values
 * @param {{ interval: number, callbackFunc: (tasks: { activeTasks: number, completeTasks: number }) => void }} callback Callback
 * @returns {Promise<{void}>} Response
 */
export async function handleConcurrency(
  concurrency,
  handleFunc,
  values,
  callback,
) {
  let intervalID;

  try {
    const mutex = new Mutex();

    const tasks = {
      activeTasks: 0,
      completeTasks: 0,
    };

    const { interval, callbackFunc } = callback || {};

    /* Call callback */
    if (interval > 0 && callbackFunc) {
      intervalID = setInterval(() => callbackFunc(tasks), interval);
    }

    for (let idx = 0; idx < values.length; idx++) {
      /* Wait slot for a task */
      while (tasks.activeTasks >= concurrency) {
        await delay(25);
      }

      /* Acquire mutex */
      await mutex.runExclusive(() => {
        tasks.activeTasks++;
        tasks.completeTasks++;
      });

      /* Run a task */
      handleFunc(idx, values, tasks).finally(() =>
        /* Release mutex */
        mutex.runExclusive(() => {
          tasks.activeTasks--;
        }),
      );
    }

    /* Wait all tasks done */
    while (tasks.activeTasks > 0) {
      await delay(25);
    }

    /* Last call callback */
    if (interval > 0 && callbackFunc) {
      callbackFunc(tasks);
    }
  } catch (error) {
    throw error;
  } finally {
    if (intervalID) {
      clearInterval(intervalID);
    }
  }
}

/**
 * Return either a format as an extension: png, pbf, jpeg, webp, gif, ttf, otf, woff, woff2 and
 * headers - Content-Type and Content-Encoding - for a response containing this kind of binary data
 * @param {Buffer} buffer Input data
 * @returns {{ format: "jpeg"|"png"|"webp"|"gif"|"pbf"|"ttf"|"otf"|"woff"|"woff2", headers: object }}
 */
export function detectFormatAndHeaders(buffer) {
  let format;
  const headers = {};

  if (
    buffer[0] === 0x89 &&
    buffer[1] === 0x50 &&
    buffer[2] === 0x4e &&
    buffer[3] === 0x47 &&
    buffer[4] === 0x0d &&
    buffer[5] === 0x0a &&
    buffer[6] === 0x1a &&
    buffer[7] === 0x0a
  ) {
    format = "png";
    headers["content-type"] = "image/png";
  } else if (
    buffer[0] === 0xff &&
    buffer[1] === 0xd8 &&
    buffer[buffer.length - 2] === 0xff &&
    buffer[buffer.length - 1] === 0xd9
  ) {
    format = "jpeg";
    headers["content-type"] = "image/jpeg";
  } else if (
    buffer[0] === 0x47 &&
    buffer[1] === 0x49 &&
    buffer[2] === 0x46 &&
    buffer[3] === 0x38 &&
    (buffer[4] === 0x39 || buffer[4] === 0x37) &&
    buffer[5] === 0x61
  ) {
    format = "gif";
    headers["content-type"] = "image/gif";
  } else if (
    buffer[0] === 0x52 &&
    buffer[1] === 0x49 &&
    buffer[2] === 0x46 &&
    buffer[3] === 0x46 &&
    buffer[8] === 0x57 &&
    buffer[9] === 0x45 &&
    buffer[10] === 0x42 &&
    buffer[11] === 0x50
  ) {
    format = "webp";
    headers["content-type"] = "image/webp";
  } else if (
    buffer[0] === 0x77 &&
    buffer[1] === 0x4f &&
    buffer[2] === 0x46 &&
    buffer[3] === 0x46
  ) {
    format = "woff";
    headers["content-type"] = "font/woff";
  } else if (
    buffer[0] === 0x77 &&
    buffer[1] === 0x4f &&
    buffer[2] === 0x46 &&
    buffer[3] === 0x32
  ) {
    format = "woff2";
    headers["content-type"] = "font/woff2";
  } else if (
    buffer[0] === 0x4f &&
    buffer[1] === 0x54 &&
    buffer[2] === 0x54 &&
    buffer[3] === 0x4f
  ) {
    format = "otf";
    headers["content-type"] = "font/otf";
  } else if (
    buffer[0] === 0x00 &&
    buffer[1] === 0x01 &&
    buffer[2] === 0x00 &&
    buffer[3] === 0x00
  ) {
    format = "ttf";
    headers["content-type"] = "font/ttf";
  } else {
    format = "pbf";
    headers["content-type"] = "application/x-protobuf";

    if (buffer[0] === 0x78 && buffer[1] === 0x9c) {
      headers["content-encoding"] = "deflate";
    } else if (buffer[0] === 0x1f && buffer[1] === 0x8b) {
      headers["content-encoding"] = "gzip";
    }
  }

  return {
    format,
    headers,
  };
}

/**
 * Get content-type from format
 * @param {string} format Data format
 * @returns {string}
 */
export function detectContentTypeFromFormat(format) {
  return mime.getType(format);
}

/**
 * Convert a value from one unit to another
 * @param {number} value Numeric value
 * @param {"km"|"hm"|"dam"|"m"|"dm"|"cm"|"mm"} from Unit of input value (Default: "m")
 * @param {"km"|"hm"|"dam"|"m"|"dm"|"cm"|"mm"} to Unit of output value (Default: "m")
 * @returns {number} Converted value
 */
export function convertLength(value, from, to) {
  const factors = {
    km: 1000,
    hm: 100,
    dam: 10,
    m: 1,
    dm: 0.1,
    cm: 0.01,
    mm: 0.001,
  };

  return (
    (value * (factors[from] ?? factors["m"])) / (factors[to] ?? factors["m"])
  );
}

/**
 * Convert a value with unit to pixels
 * @param {number} value Mumeric value
 * @param {"km"|"hm"|"dam"|"m"|"dm"|"cm"|"mm"} unit Unit of the value (Default: m)
 * @param {number} ppi Pixel per inch
 * @returns {number} Value in pixel
 */
export function toPixel(value, unit, ppi = 96) {
  const factors = {
    km: 1000,
    hm: 100,
    dam: 10,
    m: 1,
    dm: 0.1,
    cm: 0.01,
    mm: 0.001,
  };

  return (value * ppi * (factors[unit] ?? factors["m"])) / 0.0254;
}

/**
 * Convert pixels to a value with unit
 * @param {number} pixels Value in pixel
 * @param {"km"|"hm"|"dam"|"m"|"dm"|"cm"|"mm"} unit Target unit (Default: m)
 * @param {number} ppi Pixel per inch
 * @returns {number} Value in the given unit
 */
export function fromPixel(pixels, unit, ppi = 96) {
  const factors = {
    km: 1000,
    hm: 100,
    dam: 10,
    m: 1,
    dm: 0.1,
    cm: 0.01,
    mm: 0.001,
  };

  return (pixels * 0.0254) / (ppi * (factors[unit] ?? factors["m"]));
}
