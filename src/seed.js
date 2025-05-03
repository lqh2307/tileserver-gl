"use strict";

import { downloadStyleFile, getStyle, getStyleCreated } from "./style.js";
import { downloadSpriteFile, getSpriteCreated } from "./sprite.js";
import { downloadFontFile, getFontCreated } from "./font.js";
import { readFile } from "node:fs/promises";
import { readFileSync } from "node:fs";
import { printLog } from "./logger.js";
import { Mutex } from "async-mutex";
import {
  downloadGeoJSONFile,
  getGeoJSONCreated,
  getGeoJSON,
} from "./geojson.js";
import {
  getXYZTileExtraInfoFromCoverages,
  updateXYZMetadata,
  downloadXYZTile,
  closeXYZMD5DB,
  openXYZMD5DB,
} from "./tile_xyz.js";
import {
  getMBTilesTileExtraInfoFromCoverages,
  updateMBTilesMetadata,
  downloadMBTilesTile,
  closeMBTilesDB,
  openMBTilesDB,
} from "./tile_mbtiles.js";
import {
  getTileBoundsFromCoverages,
  createFileWithLock,
  removeEmptyFolders,
  processCoverages,
  getDataFromURL,
  getJSONSchema,
  postDataToURL,
  validateJSON,
  calculateMD5,
  unzipAsync,
  delay,
} from "./utils.js";
import {
  getPostgreSQLTileExtraInfoFromCoverages,
  updatePostgreSQLMetadata,
  downloadPostgreSQLTile,
  closePostgreSQLDB,
  openPostgreSQLDB,
} from "./tile_postgresql.js";

let seed;

/* Load seed.json */
if (seed === undefined) {
  seed = JSON.parse(readFileSync(`${process.env.DATA_DIR}/seed.json`, "utf8"));
}

/**
 * Validate seed.json file
 * @returns {Promise<void>}
 */
async function validateSeedFile() {
  validateJSON(await getJSONSchema("seed"), seed);
}

/**
 * Read seed.json file
 * @returns {Promise<Object>}
 */
async function readSeedFile() {
  return await readFile(`${process.env.DATA_DIR}/seed.json`, "utf8");
}

/**
 * Update seed.json file content with lock
 * @param {object} seed Seed object
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<void>}
 */
async function updateSeedFile(seed, timeout) {
  await createFileWithLock(
    `${process.env.DATA_DIR}/seed.json`,
    JSON.stringify(seed, null, 2),
    timeout
  );
}

/**
 * Seed MBTiles tiles
 * @param {string} id Cache MBTiles ID
 * @param {object} metadata Metadata object
 * @param {string} url Tile URL to download
 * @param {"tms"|"xyz"} scheme Tile scheme
 * @param {{ zoom: number, bbox: [number, number, number, number]}[]} coverages Specific coverages
 * @param {number} concurrency Concurrency download
 * @param {number} maxTry Number of retry attempts on failure
 * @param {number} timeout Timeout in milliseconds
 * @param {boolean} storeTransparent Is store transparent tile?
 * @param {string|number|boolean} refreshBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be refreshed/Compare MD5
 * @returns {Promise<void>}
 */
async function seedMBTilesTiles(
  id,
  metadata,
  url,
  scheme,
  coverages,
  concurrency,
  maxTry,
  timeout,
  storeTransparent,
  refreshBefore
) {
  const startTime = Date.now();

  let source;

  try {
    /* Calculate summary */
    const targetCoverages = processCoverages(coverages);
    const { total, tileBounds } = getTileBoundsFromCoverages(
      targetCoverages,
      "xyz"
    );

    let log = `Seeding ${total} tiles of mbtiles "${id}" with:`;
    log += `\n\tStore transparent: ${storeTransparent}`;
    log += `\n\tConcurrency: ${concurrency}`;
    log += `\n\tMax try: ${maxTry}`;
    log += `\n\tTimeout: ${timeout}`;
    log += `\n\tCoverages: ${JSON.stringify(coverages)}`;
    log += `\n\tTarget coverages: ${JSON.stringify(targetCoverages)}`;

    let refreshTimestamp;
    if (typeof refreshBefore === "string") {
      refreshTimestamp = new Date(refreshBefore).getTime();

      log += `\n\tRefresh before: ${refreshBefore}`;
    } else if (typeof refreshBefore === "number") {
      const now = new Date();

      refreshTimestamp = now.setDate(now.getDate() - refreshBefore);

      log += `\n\tOld than: ${refreshBefore} days`;
    } else if (typeof refreshBefore === "boolean") {
      refreshTimestamp = true;

      log += `\n\tRefresh before: check MD5`;
    }
    const refreshTimestampType = typeof refreshTimestamp;

    printLog("info", log);

    /* Open MBTiles SQLite database */
    const filePath = `${process.env.DATA_DIR}/caches/mbtiles/${id}/${id}.mbtiles`;

    source = await openMBTilesDB(
      filePath,
      true,
      30000 // 30 secs
    );

    /* Get tile extra info */
    let targetTileExtraInfo;
    let tileExtraInfo;

    if (refreshTimestampType === "boolean") {
      const hashURL = `${url.slice(
        0,
        url.indexOf("/{z}/{x}/{y}")
      )}/extra-info?compression=true`;

      try {
        printLog(
          "info",
          `Get target tile extra info from "${hashURL}" and tile extra info from "${filePath}"...`
        );

        const res = await postDataToURL(
          hashURL,
          300000, // 5 mins
          targetCoverages,
          "arraybuffer"
        );

        if (res.headers["content-encoding"] === "gzip") {
          targetTileExtraInfo = JSON.parse(await unzipAsync(res.data));
        } else {
          targetTileExtraInfo = JSON.parse(res.data);
        }

        tileExtraInfo = getMBTilesTileExtraInfoFromCoverages(
          source,
          targetCoverages,
          false
        );
      } catch (error) {
        printLog(
          "error",
          `Failed to get target tile extra info from "${hashURL}" and tile extra info from "${filePath}": ${error}`
        );

        targetTileExtraInfo = {};
        tileExtraInfo = {};
      }
    } else if (refreshTimestampType === "number") {
      try {
        printLog("info", `Get tile extra info from "${filePath}"...`);

        tileExtraInfo = getMBTilesTileExtraInfoFromCoverages(
          source,
          targetCoverages,
          true
        );
      } catch (error) {
        printLog(
          "error",
          `Failed to get tile extra info from "${filePath}": ${error}`
        );

        tileExtraInfo = {};
      }
    }

    /* Update MBTiles metadata */
    printLog("info", "Updating metadata...");

    await updateMBTilesMetadata(
      source,
      metadata,
      30000 // 30 secs
    );

    /* Download tiles */
    const tasks = {
      mutex: new Mutex(),
      activeTasks: 0,
      completeTasks: 0,
    };

    async function seedMBTilesTileData(z, x, y, tasks) {
      const tileName = `${z}/${x}/${y}`;

      if (
        refreshTimestampType === "undefined" ||
        (refreshTimestampType === "boolean" &&
          (tileExtraInfo[tileName] === undefined ||
            tileExtraInfo[tileName] !== targetTileExtraInfo[tileName])) ||
        (refreshTimestampType === "number" &&
          (tileExtraInfo[tileName] === undefined ||
            tileExtraInfo[tileName] < refreshTimestamp))
      ) {
        const completeTasks = tasks.completeTasks;
        const tmpY = scheme === "tms" ? (1 << z) - 1 - y : y;

        const targetURL = url
          .replace("{z}", `${z}`)
          .replace("{x}", `${x}`)
          .replace("{y}", `${tmpY}`);

        printLog(
          "info",
          `Downloading data "${id}" - Tile "${tileName}" - From "${targetURL}" - ${completeTasks}/${total}...`
        );

        try {
          await downloadMBTilesTile(
            targetURL,
            source,
            z,
            x,
            tmpY,
            maxTry,
            timeout,
            storeTransparent
          );
        } catch (error) {
          printLog(
            "error",
            `Failed to seed data "${id}" - Tile "${tileName}" - From "${targetURL}" - ${completeTasks}/${total}: ${error}`
          );
        }
      }
    }

    printLog("info", "Downloading datas...");

    for (const { z, x, y } of tileBounds) {
      for (let xCount = x[0]; xCount <= x[1]; xCount++) {
        for (let yCount = y[0]; yCount <= y[1]; yCount++) {
          /* Wait slot for a task */
          while (tasks.activeTasks >= concurrency) {
            await delay(50);
          }

          await tasks.mutex.runExclusive(() => {
            tasks.activeTasks++;
            tasks.completeTasks++;
          });

          /* Run a task */
          seedMBTilesTileData(z, xCount, yCount, tasks).finally(() =>
            tasks.mutex.runExclusive(() => {
              tasks.activeTasks--;
            })
          );
        }
      }
    }

    /* Wait all tasks done */
    while (tasks.activeTasks > 0) {
      await delay(50);
    }

    printLog(
      "info",
      `Completed seed ${total} tiles of mbtiles "${id}" after ${
        (Date.now() - startTime) / 1000
      }s!`
    );
  } catch (error) {
    throw error;
  } finally {
    if (source !== undefined) {
      // Close MBTiles SQLite database
      closeMBTilesDB(source);
    }
  }
}

/**
 * Seed PostgreSQL tiles
 * @param {string} id Cache PostgreSQL ID
 * @param {object} metadata Metadata object
 * @param {string} url Tile URL to download
 * @param {"tms"|"xyz"} scheme Tile scheme
 * @param {{ zoom: number, bbox: [number, number, number, number]}[]} coverages Specific coverages
 * @param {number} concurrency Concurrency download
 * @param {number} maxTry Number of retry attempts on failure
 * @param {number} timeout Timeout in milliseconds
 * @param {boolean} storeTransparent Is store transparent tile?
 * @param {string|number|boolean} refreshBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be refreshed/Compare MD5
 * @returns {Promise<void>}
 */
async function seedPostgreSQLTiles(
  id,
  metadata,
  url,
  scheme,
  coverages,
  concurrency,
  maxTry,
  timeout,
  storeTransparent,
  refreshBefore
) {
  const startTime = Date.now();

  let source;

  try {
    /* Calculate summary */
    const targetCoverages = processCoverages(coverages);
    const { total, tileBounds } = getTileBoundsFromCoverages(
      targetCoverages,
      "xyz"
    );

    let log = `Seeding ${total} tiles of postgresql "${id}" with:`;
    log += `\n\tStore transparent: ${storeTransparent}`;
    log += `\n\tConcurrency: ${concurrency}`;
    log += `\n\tMax try: ${maxTry}`;
    log += `\n\tTimeout: ${timeout}`;
    log += `\n\tCoverages: ${JSON.stringify(coverages)}`;
    log += `\n\tTarget coverages: ${JSON.stringify(targetCoverages)}`;

    let refreshTimestamp;
    if (typeof refreshBefore === "string") {
      refreshTimestamp = new Date(refreshBefore).getTime();

      log += `\n\tRefresh before: ${refreshBefore}`;
    } else if (typeof refreshBefore === "number") {
      const now = new Date();

      refreshTimestamp = now.setDate(now.getDate() - refreshBefore);

      log += `\n\tOld than: ${refreshBefore} days`;
    } else if (typeof refreshBefore === "boolean") {
      refreshTimestamp = true;

      log += `\n\tRefresh before: check MD5`;
    }
    const refreshTimestampType = typeof refreshTimestamp;

    printLog("info", log);

    /* Open PostgreSQL database */
    const filePath = `${process.env.POSTGRESQL_BASE_URI}/${id}`;

    source = await openPostgreSQLDB(filePath, true);

    /* Get tile extra info */
    let targetTileExtraInfo;
    let tileExtraInfo;

    if (refreshTimestampType === "boolean") {
      const hashURL = `${url.slice(
        0,
        url.indexOf("/{z}/{x}/{y}")
      )}/extra-info?compression=true`;

      try {
        printLog(
          "info",
          `Get target tile extra info from "${hashURL}" and tile extra info from "${filePath}"...`
        );

        const res = await postDataToURL(
          hashURL,
          300000, // 5 mins
          targetCoverages,
          "arraybuffer"
        );

        if (res.headers["content-encoding"] === "gzip") {
          targetTileExtraInfo = JSON.parse(await unzipAsync(res.data));
        } else {
          targetTileExtraInfo = JSON.parse(res.data);
        }

        tileExtraInfo = getPostgreSQLTileExtraInfoFromCoverages(
          source,
          targetCoverages,
          false
        );
      } catch (error) {
        printLog(
          "error",
          `Failed to get target tile extra info from "${hashURL}" and tile extra info from "${filePath}": ${error}`
        );

        targetTileExtraInfo = {};
        tileExtraInfo = {};
      }
    } else if (refreshTimestampType === "number") {
      try {
        printLog("info", `Get tile extra info from "${filePath}"...`);

        tileExtraInfo = getPostgreSQLTileExtraInfoFromCoverages(
          source,
          targetCoverages,
          true
        );
      } catch (error) {
        printLog(
          "error",
          `Failed to get tile extra info from "${filePath}": ${error}`
        );

        tileExtraInfo = {};
      }
    }

    /* Update PostgreSQL metadata */
    printLog("info", "Updating metadata...");

    await updatePostgreSQLMetadata(
      source,
      metadata,
      30000 // 30 secs
    );

    /* Download tiles */
    const tasks = {
      mutex: new Mutex(),
      activeTasks: 0,
      completeTasks: 0,
    };

    async function seedPostgreSQLTileData(z, x, y, tasks) {
      const tileName = `${z}/${x}/${y}`;

      if (
        refreshTimestampType === "undefined" ||
        (refreshTimestampType === "boolean" &&
          (tileExtraInfo[tileName] === undefined ||
            tileExtraInfo[tileName] !== targetTileExtraInfo[tileName])) ||
        (refreshTimestampType === "number" &&
          (tileExtraInfo[tileName] === undefined ||
            tileExtraInfo[tileName] < refreshTimestamp))
      ) {
        const completeTasks = tasks.completeTasks;
        const tmpY = scheme === "tms" ? (1 << z) - 1 - y : y;

        const targetURL = url
          .replace("{z}", `${z}`)
          .replace("{x}", `${x}`)
          .replace("{y}", `${tmpY}`);

        printLog(
          "info",
          `Downloading data "${id}" - Tile "${tileName}" - From "${targetURL}" - ${completeTasks}/${total}...`
        );

        try {
          await downloadPostgreSQLTile(
            targetURL,
            source,
            z,
            x,
            tmpY,
            maxTry,
            timeout,
            storeTransparent
          );
        } catch (error) {
          printLog(
            "error",
            `Failed to seed data "${id}" - Tile "${tileName}" - From "${targetURL}" - ${completeTasks}/${total}: ${error}`
          );
        }
      }
    }

    printLog("info", "Downloading datas...");

    for (const { z, x, y } of tileBounds) {
      for (let xCount = x[0]; xCount <= x[1]; xCount++) {
        for (let yCount = y[0]; yCount <= y[1]; yCount++) {
          /* Wait slot for a task */
          while (tasks.activeTasks >= concurrency) {
            await delay(50);
          }

          await tasks.mutex.runExclusive(() => {
            tasks.activeTasks++;
            tasks.completeTasks++;
          });

          /* Run a task */
          seedPostgreSQLTileData(z, xCount, yCount, tasks).finally(() =>
            tasks.mutex.runExclusive(() => {
              tasks.activeTasks--;
            })
          );
        }
      }
    }

    /* Wait all tasks done */
    while (tasks.activeTasks > 0) {
      await delay(50);
    }

    printLog(
      "info",
      `Completed seed ${total} tiles of postgresql "${id}" after ${
        (Date.now() - startTime) / 1000
      }s!`
    );
  } catch (error) {
    throw error;
  } finally {
    if (source !== undefined) {
      /* Close PostgreSQL database */
      await closePostgreSQLDB(source);
    }
  }
}

/**
 * Seed XYZ tiles
 * @param {string} id Cache XYZ ID
 * @param {object} metadata Metadata object
 * @param {string} url Tile URL
 * @param {"tms"|"xyz"} scheme Tile scheme
 * @param {{ zoom: number, bbox: [number, number, number, number]}[]} coverages Specific coverages
 * @param {number} concurrency Concurrency to download
 * @param {number} maxTry Number of retry attempts on failure
 * @param {number} timeout Timeout in milliseconds
 * @param {boolean} storeTransparent Is store transparent tile?
 * @param {string|number|boolean} refreshBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be refreshed/Compare MD5
 * @returns {Promise<void>}
 */
async function seedXYZTiles(
  id,
  metadata,
  url,
  scheme,
  coverages,
  concurrency,
  maxTry,
  timeout,
  storeTransparent,
  refreshBefore
) {
  const startTime = Date.now();

  let source;

  try {
    /* Calculate summary */
    const targetCoverages = processCoverages(coverages);
    const { total, tileBounds } = getTileBoundsFromCoverages(
      targetCoverages,
      "xyz"
    );

    let log = `Seeding ${total} tiles of xyz "${id}" with:`;
    log += `\n\tStore transparent: ${storeTransparent}`;
    log += `\n\tConcurrency: ${concurrency}`;
    log += `\n\tMax try: ${maxTry}`;
    log += `\n\tTimeout: ${timeout}`;
    log += `\n\tCoverages: ${JSON.stringify(coverages)}`;
    log += `\n\tTarget coverages: ${JSON.stringify(targetCoverages)}`;

    let refreshTimestamp;
    if (typeof refreshBefore === "string") {
      refreshTimestamp = new Date(refreshBefore).getTime();

      log += `\n\tRefresh before: ${refreshBefore}`;
    } else if (typeof refreshBefore === "number") {
      const now = new Date();

      refreshTimestamp = now.setDate(now.getDate() - refreshBefore);

      log += `\n\tOld than: ${refreshBefore} days`;
    } else if (typeof refreshBefore === "boolean") {
      refreshTimestamp = true;

      log += `\n\tRefresh before: check MD5`;
    }
    const refreshTimestampType = typeof refreshTimestamp;

    printLog("info", log);

    /* Open MD5 SQLite database */
    const sourcePath = `${process.env.DATA_DIR}/caches/xyzs/${id}`;
    const filePath = `${sourcePath}/${id}.sqlite`;

    source = await openXYZMD5DB(
      filePath,
      true,
      30000 // 30 secs
    );

    /* Get tile extra info */
    let targetTileExtraInfo;
    let tileExtraInfo;

    if (refreshTimestampType === "boolean") {
      const hashURL = `${url.slice(
        0,
        url.indexOf("/{z}/{x}/{y}")
      )}/extra-info?compression=true`;

      try {
        printLog(
          "info",
          `Get target tile extra info from "${hashURL}" and tile extra info from "${filePath}"...`
        );

        const res = await postDataToURL(
          hashURL,
          300000, // 5 mins
          targetCoverages,
          "arraybuffer"
        );

        if (res.headers["content-encoding"] === "gzip") {
          targetTileExtraInfo = JSON.parse(await unzipAsync(res.data));
        } else {
          targetTileExtraInfo = JSON.parse(res.data);
        }

        tileExtraInfo = getXYZTileExtraInfoFromCoverages(
          source,
          targetCoverages,
          false
        );
      } catch (error) {
        printLog(
          "error",
          `Failed to get target tile extra info from "${hashURL}" and tile extra info from "${filePath}": ${error}`
        );

        targetTileExtraInfo = {};
        tileExtraInfo = {};
      }
    } else if (refreshTimestampType === "number") {
      try {
        printLog("info", `Get tile extra info from "${filePath}"...`);

        tileExtraInfo = getXYZTileExtraInfoFromCoverages(
          source,
          targetCoverages,
          true
        );
      } catch (error) {
        printLog(
          "error",
          `Failed to get tile extra info from "${filePath}": ${error}`
        );

        tileExtraInfo = {};
      }
    }

    /* Update XYZ metadata */
    printLog("info", "Updating metadata...");

    await updateXYZMetadata(
      source,
      metadata,
      30000 // 30 secs
    );

    /* Download tile files */
    const tasks = {
      mutex: new Mutex(),
      activeTasks: 0,
      completeTasks: 0,
    };

    async function seedXYZTileData(z, x, y, tasks) {
      const tileName = `${z}/${x}/${y}`;

      if (
        refreshTimestampType === "undefined" ||
        (refreshTimestampType === "boolean" &&
          (tileExtraInfo[tileName] === undefined ||
            tileExtraInfo[tileName] !== targetTileExtraInfo[tileName])) ||
        (refreshTimestampType === "number" &&
          (tileExtraInfo[tileName] === undefined ||
            tileExtraInfo[tileName] < refreshTimestamp))
      ) {
        const completeTasks = tasks.completeTasks;
        const tmpY = scheme === "tms" ? (1 << z) - 1 - y : y;

        const targetURL = url
          .replace("{z}", `${z}`)
          .replace("{x}", `${x}`)
          .replace("{y}", `${tmpY}`);

        printLog(
          "info",
          `Downloading data "${id}" - Tile "${tileName}" - From "${targetURL}" - ${completeTasks}/${total}...`
        );

        try {
          await downloadXYZTile(
            targetURL,
            id,
            source,
            z,
            x,
            tmpY,
            metadata.format,
            maxTry,
            timeout,
            storeTransparent
          );
        } catch (error) {
          printLog(
            "error",
            `Failed to seed data "${id}" - Tile "${tileName}" - From "${targetURL}" - ${completeTasks}/${total}: ${error}`
          );
        }
      }
    }

    printLog("info", "Downloading datas...");

    for (const { z, x, y } of tileBounds) {
      for (let xCount = x[0]; xCount <= x[1]; xCount++) {
        for (let yCount = y[0]; yCount <= y[1]; yCount++) {
          /* Wait slot for a task */
          while (tasks.activeTasks >= concurrency) {
            await delay(50);
          }

          await tasks.mutex.runExclusive(() => {
            tasks.activeTasks++;
            tasks.completeTasks++;
          });

          /* Run a task */
          seedXYZTileData(z, xCount, yCount, tasks).finally(() =>
            tasks.mutex.runExclusive(() => {
              tasks.activeTasks--;
            })
          );
        }
      }
    }

    /* Wait all tasks done */
    while (tasks.activeTasks > 0) {
      await delay(50);
    }

    /* Remove parent folders if empty */
    await removeEmptyFolders(sourcePath, /^.*\.(gif|png|jpg|jpeg|webp|pbf)$/);

    printLog(
      "info",
      `Completed seed ${total} tiles of xyz "${id}" after ${
        (Date.now() - startTime) / 1000
      }s!`
    );
  } catch (error) {
    throw error;
  } finally {
    if (source !== undefined) {
      /* Close MD5 SQLite database */
      closeXYZMD5DB(source);
    }
  }
}

/**
 * Seed geojson
 * @param {string} id Cache geojson ID
 * @param {string} url GeoJSON URL
 * @param {number} maxTry Number of retry attempts on failure
 * @param {number} timeout Timeout in milliseconds
 * @param {string|number} refreshBefore Date string in format "YYYY-MM-DDTHH:mm:ss" or number of days before which file should be refreshed
 * @returns {Promise<void>}
 */
async function seedGeoJSON(id, url, maxTry, timeout, refreshBefore) {
  const startTime = Date.now();

  let log = `Seeding geojson "${id}" with:`;
  log += `\n\tMax try: ${maxTry}`;
  log += `\n\tTimeout: ${timeout}`;

  let refreshTimestamp;
  if (typeof refreshBefore === "string") {
    refreshTimestamp = new Date(refreshBefore).getTime();

    log += `\n\tRefresh before: ${refreshBefore}`;
  } else if (typeof refreshBefore === "number") {
    const now = new Date();

    refreshTimestamp = now.setDate(now.getDate() - refreshBefore);

    log += `\n\tOld than: ${refreshBefore} days`;
  } else if (typeof refreshBefore === "boolean") {
    refreshTimestamp = true;

    log += `\n\tRefresh before: check MD5`;
  }

  printLog("info", log);

  /* Download GeoJSON file */
  const sourcePath = `${process.env.DATA_DIR}/caches/geojsons/${id}`;
  const filePath = `${sourcePath}/${id}.geojson`;

  try {
    let needDownload = false;

    if (refreshTimestamp === true) {
      try {
        const [response, geoJSONData] = await Promise.all([
          getDataFromURL(
            `${url.slice(0, url.indexOf(`/${id}.geojson`))}/md5`,
            timeout,
            "arraybuffer"
          ),
          getGeoJSON(filePath, false),
        ]);

        if (
          response.headers["etag"] === undefined ||
          response.headers["etag"] !== calculateMD5(geoJSONData)
        ) {
          needDownload = true;
        }
      } catch (error) {
        if (error.message === "GeoJSON does not exist") {
          needDownload = true;
        } else {
          throw error;
        }
      }
    } else if (refreshTimestamp !== undefined) {
      try {
        const created = await getGeoJSONCreated(filePath);

        if (created === undefined || created < refreshTimestamp) {
          needDownload = true;
        }
      } catch (error) {
        if (error.message === "GeoJSON created does not exist") {
          needDownload = true;
        } else {
          throw error;
        }
      }
    } else {
      needDownload = true;
    }

    printLog("info", "Downloading geojson...");

    if (needDownload === true) {
      printLog(
        "info",
        `Downloading geojson "${id}" - File "${filePath}" - From "${url}"...`
      );

      await downloadGeoJSONFile(url, filePath, maxTry, timeout);
    }
  } catch (error) {
    printLog("error", `Failed to seed geojson "${id}": ${error}`);
  }

  /* Remove parent folders if empty */
  await removeEmptyFolders(sourcePath, /^.*\.geojson$/);

  printLog(
    "info",
    `Completed seed geojson "${id}" after ${(Date.now() - startTime) / 1000}s!`
  );
}

/**
 * Seed sprite
 * @param {string} id Cache sprite ID
 * @param {string} url Sprite URL
 * @param {number} maxTry Number of retry attempts on failure
 * @param {number} timeout Timeout in milliseconds
 * @param {string|number} refreshBefore Date string in format "YYYY-MM-DDTHH:mm:ss" or number of days before which file should be refreshed
 * @returns {Promise<void>}
 */
async function seedSprite(id, url, maxTry, timeout, refreshBefore) {
  const startTime = Date.now();

  let log = `Seeding sprite "${id}" with:`;
  log += `\n\tMax try: ${maxTry}`;
  log += `\n\tTimeout: ${timeout}`;

  let refreshTimestamp;
  if (typeof refreshBefore === "string") {
    refreshTimestamp = new Date(refreshBefore).getTime();

    log += `\n\tRefresh before: ${refreshBefore}`;
  } else if (typeof refreshBefore === "number") {
    const now = new Date();

    refreshTimestamp = now.setDate(now.getDate() - refreshBefore);

    log += `\n\tOld than: ${refreshBefore} days`;
  }

  printLog("info", log);

  /* Download sprite files */
  const sourcePath = `${process.env.DATA_DIR}/caches/sprites/${id}`;

  async function seedSpriteData(fileName) {
    const filePath = `${sourcePath}/${fileName}`;

    try {
      let needDownload = false;

      if (refreshTimestamp !== undefined) {
        try {
          const created = await getSpriteCreated(filePath);

          if (created === undefined || created < refreshTimestamp) {
            needDownload = true;
          }
        } catch (error) {
          if (error.message === "Sprite created does not exist") {
            needDownload = true;
          } else {
            throw error;
          }
        }
      } else {
        needDownload = true;
      }

      if (needDownload === true) {
        const targetURL = url.replace("{id}", `${id}`);

        printLog(
          "info",
          `Downloading sprite "${id}" - File "${fileName}" - From "${targetURL}"...`
        );

        await downloadSpriteFile(targetURL, id, fileName, maxTry, timeout);
      }
    } catch (error) {
      printLog(
        "error",
        `Failed to seed sprite "${id}" - File "${fileName}": ${error}`
      );
    }
  }

  printLog("info", "Downloading sprites...");

  await Promise.all(
    ["sprite.json", "sprite.png", "sprite@2x.json", "sprite@2x.png"].map(
      (fileName) => seedSpriteData(fileName)
    )
  );

  /* Remove parent folders if empty */
  await removeEmptyFolders(sourcePath, /^.*\.(json|png)$/);

  printLog(
    "info",
    `Completed seed sprite "${id}" after ${(Date.now() - startTime) / 1000}s!`
  );
}

/**
 * Seed font
 * @param {string} id Cache font ID
 * @param {string} url Font URL
 * @param {number} concurrency Concurrency download
 * @param {number} maxTry Number of retry attempts on failure
 * @param {number} timeout Timeout in milliseconds
 * @param {string|number} refreshBefore Date string in format "YYYY-MM-DDTHH:mm:ss" or number of days before which file should be refreshed
 * @returns {Promise<void>}
 */
async function seedFont(id, url, concurrency, maxTry, timeout, refreshBefore) {
  const startTime = Date.now();

  const total = 256;

  let log = `Seeding ${total} fonts of font "${id}" with:`;
  log += `\n\tConcurrency: ${concurrency}`;
  log += `\n\tMax try: ${maxTry}`;
  log += `\n\tTimeout: ${timeout}`;

  let refreshTimestamp;
  if (typeof refreshBefore === "string") {
    refreshTimestamp = new Date(refreshBefore).getTime();

    log += `\n\tRefresh before: ${refreshBefore}`;
  } else if (typeof refreshBefore === "number") {
    const now = new Date();

    refreshTimestamp = now.setDate(now.getDate() - refreshBefore);

    log += `\n\tOld than: ${refreshBefore} days`;
  }

  printLog("info", log);

  /* Download font files */
  const sourcePath = `${process.env.DATA_DIR}/caches/fonts/${id}`;

  const tasks = {
    mutex: new Mutex(),
    activeTasks: 0,
    completeTasks: 0,
  };

  async function seedFontData(start, end, tasks) {
    const range = `${start}-${end}`;
    const filePath = `${sourcePath}/${range}.pbf`;
    const completeTasks = tasks.completeTasks;

    try {
      let needDownload = false;

      if (refreshTimestamp !== undefined) {
        try {
          const created = await getFontCreated(filePath);

          if (created === undefined || created < refreshTimestamp) {
            needDownload = true;
          }
        } catch (error) {
          if (error.message === "Font created does not exist") {
            needDownload = true;
          } else {
            throw error;
          }
        }
      } else {
        needDownload = true;
      }

      if (needDownload === true) {
        const targetURL = url.replace("{range}", `${range}`);

        printLog(
          "info",
          `Downloading font "${id}" - Range "${range}" - From "${targetURL}" - ${completeTasks}/${total}...`
        );

        await downloadFontFile(targetURL, id, range, maxTry, timeout);
      }
    } catch (error) {
      printLog(
        "error",
        `Failed to seed font "${id}" - Range "${range}" - ${completeTasks}/${total}: ${error}`
      );
    }
  }

  printLog("info", "Downloading fonts...");

  for (let i = 0; i < 256; i++) {
    /* Wait slot for a task */
    while (tasks.activeTasks >= concurrency) {
      await delay(50);
    }

    await tasks.mutex.runExclusive(() => {
      tasks.activeTasks++;
      tasks.completeTasks++;
    });

    /* Run a task */
    seedFontData(i * 256, i * 256 + 255, tasks).finally(() =>
      tasks.mutex.runExclusive(() => {
        tasks.activeTasks--;
      })
    );
  }

  /* Wait all tasks done */
  while (tasks.activeTasks > 0) {
    await delay(50);
  }

  /* Remove parent folders if empty */
  await removeEmptyFolders(sourcePath, /^.*\.pbf$/);

  printLog(
    "info",
    `Completed seed ${total} fonts of font "${id}" after ${
      (Date.now() - startTime) / 1000
    }s!`
  );
}

/**
 * Seed style
 * @param {string} id Cache style ID
 * @param {string} url Style URL
 * @param {number} maxTry Number of retry attempts on failure
 * @param {number} timeout Timeout in milliseconds
 * @param {string|number} refreshBefore Date string in format "YYYY-MM-DDTHH:mm:ss" or number of days before which file should be refreshed
 * @returns {Promise<void>}
 */
async function seedStyle(id, url, maxTry, timeout, refreshBefore) {
  const startTime = Date.now();

  let log = `Seeding style "${id}" with:`;
  log += `\n\tMax try: ${maxTry}`;
  log += `\n\tTimeout: ${timeout}`;

  let refreshTimestamp;
  if (typeof refreshBefore === "string") {
    refreshTimestamp = new Date(refreshBefore).getTime();

    log += `\n\tRefresh before: ${refreshBefore}`;
  } else if (typeof refreshBefore === "number") {
    const now = new Date();

    refreshTimestamp = now.setDate(now.getDate() - refreshBefore);

    log += `\n\tOld than: ${refreshBefore} days`;
  } else if (typeof refreshBefore === "boolean") {
    refreshTimestamp = true;

    log += `\n\tRefresh before: check MD5`;
  }

  printLog("info", log);

  /* Download style.json file */
  const sourcePath = `${process.env.DATA_DIR}/caches/styles/${id}`;
  const filePath = `${sourcePath}/style.json`;

  try {
    let needDownload = false;

    if (refreshTimestamp === true) {
      try {
        const [response, styleData] = await Promise.all([
          getDataFromURL(
            `${url.slice(0, url.indexOf(`/${id}/style.json?raw=true`))}/md5`,
            timeout,
            "arraybuffer"
          ),
          getStyle(filePath, false),
        ]);

        if (
          response.headers["etag"] === undefined ||
          response.headers["etag"] !== calculateMD5(styleData)
        ) {
          needDownload = true;
        }
      } catch (error) {
        if (error.message === "Style does not exist") {
          needDownload = true;
        } else {
          throw error;
        }
      }
    } else if (refreshTimestamp !== undefined) {
      try {
        const created = await getStyleCreated(filePath);

        if (created === undefined || created < refreshTimestamp) {
          needDownload = true;
        }
      } catch (error) {
        if (error.message === "Style created does not exist") {
          needDownload = true;
        } else {
          throw error;
        }
      }
    } else {
      needDownload = true;
    }

    if (refreshTimestamp !== undefined) {
      try {
        const created = await getStyleCreated(filePath);

        if (created === undefined || created < refreshTimestamp) {
          needDownload = true;
        }
      } catch (error) {
        if (error.message === "Style created does not exist") {
          needDownload = true;
        } else {
          throw error;
        }
      }
    } else {
      needDownload = true;
    }

    printLog("info", "Downloading style...");

    if (needDownload === true) {
      printLog(
        "info",
        `Downloading style "${id}" - File "${filePath}" - From "${url}"...`
      );

      await downloadStyleFile(url, filePath, maxTry, timeout);
    }
  } catch (error) {
    printLog("error", `Failed to seed style "${id}": ${error}`);
  }

  /* Remove parent folders if empty */
  await removeEmptyFolders(sourcePath, /^.*\.json$/);

  printLog(
    "info",
    `Completed seed style "${id}" after ${(Date.now() - startTime) / 1000}s!`
  );
}

export {
  seedPostgreSQLTiles,
  seedMBTilesTiles,
  validateSeedFile,
  updateSeedFile,
  readSeedFile,
  seedXYZTiles,
  seedGeoJSON,
  seedSprite,
  seedStyle,
  seedFont,
  seed,
};
