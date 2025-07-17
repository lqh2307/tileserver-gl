"use strict";

import { downloadStyleFile, getStyle, getStyleCreated } from "./style.js";
import { downloadSpriteFile, getSpriteCreated } from "./sprite.js";
import { downloadFontFile, getFontCreated } from "./font.js";
import { readFile } from "node:fs/promises";
import { readFileSync } from "node:fs";
import { printLog } from "./logger.js";
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
  handleTilesConcurrency,
  createFileWithLock,
  removeEmptyFolders,
  handleConcurrency,
  getDataFromURL,
  getJSONSchema,
  postDataToURL,
  getTileBounds,
  validateJSON,
  calculateMD5,
  unzipAsync,
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
  try {
    seed = JSON.parse(
      readFileSync(`${process.env.DATA_DIR || "data"}/seed.json`, "utf8")
    );
  } catch (error) {
    printLog("error", `Failed to load seed.json file: ${error}`);

    seed = {};
  }
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
 * @param {boolean} isParse Parse JSON?
 * @returns {Promise<object>}
 */
async function readSeedFile(isParse) {
  const data = await readFile(`${process.env.DATA_DIR}/seed.json`, "utf8");

  if (isParse) {
    return JSON.parse(data);
  } else {
    return data;
  }
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
 * @param {number} concurrency Concurrency
 * @param {number} maxTry Number of retry attempts on failure
 * @param {number} timeout Timeout in milliseconds
 * @param {boolean} storeTransparent Is store transparent tile?
 * @param {string|number|boolean} refreshBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be refreshed/Compare MD5
 * @param {object} headers Headers
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
  refreshBefore,
  headers
) {
  const startTime = Date.now();

  let source;

  try {
    /* Calculate summary */
    const { realBBox, total, tileBounds } = getTileBounds({
      coverages: coverages,
    });

    let log = `Seeding ${total} tiles of mbtiles "${id}" with:`;
    log += `\n\tURL: ${url} - Header: ${JSON.stringify(
      headers
    )} - Scheme: ${scheme}`;
    log += `\n\tStore transparent: ${storeTransparent}`;
    log += `\n\tConcurrency: ${concurrency} - Max try: ${maxTry} - Timeout: ${timeout}`;
    log += `\n\tCoverages: ${JSON.stringify(coverages)}`;

    let refreshTimestamp;
    if (typeof refreshBefore === "string") {
      refreshTimestamp = new Date(refreshBefore).getTime();

      log += `\n\tRefresh before: ${refreshBefore}`;
    } else if (typeof refreshBefore === "number") {
      const now = new Date();

      refreshTimestamp = now.setDate(now.getDate() - refreshBefore);

      log += `\n\tOld than: ${refreshBefore} days`;
    } else if (refreshBefore === true) {
      refreshTimestamp = true;

      log += `\n\tRefresh before: check MD5`;
    }

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

    if (refreshTimestamp === true) {
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
          3600000, // 1 hours
          coverages,
          "arraybuffer",
          false,
          {
            "Content-Type": "application/json",
          }
        );

        if (res.headers["content-encoding"] === "gzip") {
          targetTileExtraInfo = JSON.parse(await unzipAsync(res.data));
        } else {
          targetTileExtraInfo = JSON.parse(res.data);
        }

        tileExtraInfo = getMBTilesTileExtraInfoFromCoverages(
          source,
          coverages,
          false
        );
      } catch (error) {
        if (error.statusCode >= 500) {
          printLog(
            "error",
            `Failed to get target tile extra info from "${hashURL}": ${error}. Skipping seed mbtiles "${id}"...`
          );

          return;
        }

        printLog(
          "error",
          `Failed to get target tile extra info from "${hashURL}" and tile extra info from "${filePath}": ${error}`
        );

        targetTileExtraInfo = {};
        tileExtraInfo = {};
      }
    } else if (refreshTimestamp) {
      try {
        printLog("info", `Get tile extra info from "${filePath}"...`);

        tileExtraInfo = getMBTilesTileExtraInfoFromCoverages(
          source,
          coverages,
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
    printLog("info", "Updating MBTiles metadata...");

    await updateMBTilesMetadata(
      source,
      {
        ...metadata,
        bbox: realBBox,
      },
      30000 // 30 secs
    );

    /* Download tiles */
    async function seedMBTilesTileData(z, x, y, tasks) {
      const tileName = `${z}/${x}/${y}`;

      if (
        (refreshTimestamp === true &&
          tileExtraInfo[tileName] &&
          tileExtraInfo[tileName] === targetTileExtraInfo[tileName]) ||
        (refreshTimestamp && tileExtraInfo[tileName] >= refreshTimestamp)
      ) {
        return;
      }

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
          storeTransparent,
          headers
        );
      } catch (error) {
        printLog(
          "error",
          `Failed to seed data "${id}" - Tile "${tileName}" - From "${targetURL}" - ${completeTasks}/${total}: ${error}`
        );
      }
    }

    printLog("info", "Downloading datas...");

    await handleTilesConcurrency(concurrency, seedMBTilesTileData, tileBounds);

    printLog(
      "info",
      `Completed seed ${total} tiles of mbtiles "${id}" after ${(Date.now() - startTime) / 1000
      }s!`
    );
  } catch (error) {
    throw error;
  } finally {
    // Close MBTiles SQLite database
    if (source) {
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
 * @param {number} concurrency Concurrency
 * @param {number} maxTry Number of retry attempts on failure
 * @param {number} timeout Timeout in milliseconds
 * @param {boolean} storeTransparent Is store transparent tile?
 * @param {string|number|boolean} refreshBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be refreshed/Compare MD5
 * @param {object} headers Headers
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
  refreshBefore,
  headers
) {
  const startTime = Date.now();

  let source;

  try {
    /* Calculate summary */
    const { realBBox, total, tileBounds } = getTileBounds({
      coverages: coverages,
    });

    let log = `Seeding ${total} tiles of postgresql "${id}" with:`;
    log += `\n\tURL: ${url} - Header: ${JSON.stringify(
      headers
    )} - Scheme: ${scheme}`;
    log += `\n\tStore transparent: ${storeTransparent}`;
    log += `\n\tConcurrency: ${concurrency} - Max try: ${maxTry} - Timeout: ${timeout}`;
    log += `\n\tCoverages: ${JSON.stringify(coverages)}`;

    let refreshTimestamp;
    if (typeof refreshBefore === "string") {
      refreshTimestamp = new Date(refreshBefore).getTime();

      log += `\n\tRefresh before: ${refreshBefore}`;
    } else if (typeof refreshBefore === "number") {
      const now = new Date();

      refreshTimestamp = now.setDate(now.getDate() - refreshBefore);

      log += `\n\tOld than: ${refreshBefore} days`;
    } else if (refreshBefore === true) {
      refreshTimestamp = true;

      log += `\n\tRefresh before: check MD5`;
    }

    printLog("info", log);

    /* Open PostgreSQL database */
    const filePath = `${process.env.POSTGRESQL_BASE_URI}/${id}`;

    source = await openPostgreSQLDB(filePath, true);

    /* Get tile extra info */
    let targetTileExtraInfo;
    let tileExtraInfo;

    if (refreshTimestamp === true) {
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
          3600000, // 1 hours
          coverages,
          "arraybuffer",
          false,
          {
            "Content-Type": "application/json",
          }
        );

        if (res.headers["content-encoding"] === "gzip") {
          targetTileExtraInfo = JSON.parse(await unzipAsync(res.data));
        } else {
          targetTileExtraInfo = JSON.parse(res.data);
        }

        tileExtraInfo = getPostgreSQLTileExtraInfoFromCoverages(
          source,
          coverages,
          false
        );
      } catch (error) {
        if (error.statusCode >= 500) {
          printLog(
            "error",
            `Failed to get target tile extra info from "${hashURL}": ${error}. Skipping seed postgresql "${id}"...`
          );

          return;
        }

        printLog(
          "error",
          `Failed to get target tile extra info from "${hashURL}" and tile extra info from "${filePath}": ${error}`
        );

        targetTileExtraInfo = {};
        tileExtraInfo = {};
      }
    } else if (refreshTimestamp) {
      try {
        printLog("info", `Get tile extra info from "${filePath}"...`);

        tileExtraInfo = getPostgreSQLTileExtraInfoFromCoverages(
          source,
          coverages,
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
    printLog("info", "Updating PostgreSQL metadata...");

    await updatePostgreSQLMetadata(
      source,
      {
        ...metadata,
        bbox: realBBox,
      }
    );

    /* Download tiles */
    async function seedPostgreSQLTileData(z, x, y, tasks) {
      const tileName = `${z}/${x}/${y}`;

      if (
        (refreshTimestamp === true &&
          tileExtraInfo[tileName] &&
          tileExtraInfo[tileName] === targetTileExtraInfo[tileName]) ||
        (refreshTimestamp && tileExtraInfo[tileName] >= refreshTimestamp)
      ) {
        return;
      }

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
          storeTransparent,
          headers
        );
      } catch (error) {
        printLog(
          "error",
          `Failed to seed data "${id}" - Tile "${tileName}" - From "${targetURL}" - ${completeTasks}/${total}: ${error}`
        );
      }
    }

    printLog("info", "Downloading datas...");

    await handleTilesConcurrency(
      concurrency,
      seedPostgreSQLTileData,
      tileBounds
    );

    printLog(
      "info",
      `Completed seed ${total} tiles of postgresql "${id}" after ${(Date.now() - startTime) / 1000
      }s!`
    );
  } catch (error) {
    throw error;
  } finally {
    /* Close PostgreSQL database */
    if (source) {
      closePostgreSQLDB(source);
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
 * @param {number} concurrency Concurrency
 * @param {number} maxTry Number of retry attempts on failure
 * @param {number} timeout Timeout in milliseconds
 * @param {boolean} storeTransparent Is store transparent tile?
 * @param {string|number|boolean} refreshBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be refreshed/Compare MD5
 * @param {object} headers Headers
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
  refreshBefore,
  headers
) {
  const startTime = Date.now();

  let source;

  try {
    /* Calculate summary */
    const { realBBox, total, tileBounds } = getTileBounds({
      coverages: coverages,
    });

    let log = `Seeding ${total} tiles of xyz "${id}" with:`;
    log += `\n\tURL: ${url} - Header: ${JSON.stringify(
      headers
    )} - Scheme: ${scheme}`;
    log += `\n\tStore transparent: ${storeTransparent}`;
    log += `\n\tConcurrency: ${concurrency} - Max try: ${maxTry} - Timeout: ${timeout}`;
    log += `\n\tCoverages: ${JSON.stringify(coverages)}`;

    let refreshTimestamp;
    if (typeof refreshBefore === "string") {
      refreshTimestamp = new Date(refreshBefore).getTime();

      log += `\n\tRefresh before: ${refreshBefore}`;
    } else if (typeof refreshBefore === "number") {
      const now = new Date();

      refreshTimestamp = now.setDate(now.getDate() - refreshBefore);

      log += `\n\tOld than: ${refreshBefore} days`;
    } else if (refreshBefore === true) {
      refreshTimestamp = true;

      log += `\n\tRefresh before: check MD5`;
    }

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

    if (refreshTimestamp === true) {
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
          3600000, // 1 hours
          coverages,
          "arraybuffer",
          false,
          {
            "Content-Type": "application/json",
          }
        );

        if (res.headers["content-encoding"] === "gzip") {
          targetTileExtraInfo = JSON.parse(await unzipAsync(res.data));
        } else {
          targetTileExtraInfo = JSON.parse(res.data);
        }

        tileExtraInfo = getXYZTileExtraInfoFromCoverages(
          source,
          coverages,
          false
        );
      } catch (error) {
        if (error.statusCode >= 500) {
          printLog(
            "error",
            `Failed to get target tile extra info from "${hashURL}": ${error}. Skipping seed xyz "${id}"...`
          );

          return;
        }

        printLog(
          "error",
          `Failed to get target tile extra info from "${hashURL}" and tile extra info from "${filePath}": ${error}`
        );

        targetTileExtraInfo = {};
        tileExtraInfo = {};
      }
    } else if (refreshTimestamp) {
      try {
        printLog("info", `Get tile extra info from "${filePath}"...`);

        tileExtraInfo = getXYZTileExtraInfoFromCoverages(
          source,
          coverages,
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
    printLog("info", "Updating XYZ metadata...");

    await updateXYZMetadata(
      source,
      {
        ...metadata,
        bbox: realBBox,
      },
      30000 // 30 secs
    );

    /* Download tile files */
    async function seedXYZTileData(z, x, y, tasks) {
      const tileName = `${z}/${x}/${y}`;

      if (
        (refreshTimestamp === true &&
          tileExtraInfo[tileName] &&
          tileExtraInfo[tileName] === targetTileExtraInfo[tileName]) ||
        (refreshTimestamp && tileExtraInfo[tileName] >= refreshTimestamp)
      ) {
        return;
      }

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
          storeTransparent,
          headers
        );
      } catch (error) {
        printLog(
          "error",
          `Failed to seed data "${id}" - Tile "${tileName}" - From "${targetURL}" - ${completeTasks}/${total}: ${error}`
        );
      }
    }

    printLog("info", "Downloading datas...");

    await handleTilesConcurrency(concurrency, seedXYZTileData, tileBounds);

    /* Remove parent folders if empty */
    await removeEmptyFolders(sourcePath, /^.*\.(gif|png|jpg|jpeg|webp|pbf)$/);

    printLog(
      "info",
      `Completed seed ${total} tiles of xyz "${id}" after ${(Date.now() - startTime) / 1000
      }s!`
    );
  } catch (error) {
    throw error;
  } finally {
    /* Close MD5 SQLite database */
    if (source) {
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
 * @param {object} headers Headers
 * @returns {Promise<void>}
 */
async function seedGeoJSON(id, url, maxTry, timeout, refreshBefore, headers) {
  const startTime = Date.now();

  let log = `Seeding geojson "${id}" with:`;
  log += `\n\tURL: ${url} - Header: ${JSON.stringify(headers)}`;
  log += `\n\tMax try: ${maxTry} - Timeout: ${timeout}`;

  let refreshTimestamp;
  if (typeof refreshBefore === "string") {
    refreshTimestamp = new Date(refreshBefore).getTime();

    log += `\n\tRefresh before: ${refreshBefore}`;
  } else if (typeof refreshBefore === "number") {
    const now = new Date();

    refreshTimestamp = now.setDate(now.getDate() - refreshBefore);

    log += `\n\tOld than: ${refreshBefore} days`;
  } else if (refreshBefore === true) {
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
          getGeoJSON(filePath),
        ]);

        if (
          !response.headers["etag"] ||
          response.headers["etag"] !== calculateMD5(geoJSONData)
        ) {
          needDownload = true;
        }
      } catch (error) {
        if (error.message === "JSON does not exist") {
          needDownload = true;
        } else {
          throw error;
        }
      }
    } else if (refreshTimestamp) {
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

    if (needDownload) {
      printLog(
        "info",
        `Downloading geojson "${id}" - File "${filePath}" - From "${url}"...`
      );

      await downloadGeoJSONFile(url, filePath, maxTry, timeout, headers);
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
 * @param {object} headers Headers
 * @returns {Promise<void>}
 */
async function seedSprite(id, url, maxTry, timeout, refreshBefore, headers) {
  const startTime = Date.now();

  let log = `Seeding sprite "${id}" with:`;
  log += `\n\tURL: ${url} - Header: ${JSON.stringify(headers)}`;
  log += `\n\tMax try: ${maxTry} - Timeout: ${timeout}`;

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

      if (refreshTimestamp) {
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

      if (needDownload) {
        const targetURL = url.replace("{name}", `${fileName}`);

        printLog(
          "info",
          `Downloading sprite "${id}" - File "${fileName}" - From "${targetURL}"...`
        );

        await downloadSpriteFile(
          targetURL,
          id,
          fileName,
          maxTry,
          timeout,
          headers
        );
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
 * @param {number} concurrency Concurrency
 * @param {number} maxTry Number of retry attempts on failure
 * @param {number} timeout Timeout in milliseconds
 * @param {string|number} refreshBefore Date string in format "YYYY-MM-DDTHH:mm:ss" or number of days before which file should be refreshed
 * @param {object} headers Headers
 * @returns {Promise<void>}
 */
async function seedFont(
  id,
  url,
  concurrency,
  maxTry,
  timeout,
  refreshBefore,
  headers
) {
  const startTime = Date.now();

  const total = 256;

  let log = `Seeding ${total} fonts of font "${id}" with:`;
  log += `\n\tURL: ${url} - Header: ${JSON.stringify(headers)}`;
  log += `\n\tConcurrency: ${concurrency} - Max try: ${maxTry} - Timeout: ${timeout}`;

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

  async function seedFontData(idx, ranges, tasks) {
    const fileName = `${ranges[idx]}.pbf`;
    const filePath = `${sourcePath}/${fileName}`;
    const completeTasks = tasks.completeTasks;

    try {
      let needDownload = false;

      if (refreshTimestamp) {
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

      if (needDownload) {
        const targetURL = url.replace("{range}.pbf", `${ranges[idx]}.pbf`);

        printLog(
          "info",
          `Downloading font "${id}" - Filename "${fileName}" - From "${targetURL}" - ${completeTasks}/${total}...`
        );

        await downloadFontFile(
          targetURL,
          id,
          fileName,
          maxTry,
          timeout,
          headers
        );
      }
    } catch (error) {
      printLog(
        "error",
        `Failed to seed font "${id}" - Filename "${fileName}" - ${completeTasks}/${total}: ${error}`
      );
    }
  }

  printLog("info", "Downloading fonts...");

  await handleConcurrency(
    concurrency,
    seedFontData,
    Array.from({ length: 256 }, (_, idx) => `${idx * 256}-${idx * 256 + 255}`)
  );

  /* Remove parent folders if empty */
  await removeEmptyFolders(sourcePath, /^.*\.pbf$/);

  printLog(
    "info",
    `Completed seed ${total} fonts of font "${id}" after ${(Date.now() - startTime) / 1000
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
 * @param {object} headers Headers
 * @returns {Promise<void>}
 */
async function seedStyle(id, url, maxTry, timeout, refreshBefore, headers) {
  const startTime = Date.now();

  let log = `Seeding style "${id}" with:`;
  log += `\n\tURL: ${url} - Header: ${JSON.stringify(headers)}`;
  log += `\n\tMax try: ${maxTry} - Timeout: ${timeout}`;

  let refreshTimestamp;
  if (typeof refreshBefore === "string") {
    refreshTimestamp = new Date(refreshBefore).getTime();

    log += `\n\tRefresh before: ${refreshBefore}`;
  } else if (typeof refreshBefore === "number") {
    const now = new Date();

    refreshTimestamp = now.setDate(now.getDate() - refreshBefore);

    log += `\n\tOld than: ${refreshBefore} days`;
  } else if (refreshBefore === true) {
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
          getStyle(filePath),
        ]);

        if (
          !response.headers["etag"] ||
          response.headers["etag"] !== calculateMD5(styleData)
        ) {
          needDownload = true;
        }
      } catch (error) {
        if (error.message === "JSON does not exist") {
          needDownload = true;
        } else {
          throw error;
        }
      }
    } else if (refreshTimestamp) {
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

    if (refreshTimestamp) {
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

    if (needDownload) {
      printLog(
        "info",
        `Downloading style "${id}" - File "${filePath}" - From "${url}"...`
      );

      await downloadStyleFile(url, filePath, maxTry, timeout, headers);
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
