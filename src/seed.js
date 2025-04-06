"use strict";

import { downloadSpriteFile, getSpriteCreated } from "./sprite.js";
import { downloadStyleFile, getStyleCreated } from "./style.js";
import { downloadFontFile, getFontCreated } from "./font.js";
import fsPromise from "node:fs/promises";
import { printLog } from "./logger.js";
import { Mutex } from "async-mutex";
import sqlite3 from "sqlite3";
import {
  downloadGeoJSONFile,
  getGeoJSONCreated,
  getGeoJSON,
} from "./geojson.js";
import {
  updateXYZMetadata,
  getXYZTileCreated,
  downloadXYZTile,
  closeXYZMD5DB,
  getXYZTileMD5,
  openXYZMD5DB,
} from "./tile_xyz.js";
import {
  updateMBTilesMetadata,
  getMBTilesTileCreated,
  downloadMBTilesTile,
  getMBTilesTileMD5,
  closeMBTilesDB,
  openMBTilesDB,
} from "./tile_mbtiles.js";
import {
  getTileBoundsFromCoverages,
  createFileWithLock,
  removeEmptyFolders,
  getDataFromURL,
  getJSONSchema,
  validateJSON,
  calculateMD5,
  delay,
} from "./utils.js";
import {
  updatePostgreSQLMetadata,
  getPostgreSQLTileCreated,
  downloadPostgreSQLTile,
  getPostgreSQLTileMD5,
  closePostgreSQLDB,
  openPostgreSQLDB,
} from "./tile_postgresql.js";

let seed = {};

/**
 * Read seed.json file
 * @param {boolean} isValidate Is validate file content?
 * @returns {Promise<Object>}
 */
async function readSeedFile(isValidate) {
  /* Read seed.json file */
  const data = await fsPromise.readFile(
    `${process.env.DATA_DIR}/seed.json`,
    "utf8"
  );

  Object.assign(seed, JSON.parse(data));

  /* Validate seed.json file */
  if (isValidate === true) {
    validateJSON(await getJSONSchema("seed"), seed);
  }

  return seed;
}

/**
 * Load seed.json file content to global variable
 * @returns {Promise<void>}
 */
async function loadSeedFile() {
  seed = await readSeedFile(true);
}

/**
 * Update seed.json file content with lock
 * @param {Object} seed Seed object
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
 * @param {Object} metadata Metadata object
 * @param {string} url Tile URL to download
 * @param {"tms"|"xyz"} scheme Tile scheme
 * @param {{ zoom: number, bbox: [number, number, number, number]}[]} coverages Specific coverages
 * @param {number} concurrency Concurrency download
 * @param {number} maxTry Number of retry attempts on failure
 * @param {number} timeout Timeout in milliseconds
 * @param {boolean} storeMD5 Is store MD5 hashed?
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
  storeMD5,
  storeTransparent,
  refreshBefore
) {
  const startTime = Date.now();

  /* Calculate summary */
  const { total, tileBounds } = getTileBoundsFromCoverages(coverages, "xyz");

  let log = `Seeding ${total} tiles of mbtiles "${id}" with:\n\tStore MD5: ${storeMD5}\n\tStore transparent: ${storeTransparent}\n\tConcurrency: ${concurrency}\n\tMax try: ${maxTry}\n\tTimeout: ${timeout}\n\tBBoxs: ${JSON.stringify(
    coverages
  )}`;

  let hashs;
  let refreshTimestamp;
  if (typeof refreshBefore === "string") {
    refreshTimestamp = new Date(refreshBefore).getTime();

    log += `\n\tRefresh before: ${refreshBefore}`;

    printLog("info", log);
  } else if (typeof refreshBefore === "number") {
    const now = new Date();

    refreshTimestamp = now.setDate(now.getDate() - refreshBefore);

    log += `\n\tOld than: ${refreshBefore} days`;

    printLog("info", log);
  } else if (typeof refreshBefore === "boolean") {
    refreshTimestamp = true;

    log += `\n\tRefresh before: check MD5`;

    printLog("info", log);

    /* Get hashs */
    printLog("info", "Get hashs...");

    try {
      hashs = await getDataFromURL(
        `${url.slice(0, url.indexOf("/{z}/{x}/{y}"))}/md5`,
        300000, // 5 mins
        "json"
      );
    } catch (error) {
      printLog("error", `Failed to get hashs: ${error}`);

      hashs = {};
    }
  }

  /* Open MBTiles SQLite database */
  const source = await openMBTilesDB(
    `${process.env.DATA_DIR}/caches/mbtiles/${id}/${id}.mbtiles`,
    sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE,
    false
  );

  /* Update metadata */
  printLog("info", "Updating metadata...");

  await updateMBTilesMetadata(
    source,
    metadata,
    300000 // 5 mins
  );

  /* Download tiles */
  const tasks = {
    mutex: new Mutex(),
    activeTasks: 0,
    completeTasks: 0,
  };

  async function seedMBTilesTileData(z, x, y, tasks) {
    const tileName = `${z}/${x}/${y}`;

    const completeTasks = tasks.completeTasks;

    try {
      let needDownload = false;

      if (refreshTimestamp === true) {
        try {
          const md5 = await getMBTilesTileMD5(source, z, x, y);

          if (md5 !== hashs[tileName]) {
            needDownload = true;
          }
        } catch (error) {
          if (error.message === "Tile MD5 does not exist") {
            needDownload = true;
          } else {
            throw error;
          }
        }
      } else if (refreshTimestamp !== undefined) {
        try {
          const created = await getMBTilesTileCreated(source, z, x, y);

          if (!created || created < refreshTimestamp) {
            needDownload = true;
          }
        } catch (error) {
          if (error.message === "Tile created does not exist") {
            needDownload = true;
          } else {
            throw error;
          }
        }
      } else {
        needDownload = true;
      }

      if (needDownload === true) {
        const tmpY = scheme === "tms" ? (1 << z) - 1 - y : y;

        const targetURL = url
          .replace("{z}", `${z}`)
          .replace("{x}", `${x}`)
          .replace("{y}", `${tmpY}`);

        printLog(
          "info",
          `Downloading data "${id}" - Tile "${tileName}" - From "${targetURL}" - ${completeTasks}/${total}...`
        );

        await downloadMBTilesTile(
          targetURL,
          source,
          z,
          x,
          tmpY,
          maxTry,
          timeout,
          storeMD5,
          storeTransparent
        );
      }
    } catch (error) {
      printLog(
        "error",
        `Failed to seed data "${id}" - Tile "${tileName}" - ${completeTasks}/${total}: ${error}`
      );
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

  // Close MBTiles SQLite database
  await closeMBTilesDB(source);

  printLog(
    "info",
    `Completed seed ${total} tiles of mbtiles "${id}" after ${
      (Date.now() - startTime) / 1000
    }s!`
  );
}

/**
 * Seed PostgreSQL tiles
 * @param {string} id Cache PostgreSQL ID
 * @param {Object} metadata Metadata object
 * @param {string} url Tile URL to download
 * @param {"tms"|"xyz"} scheme Tile scheme
 * @param {{ zoom: number, bbox: [number, number, number, number]}[]} coverages Specific coverages
 * @param {number} concurrency Concurrency download
 * @param {number} maxTry Number of retry attempts on failure
 * @param {number} timeout Timeout in milliseconds
 * @param {boolean} storeMD5 Is store MD5 hashed?
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
  storeMD5,
  storeTransparent,
  refreshBefore
) {
  const startTime = Date.now();

  /* Calculate summary */
  const { total, tileBounds } = getTileBoundsFromCoverages(coverages, "xyz");

  let log = `Seeding ${total} tiles of postgresql "${id}" with:\n\tStore MD5: ${storeMD5}\n\tStore transparent: ${storeTransparent}\n\tConcurrency: ${concurrency}\n\tMax try: ${maxTry}\n\tTimeout: ${timeout}\n\tBBoxs: ${JSON.stringify(
    coverages
  )}`;

  let hashs;
  let refreshTimestamp;
  if (typeof refreshBefore === "string") {
    refreshTimestamp = new Date(refreshBefore).getTime();

    log += `\n\tRefresh before: ${refreshBefore}`;

    printLog("info", log);
  } else if (typeof refreshBefore === "number") {
    const now = new Date();

    refreshTimestamp = now.setDate(now.getDate() - refreshBefore);

    log += `\n\tOld than: ${refreshBefore} days`;

    printLog("info", log);
  } else if (typeof refreshBefore === "boolean") {
    refreshTimestamp = true;

    log += `\n\tRefresh before: check MD5`;

    printLog("info", log);

    /* Get hashs */
    printLog("info", "Get hashs...");

    try {
      hashs = await getDataFromURL(
        `${url.slice(0, url.indexOf("/{z}/{x}/{y}"))}/md5`,
        300000, // 5 mins
        "json"
      );
    } catch (error) {
      printLog("error", `Failed to get hashs: ${error}`);

      hashs = {};
    }
  }

  /* Open PostgreSQL database */
  const source = await openPostgreSQLDB(
    `${process.env.POSTGRESQL_BASE_URI}/${id}`,
    true
  );

  /* Update metadata */
  printLog("info", "Updating metadata...");

  await updatePostgreSQLMetadata(
    source,
    metadata,
    300000 // 5 mins
  );

  /* Download tiles */
  const tasks = {
    mutex: new Mutex(),
    activeTasks: 0,
    completeTasks: 0,
  };

  async function seedPostgreSQLTileData(z, x, y, tasks) {
    const tileName = `${z}/${x}/${y}`;

    const completeTasks = tasks.completeTasks;

    try {
      let needDownload = false;

      if (refreshTimestamp === true) {
        try {
          const md5 = await getPostgreSQLTileMD5(source, z, x, y);

          if (md5 !== hashs[tileName]) {
            needDownload = true;
          }
        } catch (error) {
          if (error.message === "Tile MD5 does not exist") {
            needDownload = true;
          } else {
            throw error;
          }
        }
      } else if (refreshTimestamp !== undefined) {
        try {
          const created = await getPostgreSQLTileCreated(source, z, x, y);

          if (!created || created < refreshTimestamp) {
            needDownload = true;
          }
        } catch (error) {
          if (error.message === "Tile created does not exist") {
            needDownload = true;
          } else {
            throw error;
          }
        }
      } else {
        needDownload = true;
      }

      if (needDownload === true) {
        const tmpY = scheme === "tms" ? (1 << z) - 1 - y : y;

        const targetURL = url
          .replace("{z}", `${z}`)
          .replace("{x}", `${x}`)
          .replace("{y}", `${tmpY}`);

        printLog(
          "info",
          `Downloading data "${id}" - Tile "${tileName}" - From "${targetURL}" - ${completeTasks}/${total}...`
        );

        await downloadPostgreSQLTile(
          targetURL,
          source,
          z,
          x,
          tmpY,
          maxTry,
          timeout,
          storeMD5,
          storeTransparent
        );
      }
    } catch (error) {
      printLog(
        "error",
        `Failed to seed data "${id}" - Tile "${tileName}" - ${completeTasks}/${total}: ${error}`
      );
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

  /* Close PostgreSQL database */
  await closePostgreSQLDB(source);

  printLog(
    "info",
    `Completed seed ${total} tiles of postgresql "${id}" after ${
      (Date.now() - startTime) / 1000
    }s!`
  );
}

/**
 * Seed XYZ tiles
 * @param {string} id Cache XYZ ID
 * @param {Object} metadata Metadata object
 * @param {string} url Tile URL
 * @param {"tms"|"xyz"} scheme Tile scheme
 * @param {{ zoom: number, bbox: [number, number, number, number]}[]} coverages Specific coverages
 * @param {number} concurrency Concurrency to download
 * @param {number} maxTry Number of retry attempts on failure
 * @param {number} timeout Timeout in milliseconds
 * @param {boolean} storeMD5 Is store MD5 hashed?
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
  storeMD5,
  storeTransparent,
  refreshBefore
) {
  const startTime = Date.now();

  /* Calculate summary */
  const { total, tileBounds } = getTileBoundsFromCoverages(coverages, "xyz");

  let log = `Seeding ${total} tiles of xyz "${id}" with:\n\tStore MD5: ${storeMD5}\n\tStore transparent: ${storeTransparent}\n\tConcurrency: ${concurrency}\n\tMax try: ${maxTry}\n\tTimeout: ${timeout}\n\tBBoxs: ${JSON.stringify(
    coverages
  )}`;

  let hashs;
  let refreshTimestamp;
  if (typeof refreshBefore === "string") {
    refreshTimestamp = new Date(refreshBefore).getTime();

    log += `\n\tRefresh before: ${refreshBefore}`;

    printLog("info", log);
  } else if (typeof refreshBefore === "number") {
    const now = new Date();

    refreshTimestamp = now.setDate(now.getDate() - refreshBefore);

    log += `\n\tOld than: ${refreshBefore} days`;

    printLog("info", log);
  } else if (typeof refreshBefore === "boolean") {
    refreshTimestamp = true;

    log += `\n\tRefresh before: check MD5`;

    printLog("info", log);

    /* Get hashs */
    printLog("info", "Get hashs...");

    try {
      hashs = await getDataFromURL(
        `${url.slice(0, url.indexOf("/{z}/{x}/{y}"))}/md5`,
        300000, // 5 mins
        "json"
      );
    } catch (error) {
      printLog("error", `Failed to get hashs: ${error}`);

      hashs = {};
    }
  }

  /* Open MD5 SQLite database */
  const source = await openXYZMD5DB(
    `${process.env.DATA_DIR}/caches/xyzs/${id}/${id}.sqlite`,
    sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE,
    false
  );

  /* Update metadata */
  printLog("info", "Updating metadata...");

  await updateXYZMetadata(
    source,
    metadata,
    300000 // 5 mins
  );

  /* Download tile files */
  const tasks = {
    mutex: new Mutex(),
    activeTasks: 0,
    completeTasks: 0,
  };

  async function seedXYZTileData(z, x, y, tasks) {
    const tileName = `${z}/${x}/${y}`;

    const completeTasks = tasks.completeTasks;

    try {
      let needDownload = false;

      if (refreshTimestamp === true) {
        try {
          const md5 = await getXYZTileMD5(source, z, x, y);

          if (md5 !== hashs[tileName]) {
            needDownload = true;
          }
        } catch (error) {
          if (error.message === "Tile MD5 does not exist") {
            needDownload = true;
          } else {
            throw error;
          }
        }
      } else if (refreshTimestamp !== undefined) {
        try {
          const created = await getXYZTileCreated(source, z, x, y);

          if (!created || created < refreshTimestamp) {
            needDownload = true;
          }
        } catch (error) {
          if (error.message === "Tile created does not exist") {
            needDownload = true;
          } else {
            throw error;
          }
        }
      } else {
        needDownload = true;
      }

      if (needDownload === true) {
        const tmpY = scheme === "tms" ? (1 << z) - 1 - y : y;

        const targetURL = url
          .replace("{z}", `${z}`)
          .replace("{x}", `${x}`)
          .replace("{y}", `${tmpY}`);

        printLog(
          "info",
          `Downloading data "${id}" - Tile "${tileName}" - From "${targetURL}" - ${completeTasks}/${total}...`
        );

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
          storeMD5,
          storeTransparent
        );
      }
    } catch (error) {
      printLog(
        "error",
        `Failed to seed data "${id}" - Tile "${tileName}" - ${completeTasks}/${total}: ${error}`
      );
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

  /* Close MD5 SQLite database */
  await closeXYZMD5DB(source);

  /* Remove parent folders if empty */
  await removeEmptyFolders(
    `${process.env.DATA_DIR}/caches/xyzs/${id}`,
    /^.*\.(sqlite|gif|png|jpg|jpeg|webp|pbf)$/
  );

  printLog(
    "info",
    `Completed seed ${total} tiles of xyz "${id}" after ${
      (Date.now() - startTime) / 1000
    }s!`
  );
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

  let log = `Seeding geojson "${id}" with:\n\tMax try: ${maxTry}\n\tTimeout: ${timeout}`;

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
  const filePath = `${process.env.DATA_DIR}/caches/geojsons/${id}/${id}.geojson`;

  try {
    let needDownload = false;

    if (refreshTimestamp === true) {
      try {
        const [response, geoJSONData] = await Promise.all([
          getDataFromURL(
            url.replace(`${id}.geojson`, `${id}/md5`),
            timeout,
            "arraybuffer"
          ),
          getGeoJSON(filePath, false),
        ]);

        if (response.headers["etag"] !== calculateMD5(geoJSONData)) {
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

        if (!created || created < refreshTimestamp) {
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
  await removeEmptyFolders(
    `${process.env.DATA_DIR}/caches/geojsons/${id}`,
    /^.*\.geojson$/
  );

  printLog(
    "info",
    `Completed seeding geojson "${id}" after ${
      (Date.now() - startTime) / 1000
    }s!`
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

  let log = `Seeding sprite "${id}" with:\n\tMax try: ${maxTry}\n\tTimeout: ${timeout}`;

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
  async function seedSpriteData(fileName) {
    const filePath = `${process.env.DATA_DIR}/caches/sprites/${id}/${fileName}`;

    try {
      let needDownload = false;

      if (refreshTimestamp !== undefined) {
        try {
          const created = await getSpriteCreated(filePath);

          if (!created || created < refreshTimestamp) {
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

        await downloadSpriteFile(url, id, fileName, maxTry, timeout);
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
  await removeEmptyFolders(
    `${process.env.DATA_DIR}/caches/sprites/${id}`,
    /^.*\.(json|png)$/
  );

  printLog(
    "info",
    `Completed seeding sprite "${id}" after ${
      (Date.now() - startTime) / 1000
    }s!`
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

  let log = `Seeding ${total} fonts of font "${id}" with:\n\tConcurrency: ${concurrency}\n\tMax try: ${maxTry}\n\tTimeout: ${timeout}`;

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

  /* Remove font files */
  const tasks = {
    mutex: new Mutex(),
    activeTasks: 0,
    completeTasks: 0,
  };

  async function seedFontData(start, end, tasks) {
    const range = `${start}-${end}`;
    const filePath = `${process.env.DATA_DIR}/caches/fonts/${id}/${range}.pbf`;

    const completeTasks = tasks.completeTasks;

    try {
      let needDownload = false;

      if (refreshTimestamp !== undefined) {
        try {
          const created = await getFontCreated(filePath);

          if (!created || created < refreshTimestamp) {
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
  await removeEmptyFolders(
    `${process.env.DATA_DIR}/caches/fonts/${id}`,
    /^.*\.pbf$/
  );

  printLog(
    "info",
    `Completed seeding ${total} fonts of font "${id}" after ${
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

  let log = `Seeding style "${id}" with:\n\tMax try: ${maxTry}\n\tTimeout: ${timeout}`;

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

  /* Download style.json file */
  const filePath = `${process.env.DATA_DIR}/caches/styles/${id}/style.json`;

  try {
    let needDownload = false;

    if (refreshTimestamp !== undefined) {
      try {
        const created = await getStyleCreated(filePath);

        if (!created || created < refreshTimestamp) {
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
  await removeEmptyFolders(
    `${process.env.DATA_DIR}/caches/styles/${id}`,
    /^.*\.json$/
  );

  printLog(
    "info",
    `Completed seeding style "${id}" after ${(Date.now() - startTime) / 1000}s!`
  );
}

export {
  seedPostgreSQLTiles,
  seedMBTilesTiles,
  updateSeedFile,
  readSeedFile,
  seedXYZTiles,
  loadSeedFile,
  seedGeoJSON,
  seedSprite,
  seedStyle,
  seedFont,
  seed,
};
