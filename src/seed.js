"use strict";

import { downloadSpriteFile, getSpriteCreated } from "./sprite.js";
import { downloadStyleFile, getStyleCreated } from "./style.js";
import { downloadFontFile, getFontCreated } from "./font.js";
import fsPromise from "node:fs/promises";
import { printLog } from "./logger.js";
import { Mutex } from "async-mutex";
import {
  downloadGeoJSONFile,
  getGeoJSONCreated,
  getGeoJSON,
} from "./geojson.js";
import {
  getXYZTileCreated,
  updateXYZMetadata,
  downloadXYZTile,
  closeXYZMD5DB,
  getXYZTileMD5,
  openXYZMD5DB,
} from "./tile_xyz.js";
import {
  getMBTilesTileCreated,
  updateMBTilesMetadata,
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
  postDataToURL,
  validateJSON,
  calculateMD5,
  delay,
} from "./utils.js";
import {
  getPostgreSQLTileCreated,
  updatePostgreSQLMetadata,
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

  const seed = JSON.parse(data);

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
  Object.assign(seed, await readSeedFile(true));
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
    const { total, tileBounds } = getTileBoundsFromCoverages(coverages, "xyz");

    let log = `Seeding ${total} tiles of mbtiles "${id}" with:\n\tStore transparent: ${storeTransparent}\n\tConcurrency: ${concurrency}\n\tMax try: ${maxTry}\n\tTimeout: ${timeout}\n\tCoverages: ${JSON.stringify(
      coverages
    )}`;

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

    /* Get hashs */
    let hashs;
    const hashURL = `${url.slice(0, url.indexOf("/{z}/{x}/{y}"))}/md5s`;

    try {
      printLog("info", `Get hashs from "${hashURL}"...`);

      const res = await postDataToURL(
        hashURL,
        300000, // 5 mins
        coverages,
        "json"
      );

      hashs = res.data;
    } catch (error) {
      printLog("error", `Failed to get hashs from "${hashURL}": ${error}`);

      hashs = {};
    }

    /* Open MBTiles SQLite database */
    source = await openMBTilesDB(
      `${process.env.DATA_DIR}/caches/mbtiles/${id}/${id}.mbtiles`,
      true,
      30000 // 30 secs
    );

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

      const completeTasks = tasks.completeTasks;

      try {
        let needDownload = false;

        if (refreshTimestamp === true) {
          try {
            const md5 = getMBTilesTileMD5(source, z, x, y);

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
            const created = getMBTilesTileCreated(source, z, x, y);

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

    printLog(
      "info",
      `Completed seed ${total} tiles of mbtiles "${id}" after ${
        (Date.now() - startTime) / 1000
      }s!`
    );
  } catch (error) {
    printLog(
      "error",
      `Failed to seed mbtiles "${id}" after ${
        (Date.now() - startTime) / 1000
      }s: ${error}`
    );
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
 * @param {Object} metadata Metadata object
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
    const { total, tileBounds } = getTileBoundsFromCoverages(coverages, "xyz");

    let log = `Seeding ${total} tiles of postgresql "${id}" with:\n\tStore transparent: ${storeTransparent}\n\tConcurrency: ${concurrency}\n\tMax try: ${maxTry}\n\tTimeout: ${timeout}\n\tCoverages: ${JSON.stringify(
      coverages
    )}`;

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

    /* Get hashs */
    let hashs;
    const hashURL = `${url.slice(0, url.indexOf("/{z}/{x}/{y}"))}/md5s`;

    try {
      printLog("info", `Get hashs from "${hashURL}"...`);

      const res = await postDataToURL(
        hashURL,
        300000, // 5 mins
        coverages,
        "json"
      );

      hashs = res.data;
    } catch (error) {
      printLog("error", `Failed to get hashs from "${hashURL}": ${error}`);

      hashs = {};
    }

    /* Open PostgreSQL database */
    source = await openPostgreSQLDB(
      `${process.env.POSTGRESQL_BASE_URI}/${id}`,
      true
    );

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

    printLog(
      "info",
      `Completed seed ${total} tiles of postgresql "${id}" after ${
        (Date.now() - startTime) / 1000
      }s!`
    );
  } catch (error) {
    printLog(
      "error",
      `Failed to seed postgresql "${id}" after ${
        (Date.now() - startTime) / 1000
      }s: ${error}`
    );
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
 * @param {Object} metadata Metadata object
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
    const { total, tileBounds } = getTileBoundsFromCoverages(coverages, "xyz");

    let log = `Seeding ${total} tiles of xyz "${id}" with:\n\tStore transparent: ${storeTransparent}\n\tConcurrency: ${concurrency}\n\tMax try: ${maxTry}\n\tTimeout: ${timeout}\n\tCoverages: ${JSON.stringify(
      coverages
    )}`;

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

    /* Get hashs */
    let hashs;
    const hashURL = `${url.slice(0, url.indexOf("/{z}/{x}/{y}"))}/md5s`;

    try {
      printLog("info", `Get hashs from "${hashURL}"...`);

      const res = await postDataToURL(
        hashURL,
        300000, // 5 mins
        coverages,
        "json"
      );

      hashs = res.data;
    } catch (error) {
      printLog("error", `Failed to get hashs from "${hashURL}": ${error}`);

      hashs = {};
    }

    /* Open MD5 SQLite database */
    source = await openXYZMD5DB(
      `${process.env.DATA_DIR}/caches/xyzs/${id}/${id}.sqlite`,
      true,
      30000 // 30 secs
    );

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

      const completeTasks = tasks.completeTasks;

      try {
        let needDownload = false;

        if (refreshTimestamp === true) {
          try {
            const md5 = getXYZTileMD5(source, z, x, y);

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
            const created = getXYZTileCreated(source, z, x, y);

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

    /* Remove parent folders if empty */
    await removeEmptyFolders(
      `${process.env.DATA_DIR}/caches/xyzs/${id}`,
      /^.*\.(gif|png|jpg|jpeg|webp|pbf)$/
    );

    printLog(
      "info",
      `Completed seed ${total} tiles of xyz "${id}" after ${
        (Date.now() - startTime) / 1000
      }s!`
    );
  } catch (error) {
    printLog(
      "error",
      `Failed to seed xyz "${id}" after ${
        (Date.now() - startTime) / 1000
      }s: ${error}`
    );
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

  try {
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
      `Completed seed geojson "${id}" after ${
        (Date.now() - startTime) / 1000
      }s!`
    );
  } catch (error) {
    printLog(
      "error",
      `Failed to seed geojson "${id}" after ${
        (Date.now() - startTime) / 1000
      }s: ${error}`
    );
  }
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

  try {
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
      `Completed seed sprite "${id}" after ${(Date.now() - startTime) / 1000}s!`
    );
  } catch (error) {
    printLog(
      "error",
      `Failed to seed sprite "${id}" after ${
        (Date.now() - startTime) / 1000
      }s: ${error}`
    );
  }
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

  try {
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
      `Completed seed ${total} fonts of font "${id}" after ${
        (Date.now() - startTime) / 1000
      }s!`
    );
  } catch (error) {
    printLog(
      "error",
      `Failed to seed font "${id}" after ${
        (Date.now() - startTime) / 1000
      }s: ${error}`
    );
  }
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

  try {
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
      `Completed seeding style "${id}" after ${
        (Date.now() - startTime) / 1000
      }s!`
    );
  } catch (error) {
    printLog(
      "error",
      `Failed to seed style "${id}" after ${
        (Date.now() - startTime) / 1000
      }s: ${error}`
    );
  }
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
