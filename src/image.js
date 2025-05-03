"use strict";

import { cacheSpriteFile, getSprite, getSpriteFromURL } from "./sprite.js";
import { getPMTilesTile } from "./tile_pmtiles.js";
import { createPool } from "generic-pool";
import { printLog } from "./logger.js";
import { getFonts } from "./font.js";
import { config } from "./config.js";
import { Mutex } from "async-mutex";
import cluster from "cluster";
import {
  getPostgreSQLTileExtraInfoFromCoverages,
  updatePostgreSQLMetadata,
  cachePostgreSQLTileData,
  getPostgreSQLTile,
  closePostgreSQLDB,
  openPostgreSQLDB,
} from "./tile_postgresql.js";
import {
  getMBTilesTileExtraInfoFromCoverages,
  calculateMBTilesTileExtraInfo,
  updateMBTilesMetadata,
  cacheMBtilesTileData,
  getMBTilesTile,
  closeMBTilesDB,
  openMBTilesDB,
} from "./tile_mbtiles.js";
import {
  getTileBoundsFromCoverages,
  detectFormatAndHeaders,
  createFallbackTileData,
  renderImageTileData,
  removeEmptyFolders,
  getDataTileFromURL,
  getLonLatFromXYZ,
  processCoverages,
  getDataFromURL,
  calculateMD5,
  unzipAsync,
  runCommand,
  delay,
} from "./utils.js";
import {
  getXYZTileExtraInfoFromCoverages,
  updateXYZMetadata,
  cacheXYZTileFile,
  closeXYZMD5DB,
  openXYZMD5DB,
  getXYZTile,
} from "./tile_xyz.js";

let mlgl;

if (cluster.isPrimary !== true) {
  import("@maplibre/maplibre-gl-native")
    .then((module) => {
      mlgl = module.default;

      printLog(
        "info",
        `Success to import "@maplibre/maplibre-gl-native". Enable backend render!`
      );

      config.enableBackendRender = true;
    })
    .catch((error) => {
      printLog(
        "error",
        `Failed to import "@maplibre/maplibre-gl-native": ${error}. Disable backend render!`
      );
    });
}

/**
 * Create tile render
 * @param {number} tileScale Tile scale
 * @param {object} styleJSON StyleJSON
 * @returns {object}
 */
function createTileRenderer(tileScale, styleJSON) {
  const renderer = new mlgl.Map({
    mode: "tile",
    ratio: tileScale,
    request: async (req, callback) => {
      const url = decodeURIComponent(req.url);
      const parts = url.split("/");
      let data = null;
      let err = null;

      switch (parts[0]) {
        /* Get sprite */
        case "sprites:": {
          try {
            const item = config.sprites[parts[2]];

            try {
              data = await getSprite(parts[2], parts[3]);
            } catch (error) {
              if (
                item.sourceURL !== undefined &&
                error.message === "Sprite does not exist"
              ) {
                const targetURL = item.sourceURL.replace(
                  "/sprite",
                  `/${parts[3]}`
                );

                printLog(
                  "info",
                  `Forwarding sprite "${parts[2]}" - Filename "${parts[3]}" - To "${targetURL}"...`
                );

                data = await getSpriteFromURL(
                  targetURL,
                  30000 // 30 secs
                );

                if (item.storeCache === true) {
                  printLog(
                    "info",
                    `Caching sprite "${parts[2]}" - Filename "${parts[3]}"...`
                  );

                  cacheSpriteFile(item.source, parts[3], data).catch((error) =>
                    printLog(
                      "error",
                      `Failed to cache sprite "${parts[2]}" - Filename "${parts[3]}": ${error}`
                    )
                  );
                }
              } else {
                throw error;
              }
            }
          } catch (error) {
            printLog(
              "warn",
              `Failed to get sprite "${parts[2]}" - File "${parts[3]}": ${error}. Serving empty sprite...`
            );

            err = error;
          }

          break;
        }

        /* Get font */
        case "fonts:": {
          try {
            data = await getFonts(parts[2], parts[3]);

            const headers = detectFormatAndHeaders(data).headers;

            if (
              headers["content-type"] === "application/x-protobuf" &&
              headers["content-encoding"] !== undefined
            ) {
              data = await unzipAsync(data);
            }
          } catch (error) {
            printLog(
              "warn",
              `Failed to get font "${parts[2]}" - File "${parts[3]}": ${error}. Serving empty font...`
            );

            err = error;
          }

          break;
        }

        /* Get pmtiles tile */
        case "pmtiles:": {
          const z = Number(parts[3]);
          const x = Number(parts[4]);
          const y = Number(parts[5].slice(0, parts[5].indexOf(".")));
          const tileName = `${z}/${x}/${y}`;
          const item = config.datas[parts[2]];

          try {
            const dataTile = await getPMTilesTile(item.source, z, x, y);

            if (
              dataTile.headers["content-type"] === "application/x-protobuf" &&
              dataTile.headers["content-encoding"] !== undefined
            ) {
              data = await unzipAsync(dataTile.data);
            } else {
              data = dataTile.data;
            }
          } catch (error) {
            printLog(
              "warn",
              `Failed to get data "${parts[2]}" - Tile "${tileName}": ${error}. Serving empty tile...`
            );

            data = createFallbackTileData(item.tileJSON.format);
            err = error;
          }

          break;
        }

        /* Get mbtiles tile */
        case "mbtiles:": {
          const z = Number(parts[3]);
          const x = Number(parts[4]);
          const y = Number(parts[5].slice(0, parts[5].indexOf(".")));
          const tileName = `${z}/${x}/${y}`;
          const item = config.datas[parts[2]];

          try {
            let dataTile;

            try {
              dataTile = getMBTilesTile(item.source, z, x, y);
            } catch (error) {
              if (
                item.sourceURL !== undefined &&
                error.message === "Tile does not exist"
              ) {
                const tmpY = item.scheme === "tms" ? (1 << z) - 1 - y : y;

                const targetURL = item.sourceURL
                  .replace("{z}", `${z}`)
                  .replace("{x}", `${x}`)
                  .replace("{y}", `${tmpY}`);

                printLog(
                  "info",
                  `Forwarding data "${parts[2]}" - Tile "${tileName}" - To "${targetURL}"...`
                );

                dataTile = await getDataTileFromURL(
                  targetURL,
                  30000 // 30 secs
                );

                if (item.storeCache === true) {
                  printLog(
                    "info",
                    `Caching data "${parts[2]}" - Tile "${tileName}"...`
                  );

                  cacheMBtilesTileData(
                    item.source,
                    z,
                    x,
                    tmpY,
                    dataTile.data,
                    item.storeTransparent
                  ).catch((error) =>
                    printLog(
                      "error",
                      `Failed to cache data "${parts[2]}" - Tile "${tileName}": ${error}`
                    )
                  );
                }
              } else {
                throw error;
              }
            }

            if (
              dataTile.headers["content-type"] === "application/x-protobuf" &&
              dataTile.headers["content-encoding"] !== undefined
            ) {
              data = await unzipAsync(dataTile.data);
            } else {
              data = dataTile.data;
            }
          } catch (error) {
            printLog(
              "warn",
              `Failed to get data "${parts[2]}" - Tile "${tileName}": ${error}. Serving empty tile...`
            );

            data = createFallbackTileData(item.tileJSON.format);
            err = error;
          }

          break;
        }

        /* Get xyz tile */
        case "xyz:": {
          const z = Number(parts[3]);
          const x = Number(parts[4]);
          const y = Number(parts[5].slice(0, parts[5].indexOf(".")));
          const tileName = `${z}/${x}/${y}`;
          const item = config.datas[parts[2]];

          try {
            let dataTile;

            try {
              dataTile = await getXYZTile(
                item.source,
                z,
                x,
                y,
                item.tileJSON.format
              );
            } catch (error) {
              if (
                item.sourceURL !== undefined &&
                error.message === "Tile does not exist"
              ) {
                const tmpY = item.scheme === "tms" ? (1 << z) - 1 - y : y;

                const targetURL = item.sourceURL
                  .replace("{z}", `${z}`)
                  .replace("{x}", `${x}`)
                  .replace("{y}", `${tmpY}`);

                printLog(
                  "info",
                  `Forwarding data "${parts[2]}" - Tile "${tileName}" - To "${targetURL}"...`
                );

                dataTile = await getDataTileFromURL(
                  targetURL,
                  30000 // 30 secs
                );

                if (item.storeCache === true) {
                  printLog(
                    "info",
                    `Caching data "${parts[2]}" - Tile "${tileName}"...`
                  );

                  cacheXYZTileFile(
                    item.source,
                    item.md5Source,
                    z,
                    x,
                    tmpY,
                    item.tileJSON.format,
                    dataTile.data,
                    item.storeTransparent
                  ).catch((error) =>
                    printLog(
                      "error",
                      `Failed to cache data "${parts[2]}" - Tile "${tileName}": ${error}`
                    )
                  );
                }
              } else {
                throw error;
              }
            }

            if (
              dataTile.headers["content-type"] === "application/x-protobuf" &&
              dataTile.headers["content-encoding"] !== undefined
            ) {
              data = await unzipAsync(dataTile.data);
            } else {
              data = dataTile.data;
            }
          } catch (error) {
            printLog(
              "warn",
              `Failed to get data "${parts[2]}" - Tile "${tileName}": ${error}. Serving empty tile...`
            );

            data = createFallbackTileData(item.tileJSON.format);
            err = error;
          }

          break;
        }

        /* Get pg tile */
        case "pg:": {
          const z = Number(parts[3]);
          const x = Number(parts[4]);
          const y = Number(parts[5].slice(0, parts[5].indexOf(".")));
          const tileName = `${z}/${x}/${y}`;
          const item = config.datas[parts[2]];

          try {
            let dataTile;

            try {
              dataTile = await getPostgreSQLTile(item.source, z, x, y);
            } catch (error) {
              if (
                item.sourceURL !== undefined &&
                error.message === "Tile does not exist"
              ) {
                const tmpY = item.scheme === "tms" ? (1 << z) - 1 - y : y;

                const targetURL = item.sourceURL
                  .replace("{z}", `${z}`)
                  .replace("{x}", `${x}`)
                  .replace("{y}", `${tmpY}`);

                printLog(
                  "info",
                  `Forwarding data "${parts[2]}" - Tile "${tileName}" - To "${targetURL}"...`
                );

                dataTile = await getDataTileFromURL(
                  targetURL,
                  30000 // 30 secs
                );

                if (item.storeCache === true) {
                  printLog(
                    "info",
                    `Caching data "${parts[2]}" - Tile "${tileName}"...`
                  );

                  cachePostgreSQLTileData(
                    item.source,
                    z,
                    x,
                    tmpY,
                    dataTile.data,
                    item.storeTransparent
                  ).catch((error) =>
                    printLog(
                      "error",
                      `Failed to cache data "${parts[2]}" - Tile "${tileName}": ${error}`
                    )
                  );
                }
              } else {
                throw error;
              }
            }

            if (
              dataTile.headers["content-type"] === "application/x-protobuf" &&
              dataTile.headers["content-encoding"] !== undefined
            ) {
              data = await unzipAsync(dataTile.data);
            } else {
              data = dataTile.data;
            }
          } catch (error) {
            printLog(
              "warn",
              `Failed to get data "${parts[2]}" - Tile "${tileName}": ${error}. Serving empty tile...`
            );

            data = createFallbackTileData(item.tileJSON.format);
            err = error;
          }

          break;
        }

        /* Get data from remote */
        case "http:":
        case "https:": {
          try {
            printLog("info", `Getting data from "${url}"...`);

            const dataRemote = await getDataFromURL(
              url,
              30000, // 30 secs
              "arraybuffer"
            );

            /* Unzip pbf data */
            const headers = detectFormatAndHeaders(dataRemote.data).headers;

            if (
              headers["content-type"] === "application/x-protobuf" &&
              headers["content-encoding"] !== undefined
            ) {
              data = await unzipAsync(dataRemote.data);
            } else {
              data = dataRemote.data;
            }
          } catch (error) {
            printLog(
              "warn",
              `Failed to get data from "${url}": ${error}. Serving empty data...`
            );

            data = createFallbackTileData(url.slice(url.lastIndexOf(".") + 1));
            err = error;
          }

          break;
        }

        /* Default */
        default: {
          err = new Error(`Unknown scheme: "${parts[0]}`);

          printLog("warn", `Failed to render: ${err}. Skipping...`);

          break;
        }
      }

      callback(err, {
        data: data,
      });
    },
  });

  renderer.load(styleJSON);

  return renderer;
}

/**
 * Render image tile
 * @param {number} tileScale Tile scale
 * @param {256|512} tileSize Tile size
 * @param {object} styleJSON StyleJSON
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @param {"jpeg"|"jpg"|"png"|"webp"|"gif"} format Tile format
 * @returns {Promise<Buffer>}
 */
export async function renderImageTile(
  tileScale,
  tileSize,
  styleJSON,
  z,
  x,
  y,
  format
) {
  const isNeedHack = z === 0 && tileSize === 256;
  const hackTileSize = isNeedHack === false ? tileSize : tileSize * 2;

  const data = await new Promise((resolve, reject) => {
    const renderer = createTileRenderer(tileScale, styleJSON);

    renderer.render(
      {
        zoom: z !== 0 && tileSize === 256 ? z - 1 : z,
        center: getLonLatFromXYZ(x, y, z, "center", "xyz"),
        width: hackTileSize,
        height: hackTileSize,
      },
      (error, data) => {
        if (renderer !== undefined) {
          renderer.release();
        }

        if (error) {
          return reject(error);
        }

        resolve(data);
      }
    );
  });

  return await renderImageTileData(
    data,
    hackTileSize * tileScale,
    isNeedHack === false ? undefined : (hackTileSize / 2) * tileScale,
    format
  );
}

/**
 * Render MBTiles tiles
 * @param {string} id Style ID
 * @param {object} metadata Metadata object
 * @param {number} tileScale Tile scale
 * @param {256|512} tileSize Tile size
 * @param {{ zoom: number, bbox: [number, number, number, number]}[]} coverages Specific coverages
 * @param {number} maxRendererPoolSize Max renderer pool size
 * @param {number} concurrency Concurrency to download
 * @param {boolean} storeTransparent Is store transparent tile?
 * @param {boolean} createOverview Is create overview?
 * @param {string|number|boolean} refreshBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be refreshed/Compare MD5
 * @returns {Promise<void>}
 */
export async function renderMBTilesTiles(
  id,
  metadata,
  tileScale,
  tileSize,
  coverages,
  maxRendererPoolSize,
  concurrency,
  storeTransparent,
  createOverview,
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

    let log = `Rendering ${total} tiles of style "${id}" to mbtiles with:`;
    log += `\n\tID: ${metadata.id}`;
    log += `\n\tStore transparent: ${storeTransparent}`;
    log += `\n\tMax renderer pool size: ${maxRendererPoolSize}`;
    log += `\n\tConcurrency: ${concurrency}`;
    log += `\n\tTile size: ${tileSize}`;
    log += `\n\tTile scale: ${tileScale}`;
    log += `\n\tCreate overview: ${createOverview}`;
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
    const filePath = `${process.env.DATA_DIR}/exports/styles/mbtiles/${metadata.id}/${metadata.id}.mbtiles`;

    source = await openMBTilesDB(
      filePath,
      true,
      30000 // 30 secs
    );

    /* Get tile extra info */
    let tileExtraInfo;

    if (refreshTimestampType !== "undefined") {
      try {
        printLog("info", `Get tile extra info from "${filePath}"...`);

        tileExtraInfo = getMBTilesTileExtraInfoFromCoverages(
          source,
          targetCoverages,
          refreshTimestampType === "number"
        );
      } catch (error) {
        printLog(
          "error",
          `Failed to get tile extra info from "${filePath}": ${error}`
        );

        tileExtraInfo = {};
      }
    }

    /* Update metadata */
    printLog("info", "Updating metadata...");

    await updateMBTilesMetadata(
      source,
      metadata,
      30000 // 30 secs
    );

    /* Render tiles */
    const tasks = {
      mutex: new Mutex(),
      activeTasks: 0,
      completeTasks: 0,
    };

    /* Create renderer pool */
    const rendered = config.styles[id].rendered;
    const rendererPool = createPool(
      {
        create: () => createTileRenderer(tileScale, rendered.styleJSON),
        destroy: (renderer) => renderer.release(),
      },
      {
        min: 1,
        max: maxRendererPoolSize,
      }
    );

    async function renderMBTilesTileData(z, x, y, tasks) {
      const tileName = `${z}/${x}/${y}`;

      if (
        refreshTimestampType !== "number" ||
        tileExtraInfo[tileName] === undefined ||
        tileExtraInfo[tileName] < refreshTimestamp
      ) {
        const completeTasks = tasks.completeTasks;

        printLog(
          "info",
          `Rendering style "${id}" - Tile "${tileName}" - ${completeTasks}/${total}...`
        );

        try {
          // Rendered data
          const isNeedHack = z === 0 && tileSize === 256;
          const hackTileSize = isNeedHack === false ? tileSize : tileSize * 2;

          const renderer = await rendererPool.acquire();

          const dataRaw = await new Promise((resolve, reject) => {
            renderer.render(
              {
                zoom: z !== 0 && tileSize === 256 ? z - 1 : z,
                center: getLonLatFromXYZ(x, y, z, "center", "xyz"),
                width: hackTileSize,
                height: hackTileSize,
              },
              (error, dataRaw) => {
                if (renderer !== undefined) {
                  rendererPool.release(renderer);
                }

                if (error) {
                  return reject(error);
                }

                resolve(dataRaw);
              }
            );
          });

          const data = await renderImageTileData(
            dataRaw,
            hackTileSize * tileScale,
            isNeedHack === false ? undefined : (hackTileSize / 2) * tileScale,
            metadata.format
          );

          if (
            refreshTimestampType === "boolean" &&
            tileExtraInfo[tileName] === calculateMD5(data)
          ) {
            return;
          }

          // Store data
          await cacheMBtilesTileData(source, z, x, y, data, storeTransparent);
        } catch (error) {
          printLog(
            "error",
            `Failed to render style "${id}" - Tile "${tileName}" - ${completeTasks}/${total}: ${error}`
          );
        }
      }
    }

    printLog("info", "Rendering datas...");

    for (const { z, x, y } of tileBounds) {
      for (let xCount = x[0]; xCount <= x[1]; xCount++) {
        for (let yCount = y[0]; yCount <= y[1]; yCount++) {
          if (rendered.export === true) {
            return;
          }

          /* Wait slot for a task */
          while (tasks.activeTasks >= concurrency) {
            await delay(50);
          }

          await tasks.mutex.runExclusive(() => {
            tasks.activeTasks++;
            tasks.completeTasks++;
          });

          /* Run a task */
          renderMBTilesTileData(z, xCount, yCount, tasks).finally(() =>
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

    /* Destroy renderer pool */
    await rendererPool.drain();
    await rendererPool.clear();

    /* Create overviews */
    if (createOverview === true) {
      printLog("info", "Creating overviews...");

      const command = `gdaladdo -r lanczos -oo ZLEVEL=9 ${filePath} 2 4 8 16 32 64 128 256 512 1024 2048 4096 8192 16384 32768 65536 131072 262144 524288 1048576 2097152 4194304`;

      printLog("info", `Gdal command: ${command}`);

      const commandOutput = await runCommand(command);

      printLog("info", `Gdal command output: ${commandOutput}`);

      printLog("info", "Calculating tile extra info...");

      await calculateMBTilesTileExtraInfo(source);
    }

    printLog(
      "info",
      `Completed render ${total} tiles of style "${id}" to mbtiles after ${
        (Date.now() - startTime) / 1000
      }s!`
    );
  } catch (error) {
    printLog(
      "error",
      `Failed to render style "${id}" to mbtiles after ${
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
 * Render XYZ tiles
 * @param {string} id Style ID
 * @param {object} metadata Metadata object
 * @param {number} tileScale Tile scale
 * @param {256|512} tileSize Tile size
 * @param {{ zoom: number, bbox: [number, number, number, number]}[]} coverages Specific coverages
 * @param {number} maxRendererPoolSize Max renderer pool size
 * @param {number} concurrency Concurrency to download
 * @param {boolean} storeTransparent Is store transparent tile?
 * @param {boolean} createOverview Is create overview?
 * @param {string|number|boolean} refreshBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be refreshed/Compare MD5
 * @returns {Promise<void>}
 */
export async function renderXYZTiles(
  id,
  metadata,
  tileScale,
  tileSize,
  coverages,
  maxRendererPoolSize,
  concurrency,
  storeTransparent,
  createOverview,
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

    let log = `Rendering ${total} tiles of style "${id}" to xyz with:`;
    log += `\n\tID: ${metadata.id}`;
    log += `\n\tStore transparent: ${storeTransparent}`;
    log += `\n\tMax renderer pool size: ${maxRendererPoolSize}`;
    log += `\n\tConcurrency: ${concurrency}`;
    log += `\n\tTile size: ${tileSize}`;
    log += `\n\tTile scale: ${tileScale}`;
    log += `\n\tCreate overview: ${createOverview}`;
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
    const sourcePath = `${process.env.DATA_DIR}/exports/styles/xyzs/${metadata.id}`;
    const filePath = `${sourcePath}/${metadata.id}.sqlite`;

    const source = await openXYZMD5DB(
      filePath,
      true,
      30000 // 30 secs
    );

    /* Get tile extra info */
    let tileExtraInfo;

    if (refreshTimestampType !== "undefined") {
      try {
        printLog("info", `Get tile extra info from "${filePath}"...`);

        tileExtraInfo = getXYZTileExtraInfoFromCoverages(
          source,
          targetCoverages,
          refreshTimestampType === "number"
        );
      } catch (error) {
        printLog(
          "error",
          `Failed to get tile extra info from "${filePath}": ${error}`
        );

        tileExtraInfo = {};
      }
    }

    /* Update metadata */
    printLog("info", "Updating metadata...");

    await updateXYZMetadata(
      source,
      metadata,
      30000 // 30 secs
    );

    /* Render tile files */
    const tasks = {
      mutex: new Mutex(),
      activeTasks: 0,
      completeTasks: 0,
    };

    /* Create renderer pool */
    const rendered = config.styles[id].rendered;
    const rendererPool = createPool(
      {
        create: () => createTileRenderer(tileScale, rendered.styleJSON),
        destroy: (renderer) => renderer.release(),
      },
      {
        min: 1,
        max: maxRendererPoolSize,
      }
    );

    async function renderXYZTileData(z, x, y, tasks) {
      const tileName = `${z}/${x}/${y}`;

      if (
        refreshTimestampType !== "number" ||
        tileExtraInfo[tileName] === undefined ||
        tileExtraInfo[tileName] < refreshTimestamp
      ) {
        const completeTasks = tasks.completeTasks;

        printLog(
          "info",
          `Rendering style "${id}" - Tile "${tileName}" - ${completeTasks}/${total}...`
        );

        try {
          // Rendered data
          const isNeedHack = z === 0 && tileSize === 256;
          const hackTileSize = isNeedHack === false ? tileSize : tileSize * 2;

          const renderer = await rendererPool.acquire();

          const dataRaw = await new Promise((resolve, reject) => {
            renderer.render(
              {
                zoom: z !== 0 && tileSize === 256 ? z - 1 : z,
                center: getLonLatFromXYZ(x, y, z, "center", "xyz"),
                width: hackTileSize,
                height: hackTileSize,
              },
              (error, dataRaw) => {
                if (renderer !== undefined) {
                  rendererPool.release(renderer);
                }

                if (error) {
                  return reject(error);
                }

                resolve(dataRaw);
              }
            );
          });

          const data = await renderImageTileData(
            dataRaw,
            hackTileSize * tileScale,
            isNeedHack === false ? undefined : (hackTileSize / 2) * tileScale,
            metadata.format
          );

          if (
            refreshTimestampType === "boolean" &&
            tileExtraInfo[tileName] === calculateMD5(data)
          ) {
            return;
          }

          // Store data
          await cacheXYZTileFile(
            sourcePath,
            source,
            z,
            x,
            y,
            metadata.format,
            data,
            storeTransparent
          );
        } catch (error) {
          printLog(
            "error",
            `Failed to render style "${id}" - Tile "${tileName}" - ${completeTasks}/${total}: ${error}`
          );
        }
      }
    }

    printLog("info", "Rendering datas...");

    for (const { z, x, y } of tileBounds) {
      for (let xCount = x[0]; xCount <= x[1]; xCount++) {
        for (let yCount = y[0]; yCount <= y[1]; yCount++) {
          if (rendered.export === true) {
            return;
          }

          /* Wait slot for a task */
          while (tasks.activeTasks >= concurrency) {
            await delay(50);
          }

          await tasks.mutex.runExclusive(() => {
            tasks.activeTasks++;
            tasks.completeTasks++;
          });

          /* Run a task */
          renderXYZTileData(z, xCount, yCount, tasks).finally(() =>
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

    /* Destroy renderer pool */
    await rendererPool.drain();
    await rendererPool.clear();

    /* Remove parent folders if empty */
    await removeEmptyFolders(sourcePath, /^.*\.(gif|png|jpg|jpeg|webp)$/);

    /* Create overviews */
    if (createOverview === true) {
      // Do nothing
    }

    printLog(
      "info",
      `Completed render ${total} tiles of style "${id}" to xyz after ${
        (Date.now() - startTime) / 1000
      }s!`
    );
  } catch (error) {
    printLog(
      "error",
      `Failed to render style "${id}" to xyz after ${
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
 * Render PostgreSQL tiles
 * @param {string} id Style ID
 * @param {object} metadata Metadata object
 * @param {number} tileScale Tile scale
 * @param {256|512} tileSize Tile size
 * @param {{ zoom: number, bbox: [number, number, number, number]}[]} coverages Specific coverages
 * @param {number} maxRendererPoolSize Max renderer pool size
 * @param {number} concurrency Concurrency to download
 * @param {boolean} storeTransparent Is store transparent tile?
 * @param {boolean} createOverview Is create overview?
 * @param {string|number|boolean} refreshBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be refreshed/Compare MD5
 * @returns {Promise<void>}
 */
export async function renderPostgreSQLTiles(
  id,
  metadata,
  tileScale,
  tileSize,
  coverages,
  maxRendererPoolSize,
  concurrency,
  storeTransparent,
  createOverview,
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

    let log = `Rendering ${total} tiles of style "${id}" to postgresql with:`;
    log += `\n\tID: ${metadata.id}`;
    log += `\n\tStore transparent: ${storeTransparent}`;
    log += `\n\tMax renderer pool size: ${maxRendererPoolSize}`;
    log += `\n\tConcurrency: ${concurrency}`;
    log += `\n\tTile size: ${tileSize}`;
    log += `\n\tTile scale: ${tileScale}`;
    log += `\n\tCreate overview: ${createOverview}`;
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
    const filePath = `${process.env.POSTGRESQL_BASE_URI}/${metadata.id}`;

    source = await openPostgreSQLDB(filePath, true);

    /* Get tile extra info */
    let tileExtraInfo;

    if (refreshTimestampType !== "undefined") {
      try {
        printLog("info", `Get tile extra info from "${filePath}"...`);

        tileExtraInfo = await getPostgreSQLTileExtraInfoFromCoverages(
          source,
          targetCoverages,
          refreshTimestampType === "number"
        );
      } catch (error) {
        printLog(
          "error",
          `Failed to get tile extra info from "${filePath}": ${error}`
        );

        tileExtraInfo = {};
      }
    }

    /* Update metadata */
    printLog("info", "Updating metadata...");

    await updatePostgreSQLMetadata(
      source,
      metadata,
      30000 // 30 secs
    );

    /* Render tiles */
    const tasks = {
      mutex: new Mutex(),
      activeTasks: 0,
      completeTasks: 0,
    };

    /* Create renderer pool */
    const rendered = config.styles[id].rendered;
    const rendererPool = createPool(
      {
        create: () => createTileRenderer(tileScale, rendered.styleJSON),
        destroy: (renderer) => renderer.release(),
      },
      {
        min: 1,
        max: maxRendererPoolSize,
      }
    );

    async function renderPostgreSQLTileData(z, x, y, tasks) {
      const tileName = `${z}/${x}/${y}`;

      if (
        refreshTimestampType !== "number" ||
        tileExtraInfo[tileName] === undefined ||
        tileExtraInfo[tileName] < refreshTimestamp
      ) {
        const completeTasks = tasks.completeTasks;

        printLog(
          "info",
          `Rendering style "${id}" - Tile "${tileName}" - ${completeTasks}/${total}...`
        );

        try {
          // Rendered data
          const isNeedHack = z === 0 && tileSize === 256;
          const hackTileSize = isNeedHack === false ? tileSize : tileSize * 2;

          const renderer = await rendererPool.acquire();

          const dataRaw = await new Promise((resolve, reject) => {
            renderer.render(
              {
                zoom: z !== 0 && tileSize === 256 ? z - 1 : z,
                center: getLonLatFromXYZ(x, y, z, "center", "xyz"),
                width: hackTileSize,
                height: hackTileSize,
              },
              (error, dataRaw) => {
                if (renderer !== undefined) {
                  rendererPool.release(renderer);
                }

                if (error) {
                  return reject(error);
                }

                resolve(dataRaw);
              }
            );
          });

          const data = await renderImageTileData(
            dataRaw,
            hackTileSize * tileScale,
            isNeedHack === false ? undefined : (hackTileSize / 2) * tileScale,
            metadata.format
          );

          if (
            refreshTimestampType === "boolean" &&
            tileExtraInfo[tileName] === calculateMD5(data)
          ) {
            return;
          }

          // Store data
          await cachePostgreSQLTileData(
            source,
            z,
            x,
            y,
            data,
            storeTransparent
          );
        } catch (error) {
          printLog(
            "error",
            `Failed to render style "${id}" - Tile "${tileName}" - ${completeTasks}/${total}: ${error}`
          );
        }
      }
    }

    printLog("info", "Rendering datas...");

    for (const { z, x, y } of tileBounds) {
      for (let xCount = x[0]; xCount <= x[1]; xCount++) {
        for (let yCount = y[0]; yCount <= y[1]; yCount++) {
          if (rendered.export === true) {
            return;
          }

          /* Wait slot for a task */
          while (tasks.activeTasks >= concurrency) {
            await delay(50);
          }

          await tasks.mutex.runExclusive(() => {
            tasks.activeTasks++;
            tasks.completeTasks++;
          });

          /* Run a task */
          renderPostgreSQLTileData(z, xCount, yCount, tasks).finally(() =>
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

    /* Destroy renderer pool */
    await rendererPool.drain();
    await rendererPool.clear();

    /* Create overviews */
    if (createOverview === true) {
      // Do nothing
    }

    printLog(
      "info",
      `Completed render ${total} tiles of style "${id}" to postgresql after ${
        (Date.now() - startTime) / 1000
      }s!`
    );
  } catch (error) {
    printLog(
      "error",
      `Failed to render style "${id}" to postgresql after ${
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
 * Export MBTiles tiles
 * @param {string} id Style ID
 * @param {object} metadata Metadata object
 * @param {{ zoom: number, bbox: [number, number, number, number]}[]} coverages Specific coverages
 * @param {number} concurrency Concurrency to download
 * @param {boolean} storeTransparent Is store transparent tile?
 * @param {string|number|boolean} refreshBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be refreshed/Compare MD5
 * @returns {Promise<void>}
 */
export async function exportMBTilesTiles(
  id,
  metadata,
  coverages,
  concurrency,
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

    let log = `Exporting ${total} tiles of data "${id}" to mbtiles with:`;
    log += `\n\tID: ${metadata.id}`;
    log += `\n\tStore transparent: ${storeTransparent}`;
    log += `\n\tConcurrency: ${concurrency}`;
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
    const item = config.datas[id];
    const filePath = `${process.env.DATA_DIR}/exports/datas/mbtiles/${metadata.id}/${metadata.id}.mbtiles`;

    source = await openMBTilesDB(
      filePath,
      true,
      30000 // 30 secs
    );

    /* Get tile extra info */
    let targetTileExtraInfo;
    let tileExtraInfo;

    if (refreshTimestampType === "boolean") {
      try {
        printLog(
          "info",
          `Get target tile extra info from "${item.path}" and tile extra info from "${filePath}"...`
        );

        targetTileExtraInfo = getMBTilesTileExtraInfoFromCoverages(
          item.source,
          targetCoverages,
          false
        );

        tileExtraInfo = getMBTilesTileExtraInfoFromCoverages(
          source,
          targetCoverages,
          false
        );
      } catch (error) {
        printLog(
          "error",
          `Failed to get target tile extra info from "${item.path}" and tile extra info from "${filePath}": ${error}`
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

    /* Update metadata */
    printLog("info", "Updating metadata...");

    await updateMBTilesMetadata(
      source,
      metadata,
      30000 // 30 secs
    );

    /* Export tiles */
    const tasks = {
      mutex: new Mutex(),
      activeTasks: 0,
      completeTasks: 0,
    };

    async function exportMBTilesTileData(z, x, y, tasks) {
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

        printLog(
          "info",
          `Exporting data "${id}" - Tile "${tileName}" - ${completeTasks}/${total}...`
        );

        try {
          // Export data
          let dataTile;

          try {
            dataTile = getMBTilesTile(item.source, z, x, y);
          } catch (error) {
            if (
              item.sourceURL !== undefined &&
              error.message === "Tile does not exist"
            ) {
              const tmpY = item.scheme === "tms" ? (1 << z) - 1 - y : y;

              const targetURL = item.sourceURL
                .replace("{z}", `${z}`)
                .replace("{x}", `${x}`)
                .replace("{y}", `${tmpY}`);

              printLog(
                "info",
                `Forwarding data "${id}" - Tile "${tileName}" - To "${targetURL}"...`
              );

              /* Get data */
              dataTile = await getDataTileFromURL(
                targetURL,
                30000 // 30 secs
              );

              /* Cache */
              if (item.storeCache === true) {
                printLog(
                  "info",
                  `Caching data "${id}" - Tile "${tileName}"...`
                );

                cacheMBtilesTileData(
                  item.source,
                  z,
                  x,
                  tmpY,
                  dataTile.data,
                  item.storeTransparent
                ).catch((error) =>
                  printLog(
                    "error",
                    `Failed to cache data "${id}" - Tile "${tileName}": ${error}`
                  )
                );
              }
            } else {
              throw error;
            }
          }

          // Store data
          await cacheMBtilesTileData(
            source,
            z,
            x,
            y,
            dataTile.data,
            storeTransparent
          );
        } catch (error) {
          printLog(
            "error",
            `Failed to export data "${id}" - Tile "${tileName}" - ${completeTasks}/${total}: ${error}`
          );
        }
      }
    }

    printLog("info", "Exporting datas...");

    for (const { z, x, y } of tileBounds) {
      for (let xCount = x[0]; xCount <= x[1]; xCount++) {
        for (let yCount = y[0]; yCount <= y[1]; yCount++) {
          if (item.export === true) {
            return;
          }

          /* Wait slot for a task */
          while (tasks.activeTasks >= concurrency) {
            await delay(50);
          }

          await tasks.mutex.runExclusive(() => {
            tasks.activeTasks++;
            tasks.completeTasks++;
          });

          /* Run a task */
          exportMBTilesTileData(z, xCount, yCount, tasks).finally(() =>
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
      `Completed export ${total} tiles of datas "${id}" to mbtiles after ${
        (Date.now() - startTime) / 1000
      }s!`
    );
  } catch (error) {
    printLog(
      "error",
      `Failed to export data "${id}" to mbtiles after ${
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
 * Export XYZ tiles
 * @param {string} id Style ID
 * @param {object} metadata Metadata object
 * @param {{ zoom: number, bbox: [number, number, number, number]}[]} coverages Specific coverages
 * @param {number} concurrency Concurrency to download
 * @param {boolean} storeTransparent Is store transparent tile?
 * @param {string|number|boolean} refreshBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be refreshed/Compare MD5
 * @returns {Promise<void>}
 */
export async function exportXYZTiles(
  id,
  metadata,
  coverages,
  concurrency,
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

    let log = `Exporting ${total} tiles of data "${id}" to xyz with:`;
    log += `\n\tID: ${metadata.id}`;
    log += `\n\tStore transparent: ${storeTransparent}`;
    log += `\n\tConcurrency: ${concurrency}`;
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
    const item = config.datas[id];
    const sourcePath = `${process.env.DATA_DIR}/exports/datas/xyzs/${metadata.id}`;
    const filePath = `${sourcePath}/${metadata.id}.sqlite`;

    const source = await openXYZMD5DB(
      filePath,
      true,
      30000 // 30 secs
    );

    /* Get tile extra info */
    let targetTileExtraInfo;
    let tileExtraInfo;

    if (refreshTimestampType === "boolean") {
      try {
        printLog(
          "info",
          `Get target tile extra info from "${item.path}" and tile extra info from "${filePath}"...`
        );

        targetTileExtraInfo = getXYZTileExtraInfoFromCoverages(
          item.source,
          targetCoverages,
          false
        );

        tileExtraInfo = getXYZTileExtraInfoFromCoverages(
          source,
          targetCoverages,
          false
        );
      } catch (error) {
        printLog(
          "error",
          `Failed to get target tile extra info from "${item.path}" and tile extra info from "${filePath}": ${error}`
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

    /* Update metadata */
    printLog("info", "Updating metadata...");

    await updateXYZMetadata(
      source,
      metadata,
      30000 // 30 secs
    );

    /* Export tile files */
    const tasks = {
      mutex: new Mutex(),
      activeTasks: 0,
      completeTasks: 0,
    };

    async function exportXYZTileData(z, x, y, tasks) {
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

        printLog(
          "info",
          `Exporting data "${id}" - Tile "${tileName}" - ${completeTasks}/${total}...`
        );

        try {
          // Export data
          let dataTile;

          try {
            dataTile = await getXYZTile(
              item.source,
              z,
              x,
              y,
              item.tileJSON.format
            );
          } catch (error) {
            if (
              item.sourceURL !== undefined &&
              error.message === "Tile does not exist"
            ) {
              const tmpY = item.scheme === "tms" ? (1 << z) - 1 - y : y;

              const targetURL = item.sourceURL
                .replace("{z}", `${z}`)
                .replace("{x}", `${x}`)
                .replace("{y}", `${tmpY}`);

              printLog(
                "info",
                `Forwarding data "${id}" - Tile "${tileName}" - To "${targetURL}"...`
              );

              /* Get data */
              dataTile = await getDataTileFromURL(
                targetURL,
                30000 // 30 secs
              );

              /* Cache */
              if (item.storeCache === true) {
                printLog(
                  "info",
                  `Caching data "${id}" - Tile "${tileName}"...`
                );

                cacheXYZTileFile(
                  item.source,
                  item.md5Source,
                  z,
                  x,
                  tmpY,
                  item.tileJSON.format,
                  dataTile.data,
                  item.storeTransparent
                ).catch((error) =>
                  printLog(
                    "error",
                    `Failed to cache data "${id}" - Tile "${tileName}": ${error}`
                  )
                );
              }
            } else {
              throw error;
            }
          }

          // Store data
          await cacheXYZTileFile(
            sourcePath,
            source,
            z,
            x,
            y,
            metadata.format,
            dataTile.data,
            storeTransparent
          );
        } catch (error) {
          printLog(
            "error",
            `Failed to export data "${id}" - Tile "${tileName}" - ${completeTasks}/${total}: ${error}`
          );
        }
      }
    }

    printLog("info", "Exporting datas...");

    for (const { z, x, y } of tileBounds) {
      for (let xCount = x[0]; xCount <= x[1]; xCount++) {
        for (let yCount = y[0]; yCount <= y[1]; yCount++) {
          if (item.export === true) {
            return;
          }

          /* Wait slot for a task */
          while (tasks.activeTasks >= concurrency) {
            await delay(50);
          }

          await tasks.mutex.runExclusive(() => {
            tasks.activeTasks++;
            tasks.completeTasks++;
          });

          /* Run a task */
          exportXYZTileData(z, xCount, yCount, tasks).finally(() =>
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
    await removeEmptyFolders(sourcePath, /^.*\.(gif|png|jpg|jpeg|webp)$/);

    printLog(
      "info",
      `Completed export ${total} tiles of data "${id}" to xyz after ${
        (Date.now() - startTime) / 1000
      }s!`
    );
  } catch (error) {
    printLog(
      "error",
      `Failed to export data "${id}" to xyz after ${
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
 * Export PostgreSQL tiles
 * @param {string} id Style ID
 * @param {object} metadata Metadata object
 * @param {{ zoom: number, bbox: [number, number, number, number]}[]} coverages Specific coverages
 * @param {number} concurrency Concurrency to download
 * @param {boolean} storeTransparent Is store transparent tile?
 * @param {string|number|boolean} refreshBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be refreshed/Compare MD5
 * @returns {Promise<void>}
 */
export async function exportPostgreSQLTiles(
  id,
  metadata,
  coverages,
  concurrency,
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

    let log = `Exporting ${total} tiles of data "${id}" to postgresql with:`;
    log += `\n\tID: ${metadata.id}`;
    log += `\n\tStore transparent: ${storeTransparent}`;
    log += `\n\tConcurrency: ${concurrency}`;
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
    const item = config.datas[id];
    const filePath = `${process.env.POSTGRESQL_BASE_URI}/${metadata.id}`;

    source = await openPostgreSQLDB(filePath, true);

    /* Get tile extra info */
    let targetTileExtraInfo;
    let tileExtraInfo;

    if (refreshTimestampType === "boolean") {
      try {
        printLog(
          "info",
          `Get target tile extra info from "${item.path}" and tile extra info from "${filePath}"...`
        );

        targetTileExtraInfo = getPostgreSQLTileExtraInfoFromCoverages(
          item.source,
          targetCoverages,
          false
        );

        tileExtraInfo = getPostgreSQLTileExtraInfoFromCoverages(
          source,
          targetCoverages,
          false
        );
      } catch (error) {
        printLog(
          "error",
          `Failed to get target tile extra info from "${item.path}" and tile extra info from "${filePath}": ${error}`
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

    /* Update metadata */
    printLog("info", "Updating metadata...");

    await updatePostgreSQLMetadata(
      source,
      metadata,
      30000 // 30 secs
    );

    /* Export tiles */
    const tasks = {
      mutex: new Mutex(),
      activeTasks: 0,
      completeTasks: 0,
    };

    async function exportPostgreSQLTileData(z, x, y, tasks) {
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

        printLog(
          "info",
          `Exporting data "${id}" - Tile "${tileName}" - ${completeTasks}/${total}...`
        );

        try {
          // Export data
          let dataTile;

          try {
            dataTile = await getPostgreSQLTile(item.source, z, x, y);
          } catch (error) {
            if (
              item.sourceURL !== undefined &&
              error.message === "Tile does not exist"
            ) {
              const tmpY = item.scheme === "tms" ? (1 << z) - 1 - y : y;

              const targetURL = item.sourceURL
                .replace("{z}", `${z}`)
                .replace("{x}", `${x}`)
                .replace("{y}", `${tmpY}`);

              printLog(
                "info",
                `Forwarding data "${id}" - Tile "${tileName}" - To "${targetURL}"...`
              );

              /* Get data */
              dataTile = await getDataTileFromURL(
                targetURL,
                30000 // 30 secs
              );

              /* Cache */
              if (item.storeCache === true) {
                printLog(
                  "info",
                  `Caching data "${id}" - Tile "${tileName}"...`
                );

                cachePostgreSQLTileData(
                  item.source,
                  z,
                  x,
                  tmpY,
                  dataTile.data,
                  item.storeTransparent
                ).catch((error) =>
                  printLog(
                    "error",
                    `Failed to cache data "${id}" - Tile "${tileName}": ${error}`
                  )
                );
              }
            } else {
              throw error;
            }
          }

          // Store data
          await cachePostgreSQLTileData(
            source,
            z,
            x,
            y,
            dataTile.data,
            storeTransparent
          );
        } catch (error) {
          printLog(
            "error",
            `Failed to export data "${id}" - Tile "${tileName}" - ${completeTasks}/${total}: ${error}`
          );
        }
      }
    }

    printLog("info", "Exporting datas...");

    for (const { z, x, y } of tileBounds) {
      for (let xCount = x[0]; xCount <= x[1]; xCount++) {
        for (let yCount = y[0]; yCount <= y[1]; yCount++) {
          if (item.export === true) {
            return;
          }

          /* Wait slot for a task */
          while (tasks.activeTasks >= concurrency) {
            await delay(50);
          }

          await tasks.mutex.runExclusive(() => {
            tasks.activeTasks++;
            tasks.completeTasks++;
          });

          /* Run a task */
          exportPostgreSQLTileData(z, xCount, yCount, tasks).finally(() =>
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
      `Completed export ${total} tiles of data "${id}" to postgresql after ${
        (Date.now() - startTime) / 1000
      }s!`
    );
  } catch (error) {
    printLog(
      "error",
      `Failed to export data "${id}" to postgresql after ${
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
