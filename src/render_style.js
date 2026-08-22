"use strict";

import mlgl from "@maplibre/maplibre-gl-native";
import { config } from "./configs/index.js";
import { createPool } from "generic-pool";
import { nanoid } from "nanoid";
import path from "node:path";
import {
  DEFAULT_RENDERER_IDLE_TIMEOUT,
  DEFAULT_STORE_TRANSPARENT,
  DEFAULT_TILE_BATCH_SIZE,
  DEFAULT_QUERY_TIMEOUT,
  DEFAULT_CONCURRENCY,
} from "./defaults/index.js";
import {
  getAndCachePostgreSQLTileData,
  getPostgreSQLTileExtraInfo,
  getAndCacheMBTilesTileData,
  MBTILES_INSERT_TILE_QUERY,
  updatePostgreSQLMetadata,
  getMBTilesTileExtraInfo,
  storePostgreSQLTileData,
  getAndCacheDataGeoJSON,
  getAndCacheXYZTileData,
  getAndCacheDataSprite,
  updateMBTilesMetadata,
  getAndCacheDataFonts,
  getRenderedStyleJSON,
  storeMBtilesTileData,
  XYZ_INSERT_MD5_QUERY,
  getXYZTileExtraInfo,
  closePostgreSQLDB,
  updateXYZMetadata,
  storeXYZTileFile,
  openPostgreSQLDB,
  getFallbackFont,
  closeMBTilesDB,
  getPMTilesTile,
  openMBTilesDB,
  closeXYZMD5DB,
  openXYZMD5DB,
} from "./resources/index.js";
import {
  detectFormatAndHeaders,
  getTileBoundsBatches,
  FALLBACK_TILE_DATA,
  lonLat4326ToXY3857,
  xy3857ToLonLat4326,
  createImageOutput,
  getLonLatFromXYZ,
  BACKGROUND_COLOR,
  runAllWithLimit,
  calculateSizes,
  getDataFromURL,
  base64ToBuffer,
  getTileBounds,
  calculateMD5,
  unzipAsync,
  printLog,
} from "./utils/index.js";

mlgl.on("message", (msg) => {
  switch (msg.severity) {
    case "ERROR": {
      printLog("error", `Render StyleJSON: ${msg.text}`);

      break;
    }

    case "WARNING": {
      printLog("warn", `Render StyleJSON: ${msg.text}`);

      break;
    }

    default: {
      printLog("info", `Render StyleJSON: ${msg.text}`);

      break;
    }
  }
});

const tileRendererPools = new Map();
const staticRendererPools = new Map();

/**
 * Create render
 * @param {{ mode: "tile"|"static", styleJSON: object, ratio: number }} option Option object
 * @returns {{ mode: "tile"|"static", styleJSON: object, ratio: number, render: function, load: function, release: function }} Renderer
 */
function createRenderer(option) {
  const renderer = new mlgl.Map({
    mode: option.mode,
    ratio: option.ratio ?? 1,
    request: async (req, callback) => {
      const scheme = req.url.slice(0, req.url.indexOf(":"));

      let data = null;
      let err = null;

      // Handle get resource
      switch (scheme) {
        /* Get sprite */
        case "sprites": {
          const parts = decodeURIComponent(req.url).split("/");

          try {
            data = await getAndCacheDataSprite(parts[2], parts[3]);

            /* Unzip data */
            const headers = detectFormatAndHeaders(data).headers;

            if (headers["content-encoding"]) {
              data = await unzipAsync(data);
            }
          } catch (error) {
            printLog(
              "error",
              `Failed to get sprite "${parts[2]}" - File "${parts[3]}": ${error}`,
            );

            err = error;
          }

          break;
        }

        /* Get font */
        case "fonts": {
          const parts = decodeURIComponent(req.url).split("/");

          try {
            // Always uncompress
            data = await getAndCacheDataFonts(parts[2], parts[3]);
          } catch (error) {
            printLog(
              "error",
              `Failed to get font "${parts[2]}" - File "${parts[3]}": ${error}`,
            );

            err = error;
          }

          break;
        }

        /* Get geojson */
        case "geojson": {
          const parts = decodeURIComponent(req.url).split("/");

          try {
            data = await getAndCacheDataGeoJSON(parts[2], parts[3]);

            /* Unzip data */
            const headers = detectFormatAndHeaders(data).headers;

            if (headers["content-encoding"]) {
              data = await unzipAsync(data);
            }
          } catch (error) {
            printLog(
              "error",
              `Failed to get GeoJSON group "${parts[2]}" - Layer "${parts[3]}": ${error}.`,
            );

            err = error;
          }

          break;
        }

        /* Get local tile data */
        case "pmtiles":
        case "mbtiles":
        case "xyz":
        case "pg": {
          const parts = decodeURIComponent(req.url).split("/");
          const id = parts[2];
          const z = +parts[3];
          const x = +parts[4];
          const y = +parts[5].slice(0, parts[5].indexOf("."));
          const item = config.datas[id];

          try {
            let tileData;

            switch (scheme) {
              case "pmtiles": {
                tileData = await getPMTilesTile(item.source, z, x, y);

                break;
              }

              case "mbtiles": {
                tileData = await getAndCacheMBTilesTileData(id, z, x, y);

                break;
              }

              case "xyz": {
                tileData = await getAndCacheXYZTileData(id, z, x, y);

                break;
              }

              case "pg":
                tileData = await getAndCachePostgreSQLTileData(id, z, x, y);

                break;
            }

            /* Unzip data */
            data = tileData.headers["content-encoding"]
              ? await unzipAsync(tileData.data)
              : tileData.data;
          } catch (error) {
            printLog(
              "warn",
              `Failed to get data "${id}" - Tile "${`${z}/${x}/${y}`}": ${error}. Serving empty tile...`,
            );

            data = FALLBACK_TILE_DATA[item.tileJSON.format];
          }

          break;
        }

        /* Get data from remote */
        case "http":
        case "https": {
          try {
            data = await getDataFromURL(req.url, {
              method: "GET",
              timeout: DEFAULT_QUERY_TIMEOUT,
              responseType: "arraybuffer",
              decompress: true,
            });
          } catch (error) {
            if (req.kind === 3) {
              const result = req.url.match(/(png|jpg|jpeg|webp|pbf)/g);
              if (result) {
                printLog(
                  "warn",
                  `Failed to get tile from "${req.url}": ${error}. Serving empty tile...`,
                );

                data = FALLBACK_TILE_DATA[result[0]];
              } else {
                printLog("error", `Failed to detect tile from "${req.url}"`);

                err = error;
              }
            } else if (req.kind === 4) {
              const result = req.url.match(/([^/]+\/\d+-\d+\.pbf)/g);
              if (result) {
                printLog(
                  "warn",
                  `Failed to get font from "${req.url}": ${error}. Serving fallback font "Open Sans"...`,
                );

                const parts = result[0].split("/");

                data = await getFallbackFont(parts[0], parts[1]);

                /* Unzip data */
                const headers = detectFormatAndHeaders(data).headers;

                if (headers["content-encoding"]) {
                  data = await unzipAsync(data);
                }
              } else {
                printLog("error", `Failed to detect font from "${req.url}"`);

                err = error;
              }
            } else {
              printLog(
                "error",
                `Failed to get data from "${req.url}": ${error}`,
              );

              err = error;
            }
          }

          break;
        }

        /* Get base64 data */
        case "data": {
          try {
            const dataBase64 = base64ToBuffer(req.url);

            /* Unzip data */
            const headers = detectFormatAndHeaders(dataBase64).headers;

            if (headers["content-encoding"]) {
              data = await unzipAsync(dataBase64);
            } else {
              data = dataBase64;
            }
          } catch (error) {
            printLog("error", `Failed to decode base64 data: ${error}`);

            err = error;
          }

          break;
        }

        /* Default */
        default: {
          err = new Error(`Unknown scheme: "${scheme}"`);

          printLog("error", `Failed to render: ${err}`);

          break;
        }
      }

      // Call callback fn
      callback(err, {
        data,
      });
    },
  });

  // Load style
  renderer.load(option.styleJSON);

  return renderer;
}

/**
 * Create a lazy renderer pool.
 * @param {{ mode: "tile"|"static", styleJSON: object, ratio: number, max: number }} option Option object
 * @returns {object} Renderer pool
 */
function createRendererPool(option) {
  const poolOption = {
    min: 0,
    max: option.max,
  };

  if (option.idleTimeoutMillis) {
    poolOption.idleTimeoutMillis = option.idleTimeoutMillis;
    poolOption.evictionRunIntervalMillis = Math.round(
      option.idleTimeoutMillis / 2,
    );
    poolOption.numTestsPerEvictionRun = option.max;
  }

  return createPool(
    {
      create: () => {
        return createRenderer(option);
      },
      destroy: (renderer) => {
        return renderer.release();
      },
    },
    poolOption,
  );
}

/**
 * Get the persistent renderer pool for an HTTP tile render variant.
 * @param {{ key: string, styleJSON: object, tileScale: number, max?: number }} option Option object
 * @returns {object} Renderer pool
 */
export function getTileRendererPool({
  key,
  styleJSON,
  tileScale,
  max = DEFAULT_CONCURRENCY,
}) {
  const current = tileRendererPools.get(key);

  if (current?.styleJSON === styleJSON) {
    return current.pool;
  }

  const pool = createRendererPool({
    mode: "tile",
    ratio: tileScale,
    styleJSON,
    max,
    idleTimeoutMillis: DEFAULT_RENDERER_IDLE_TIMEOUT,
  });

  tileRendererPools.set(key, {
    styleJSON,
    pool,
  });

  if (current) {
    current.pool
      .drain()
      .then(() => {
        return current.pool.clear();
      })
      .catch((error) => {
        printLog("error", `Failed to clear renderer pool "${key}": ${error}`);
      });
  }

  return pool;
}

/**
 * Get the persistent renderer pool used by WMS/static map requests.
 * Static renderers use a different native map mode than tile renderers.
 * @param {{ key: string, styleJSON: object, tileScale?: number, max?: number }} option Option object
 * @returns {object} Renderer pool
 */
export function getStaticRendererPool({
  key,
  styleJSON,
  tileScale = 1,
  max = DEFAULT_CONCURRENCY,
}) {
  const current = staticRendererPools.get(key);

  if (current?.styleJSON === styleJSON) {
    return current.pool;
  }

  const pool = createRendererPool({
    mode: "static",
    ratio: tileScale,
    styleJSON,
    max,
    idleTimeoutMillis: DEFAULT_RENDERER_IDLE_TIMEOUT,
  });

  staticRendererPools.set(key, {
    styleJSON,
    pool,
  });

  if (current) {
    current.pool
      .drain()
      .then(() => {
        return current.pool.clear();
      })
      .catch((error) => {
        printLog(
          "error",
          `Failed to clear static renderer pool "${key}": ${error}`,
        );
      });
  }

  return pool;
}

/**
 * Render with an acquired renderer and return it to the pool when successful.
 * A renderer that failed is destroyed so it cannot poison later requests.
 * @param {object} renderer Renderer
 * @param {object|undefined} pool Renderer pool
 * @param {object} option Native render options
 * @returns {Promise<Buffer>} Raw image data
 */
async function renderWithRenderer(renderer, pool, option) {
  let renderError;

  try {
    return await new Promise((resolve, reject) => {
      try {
        renderer.render(option, (error, data) => {
          if (error) {
            reject(error);
          } else {
            resolve(data);
          }
        });
      } catch (error) {
        reject(error);
      }
    });
  } catch (error) {
    renderError = error;

    throw error;
  } finally {
    if (pool) {
      if (renderError) {
        await pool.destroy(renderer);
      } else {
        await pool.release(renderer);
      }
    } else {
      renderer.release();
    }
  }
}

/**
 * Render image tile data
 * @param {{ z: number, x: number, y: number, pool?: object, styleJSON: object, pitch?: number, bearing?: number, tileScale: number, tileSize: 256|512, format: "jpeg"|"jpg"|"png"|"webp", grayscale?: boolean, filePath?: string }} options Options
 * @returns {Promise<Buffer|string>}
 */
export async function renderImageTileData(option) {
  const {
    z,
    x,
    y,
    pool,
    pitch = 0,
    bearing = 0,
    format,
    grayscale,
    filePath,
    tileScale,
    tileSize,
    styleJSON,
  } = option;

  const renderer = pool
    ? await pool.acquire()
    : createRenderer({
        mode: "tile",
        ratio: tileScale,
        styleJSON,
      });

  const isNeedHack = z === 0 && tileSize === 256;
  const hackTileSize = isNeedHack ? tileSize * 2 : tileSize;

  const data = await renderWithRenderer(renderer, pool, {
    zoom: z > 0 && tileSize === 256 ? z - 1 : z,
    center: getLonLatFromXYZ(x, y, z, "center", "xyz"),
    width: hackTileSize,
    height: hackTileSize,
    pitch,
    bearing,
  });

  const size = hackTileSize * tileScale;
  const originTileSize = Math.round(size);
  const targetTileSize = isNeedHack ? Math.round(size / 2) : undefined;

  return createImageOutput({
    data,
    rawOption: {
      premultiplied: true,
      width: originTileSize,
      height: originTileSize,
      channels: 4,
    },
    format,
    grayscale,
    filePath,
    width: targetTileSize,
    height: targetTileSize,
  });
}

/**
 * Render image static data
 * @param {{ pool: object, styleJSON: object, pitch: number, bearing: number, tileScale: number, tileSize: 256|512, zoom: number, bbox: [number, number, number, number], format: "jpeg"|"jpg"|"png"|"webp", grayscale: boolean, width: number, height: number, filePath: string }} option Option object
 * @returns {Promise<Buffer|string>}
 */
export async function renderImageStaticData(option) {
  const {
    pool,
    pitch = 0,
    bearing = 0,
    format,
    grayscale,
    filePath,
    tileScale,
    tileSize,
    styleJSON,
    zoom,
    bbox,
    resizeOption,
    width,
    height,
  } = option;

  const renderer = pool
    ? await pool.acquire()
    : createRenderer({
        mode: "static",
        ratio: tileScale,
        styleJSON,
      });

  const sizes = calculateSizes(zoom, bbox, tileSize);
  const data = await renderWithRenderer(renderer, pool, {
    zoom,
    center: [(bbox[0] + bbox[2]) / 2, (bbox[1] + bbox[3]) / 2],
    width: sizes.width,
    height: sizes.height,
    pitch,
    bearing,
  });

  return createImageOutput({
    data,
    rawOption: {
      premultiplied: true,
      width: Math.round(tileScale * sizes.width),
      height: Math.round(tileScale * sizes.height),
      channels: 4,
    },
    format,
    grayscale,
    width,
    height,
    resizeOption,
    filePath,
  });
}

/**
 * Render StyleJSON
 * @param {{ styleJSON: object, tileScale: number, tileSize: 256|512, zoom: number, bbox: [number, number, number, number], format: "jpeg"|"jpg"|"png"|"webp", grayscale: boolean, width: number, height: number }} option Option object
 * @returns {Promise<string>}
 */
export async function renderStyleJSON(option) {
  const MAX_TILE_PX = 8192;

  const sizes = calculateSizes(option.zoom, option.bbox, option.tileSize);
  const totalWidth = Math.round(option.tileScale * sizes.width);
  const totalHeight = Math.round(option.tileScale * sizes.height);

  const id = nanoid();
  const dirPath = `${process.env.DATA_DIR}/exports/style_renders/${option.format}s/${id}`;
  const filePath = `${dirPath}/${id}.${option.format}`;

  if (totalWidth <= MAX_TILE_PX && totalHeight <= MAX_TILE_PX) {
    return await renderImageStaticData({
      ...option,
      filePath,
    });
  } else {
    const [minX, minY] = lonLat4326ToXY3857(option.bbox[0], option.bbox[1]);
    const [maxX, maxY] = lonLat4326ToXY3857(option.bbox[2], option.bbox[3]);

    const xSplits = Math.ceil(totalWidth / MAX_TILE_PX);
    const ySplits = Math.ceil(totalHeight / MAX_TILE_PX);

    const xStep = (maxX - minX) / xSplits;
    const yStep = (maxY - minY) / ySplits;

    const pxStep = totalWidth / xSplits;
    const pyStep = totalHeight / ySplits;

    // Create composite options
    const total = xSplits * ySplits;
    const compositesOption = Array(total);

    const pool = createRendererPool({
      mode: "static",
      ratio: option.tileScale,
      styleJSON: option.styleJSON,
      max: Math.min(DEFAULT_CONCURRENCY, total),
    });

    function* createCompositeOptionGenerator() {
      for (let idx = 0; idx < total; idx++) {
        yield async () => {
          const xi = Math.floor(idx / ySplits);
          const yi = idx % ySplits;

          const subMinX = minX + xi * xStep;
          const subMinY = minY + yi * yStep;
          const subFilePath = `${dirPath}/${idx}.${option.format}`;

          await renderImageStaticData({
            pool,
            styleJSON: option.styleJSON,
            tileScale: option.tileScale,
            tileSize: option.tileSize,
            format: option.format,
            pitch: option.pitch,
            bearing: option.bearing,
            bbox: [
              ...xy3857ToLonLat4326(subMinX, subMinY),
              ...xy3857ToLonLat4326(subMinX + xStep, subMinY + yStep),
            ],
            filePath: subFilePath,
            zoom: option.zoom,
          });

          compositesOption[idx] = {
            limitInputPixels: false,
            input: subFilePath,
            left: Math.round(xi * pxStep),
            top: Math.round(totalHeight - (yi + 1) * pyStep),
          };
        };
      }
    }

    try {
      // Batch run
      await runAllWithLimit(
        createCompositeOptionGenerator(),
        DEFAULT_CONCURRENCY,
      );

      // Create image output
      return await createImageOutput({
        createOption: {
          width: totalWidth,
          height: totalHeight,
          channels: 4,
          background: BACKGROUND_COLOR,
        },
        filePath,
        compositesOption,
        format: option.format,
        width: option.width,
        height: option.height,
        grayscale: option.grayscale,
      });
    } finally {
      await pool.drain();
      await pool.clear();
    }
  }
}

/**
 * Render tile datas
 * @param {{ id: string, storeType: "mbtiles"|"xyz"|"pg", storePath: string, metadata: { [key: string]: any }, maxRendererPoolSize?: number, concurrency?: number, batch?: number, storeTransparent?: boolean, tileScale?: number, tileSize?: 256|512, refreshBefore?: string|number|boolean }} options Options
 * @returns {Promise<void>}
 */
export async function renderTileDatas({
  id,
  storeType,
  storePath,
  metadata,
  maxRendererPoolSize,
  concurrency = DEFAULT_CONCURRENCY,
  batch = DEFAULT_TILE_BATCH_SIZE,
  storeTransparent = DEFAULT_STORE_TRANSPARENT,
  tileScale = 1,
  tileSize = 256,
  refreshBefore,
}) {
  const startTime = Date.now();

  let source;
  let pool;
  let closeDatabaseFunc;

  try {
    /* Calculate summary */
    const { realBBox, total, tileBounds } = getTileBounds({
      bbox: metadata.bounds,
      minZoom: metadata.minzoom,
      maxZoom: metadata.maxzoom,
    });
    const rendererPoolSize = Math.min(
      maxRendererPoolSize ?? concurrency,
      concurrency,
    );

    let log = `Rendering ${total} tiles of style id "${id}" to ${storeType} with:`;
    log += `\n\tStore path: ${storePath}`;
    log += `\n\tStore transparent: ${storeTransparent}`;
    log += `\n\tRenderer pool size: ${rendererPoolSize} - Concurrency: ${concurrency} - Batch: ${batch}`;
    log += `\n\tFormat: ${metadata.format} - Tile scale: ${tileScale} - Tile size: ${tileSize}`;
    log += `\n\tBBox: ${JSON.stringify(metadata.bounds)}- Minzoom: ${metadata.minzoom} - Maxzoom: ${metadata.maxzoom}`;

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

      log += `\n\tRefresh before: Check MD5`;
    }

    printLog("info", log);

    let getTileExtraInfoFunc;
    let storeTileDataFunc;
    let tileOption;

    const item = config.styles[id];
    const styleJSON = await getRenderedStyleJSON(item.path);
    const newMetadata = {
      ...metadata,
      bounds: realBBox,
    };

    /* Create renderer pool */
    pool = createRendererPool({
      mode: "tile",
      ratio: tileScale,
      styleJSON,
      max: rendererPoolSize,
    });

    switch (storeType) {
      default: {
        throw new Error(`Invalid store type "${storeType}"`);
      }

      case "mbtiles": {
        /* Create database */
        printLog("info", "Creating database...");

        source = await openMBTilesDB(storePath, true, DEFAULT_QUERY_TIMEOUT);

        /* Update metadata */
        printLog("info", "Updating metadata...");

        updateMBTilesMetadata(source, newMetadata);

        getTileExtraInfoFunc = async (batchTileBounds) => {
          return getMBTilesTileExtraInfo({
            source,
            tileBounds: batchTileBounds,
            isCreated: refreshTimestamp === true,
          });
        };

        /* Assign tile option */
        tileOption = {
          statement: source.prepare(MBTILES_INSERT_TILE_QUERY),
          created: Date.now(),
          storeTransparent,
          pool,
          styleJSON,
          tileScale,
          tileSize,
          format: metadata.format,
        };

        /* Store data function */
        storeTileDataFunc = async (z, x, y, data) => {
          return await storeMBtilesTileData(z, x, y, data, tileOption);
        };

        /* Close database function */
        closeDatabaseFunc = async () => {
          return closeMBTilesDB(source);
        };

        break;
      }

      case "pg": {
        /* Create database */
        printLog("info", "Creating database...");

        source = await openPostgreSQLDB(storePath, true, DEFAULT_QUERY_TIMEOUT);

        /* Update metadata */
        printLog("info", "Updating metadata...");

        await updatePostgreSQLMetadata(source, newMetadata);

        getTileExtraInfoFunc = async (batchTileBounds) => {
          return await getPostgreSQLTileExtraInfo({
            source,
            tileBounds: batchTileBounds,
            isCreated: refreshTimestamp === true,
          });
        };

        /* Assign tile option */
        tileOption = {
          source,
          created: Date.now(),
          storeTransparent,
          pool,
          styleJSON,
          tileScale,
          tileSize,
          format: metadata.format,
        };

        /* Store data function */
        storeTileDataFunc = async (z, x, y, data) => {
          return await storePostgreSQLTileData(z, x, y, data, tileOption);
        };

        /* Close database function */
        closeDatabaseFunc = async () => {
          return await closePostgreSQLDB(source);
        };

        break;
      }

      case "xyz": {
        const sqliteFilePath = `${storePath}/${path.basename(storePath)}.sqlite`;

        /* Create database */
        printLog("info", "Creating database...");

        source = await openXYZMD5DB(
          sqliteFilePath,
          true,
          DEFAULT_QUERY_TIMEOUT,
        );

        /* Update metadata */
        printLog("info", "Updating metadata...");

        updateXYZMetadata(source, newMetadata);

        getTileExtraInfoFunc = async (batchTileBounds) => {
          return getXYZTileExtraInfo({
            source,
            tileBounds: batchTileBounds,
            isCreated: refreshTimestamp === true,
          });
        };

        /* Assign tile option */
        tileOption = {
          statement: source.prepare(XYZ_INSERT_MD5_QUERY),
          created: Date.now(),
          sourcePath: storePath,
          storeTransparent,
          pool,
          styleJSON,
          tileScale,
          tileSize,
          format: metadata.format,
        };

        /* Store data function */
        storeTileDataFunc = async (z, x, y, data) => {
          return await storeXYZTileFile(z, x, y, data, tileOption);
        };

        /* Close database function */
        closeDatabaseFunc = async () => {
          return closeXYZMD5DB(source);
        };

        break;
      }
    }

    /* Render and store tile data generator */
    let completeTasks = 0;

    function* renderAndStoreTileDataGenerator(batchTileBounds, tileExtraInfo) {
      for (const { z, x, y } of batchTileBounds) {
        for (let xCount = x[0]; xCount <= x[1]; xCount++) {
          for (let yCount = y[0]; yCount <= y[1]; yCount++) {
            completeTasks++;

            const taskNumber = completeTasks;

            yield async () => {
              const tileName = `${z}/${xCount}/${yCount}`;
              const currentTileExtraInfo = tileExtraInfo[tileName];

              try {
                if (refreshTimestamp === true) {
                  printLog(
                    "info",
                    `Rendering style id "${id}" - Tile "${tileName}" - ${taskNumber}/${total}...`,
                  );

                  // Get tile data
                  const data = await renderImageTileData({
                    ...tileOption,
                    z,
                    x: xCount,
                    y: yCount,
                  });

                  if (currentTileExtraInfo === calculateMD5(data)) {
                    return;
                  }

                  // Store tile data
                  await storeTileDataFunc(z, xCount, yCount, data);
                } else {
                  if (
                    refreshTimestamp &&
                    currentTileExtraInfo >= refreshTimestamp
                  ) {
                    return;
                  }

                  printLog(
                    "info",
                    `Rendering style id "${id}" - Tile "${tileName}" - ${taskNumber}/${total}...`,
                  );

                  // Store tile data
                  await storeTileDataFunc(
                    z,
                    xCount,
                    yCount,
                    await renderImageTileData({
                      ...tileOption,
                      z,
                      x: xCount,
                      y: yCount,
                    }),
                  );
                }
              } catch (error) {
                printLog(
                  "error",
                  `Failed to render style id "${id}" - Tile "${tileName}" - ${taskNumber}/${total}: ${error}`,
                );
              } finally {
                delete tileExtraInfo[tileName];
              }
            };
          }
        }
      }
    }

    /* Render and store tile datas */
    printLog("info", "Rendering and storing tile datas...");

    let batchNumber = 0;

    for (const batchTileBounds of getTileBoundsBatches(tileBounds, batch)) {
      if (!item.export) {
        break;
      }

      batchNumber++;

      let tileExtraInfo = {};
      const batchTotal = batchTileBounds.reduce((sum, tileBound) => {
        return sum + tileBound.total;
      }, 0);

      if (refreshTimestamp) {
        try {
          printLog(
            "info",
            `Getting render tile extra info for batch #${batchNumber} (${batchTotal} tiles)...`,
          );

          tileExtraInfo = await getTileExtraInfoFunc(batchTileBounds);
        } catch (error) {
          printLog(
            "error",
            `Failed to get render tile extra info for batch #${batchNumber}: ${error}`,
          );
        }
      }

      await runAllWithLimit(
        renderAndStoreTileDataGenerator(batchTileBounds, tileExtraInfo),
        concurrency,
        item,
      );
    }

    printLog(
      "info",
      `Completed render ${total} tiles of style id "${id}" to ${storeType} after ${
        (Date.now() - startTime) / 1000
      }s!`,
    );
  } catch (error) {
    printLog(
      "error",
      `Failed to render style id "${id}" to ${storeType} after ${
        (Date.now() - startTime) / 1000
      }s: ${error}`,
    );
  } finally {
    /* Destroy renderer pool */
    if (pool) {
      await pool.drain();
      await pool.clear();
    }

    /* Close database */
    if (source && closeDatabaseFunc) {
      await closeDatabaseFunc();
    }
  }
}
