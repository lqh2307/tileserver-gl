"use strict";

import { config } from "./configs/index.js";
import path from "node:path";
import {
  DEFAULT_STORE_TRANSPARENT,
  DEFAULT_QUERY_TIMEOUT,
  DEFAULT_CONCURRENCY,
  DEFAULT_MAX_TRY,
  DEFAULT_TILE_BATCH_SIZE,
} from "./defaults/index.js";
import {
  getTileBoundsBatches,
  createFileWithLock,
  runAllWithLimit,
  createFolders,
  getTileBounds,
  getDuration,
  isLocalURL,
  printLog,
} from "./utils/index.js";
import {
  getAndCachePostgreSQLTileData,
  getPostgreSQLTileExtraInfo,
  getAndCacheMBTilesTileData,
  MBTILES_INSERT_TILE_QUERY,
  updatePostgreSQLMetadata,
  getMBTilesTileExtraInfo,
  storePostgreSQLTileData,
  getAndCacheXYZTileData,
  getAndCacheDataGeoJSON,
  updateMBTilesMetadata,
  getAndCacheDataSprite,
  getAndCacheDataFonts,
  getRenderedStyleJSON,
  storeMBtilesTileData,
  XYZ_INSERT_MD5_QUERY,
  getXYZTileExtraInfo,
  updateXYZMetadata,
  closePostgreSQLDB,
  openPostgreSQLDB,
  storeXYZTileFile,
  storeGeoJSONFile,
  storeSpriteFile,
  storeStyleFile,
  closeMBTilesDB,
  storeFontFile,
  openMBTilesDB,
  closeXYZMD5DB,
  openXYZMD5DB,
  getStyle,
} from "./resources/index.js";

/**
 * Export all
 * @param {string} dirPath Exported dir path
 * @param {{ concurrency?: number, parentServerHost?: string, listenPort?: number, serveFrontPage?: boolean, serveSwagger?: boolean, taskSchedule?: any, postgreSQLBaseURI?: string, process?: number, thread?: number, styles?: string[], exportData?: boolean, refreshBefore?: number }} options Export object
 * @returns {Promise<void>}
 */
export async function exportAll(dirPath, options) {
  const startTime = Date.now();

  try {
    const concurrency = options.concurrency || 256;
    const timeout = 300000; // 5 minutes
    const maxTry = DEFAULT_MAX_TRY;
    const parentServerHost =
      options.parentServerHost || "http://localhost:8080";

    let log = `Exporting all with:`;
    log += `\n\tDirectory path: ${dirPath}`;
    log += `\n\tConcurrency: ${concurrency} - Max try: ${maxTry} - Timeout: ${timeout}`;
    log += `\n\tOptions: ${JSON.stringify(options)}`;

    printLog("info", log);

    // Create folders
    await createFolders([
      path.join(dirPath, "caches", "fonts"),
      path.join(dirPath, "caches", "geojsons"),
      path.join(dirPath, "caches", "mbtiles"),
      path.join(dirPath, "caches", "pmtiles"),
      path.join(dirPath, "caches", "sprites"),
      path.join(dirPath, "caches", "styles"),
      path.join(dirPath, "caches", "xyzs"),
      path.join(dirPath, "exports"),
      path.join(dirPath, "fonts"),
      path.join(dirPath, "geojsons"),
      path.join(dirPath, "mbtiles"),
      path.join(dirPath, "pmtiles"),
      path.join(dirPath, "sprites"),
      path.join(dirPath, "styles"),
      path.join(dirPath, "xyzs"),
    ]);

    // Create config object
    const configObj = {
      options: {
        listenPort: options.listenPort ?? 8080,
        serveFrontPage: options.serveFrontPage ?? true,
        serveSwagger: options.serveSwagger ?? true,
        taskSchedule: options.taskSchedule,
        postgreSQLBaseURI: options.postgreSQLBaseURI,
        process: options.process ?? 2,
        thread: options.thread ?? 16,
      },
      styles: {},
      geojsons: {},
      datas: {},
      sprites: {},
      fonts: {},
    };

    // Create seed object
    const seedObj = {
      styles: {},
      geojsons: {},
      datas: {},
      sprites: {},
      fonts: {},
    };

    // Create cleanUp object
    const cleanUpObj = {
      styles: {},
      geojsons: {},
      datas: {},
      sprites: {},
      fonts: {},
    };

    // Export styles
    if (!options.styles) {
      printLog("info", "No styles to export. Skipping...");
    } else {
      for (const styleID of options.styles) {
        // Get style
        const styleFolder = `${styleID}_cache`;

        const style = config.styles[styleID];

        configObj.styles[styleID] = {
          style: styleFolder,
          cache: {
            store: true,
            forward: true,
          },
        };

        seedObj.styles[styleFolder] = {
          metadata: {
            name: style.name,
            zoom: style.zoom,
            center: style.center,
          },
          url: `${parentServerHost}/styles/${styleID}/style.json?raw=true`,
          refreshBefore: options.refreshBefore,
          timeout,
          maxTry,
          skip: false,
        };

        if (options.exportData) {
          const styleBuffer = await getStyle(style.path);

          await storeStyleFile(
            path.join(dirPath, "caches", "styles", styleFolder, "style.json"),
            styleBuffer,
          );
        }

        const renderedStyleJSON = await getRenderedStyleJSON(style.path);

        // Get sprite
        if (renderedStyleJSON.sprite?.startsWith("sprites://")) {
          const spriteID = renderedStyleJSON.sprite.split("/")[2];

          const spriteFolder = `${spriteID}_cache`;

          configObj.sprites[spriteID] = {
            sprite: spriteFolder,
            cache: {
              store: true,
              forward: true,
            },
          };

          seedObj.sprites[spriteFolder] = {
            url: `${parentServerHost}/sprites/${spriteID}/{name}`,
            refreshBefore: options.refreshBefore,
            timeout,
            maxTry,
            skip: false,
          };

          if (options.exportData) {
            const [spriteJSONBuffer, spritePNGBuffer] = await Promise.all([
              getAndCacheDataSprite(spriteID, "sprite.json"),
              getAndCacheDataSprite(spriteID, "sprite.png"),
            ]);

            await Promise.all([
              storeSpriteFile(
                path.join(
                  dirPath,
                  "caches",
                  "sprites",
                  spriteFolder,
                  "sprite.json",
                ),
                spriteJSONBuffer,
              ),
              storeSpriteFile(
                path.join(
                  dirPath,
                  "caches",
                  "sprites",
                  spriteFolder,
                  "sprite.png",
                ),
                spritePNGBuffer,
              ),
            ]);
          }
        }

        // Get font
        if (renderedStyleJSON.glyphs?.startsWith("fonts://")) {
          const fonts = [];

          for (const layer of renderedStyleJSON.layers) {
            if (layer.layout?.["text-font"]) {
              fonts.push(...layer.layout["text-font"]);
            }
          }

          for (const fontID of new Set(fonts)) {
            const fontFolder = `${fontID}_cache`;

            configObj.fonts[fontID] = {
              font: fontFolder,
              cache: {
                store: true,
                forward: true,
              },
            };

            seedObj.fonts[fontFolder] = {
              url: `${parentServerHost}/fonts/${fontID}/{range}.pbf`,
              refreshBefore: options.refreshBefore,
              timeout,
              concurrency,
              maxTry,
              skip: false,
            };

            if (options.exportData) {
              function* seedFontDataGenerator() {
                for (let idx = 0; idx < 256; idx++) {
                  yield async () => {
                    const rangeStart = idx * 256;
                    const rangeEnd = rangeStart + 255;

                    const fileName = `${rangeStart}-${rangeEnd}.pbf`;

                    await storeFontFile(
                      path.join(
                        dirPath,
                        "caches",
                        "fonts",
                        fontFolder,
                        fileName,
                      ),
                      await getAndCacheDataFonts(fontID, fileName),
                    );
                  };
                }
              }

              // Batch run
              await runAllWithLimit(seedFontDataGenerator(), concurrency);
            }
          }
        }

        // Get source
        for (const sourceName of Object.keys(renderedStyleJSON.sources)) {
          // Get geojson source
          const source = renderedStyleJSON.sources[sourceName];

          if (source.data) {
            if (isLocalURL(source.data)) {
              const parts = source.data.split("/");

              const geojsonFolder = `${parts[3]}_cache`;

              configObj.geojsons[parts[2]] = {
                [parts[3]]: {
                  geojson: geojsonFolder,
                  cache: {
                    store: true,
                    forward: true,
                  },
                },
              };

              seedObj.geojsons[geojsonFolder] = {
                url: `${parentServerHost}/geojsons/${parts[2]}/${parts[3]}.geojson`,
                refreshBefore: options.refreshBefore,
                timeout,
                maxTry,
                skip: false,
              };

              if (options.exportData) {
                const geoJSONBuffer = await getAndCacheDataGeoJSON(
                  parts[2],
                  parts[3],
                );

                await storeGeoJSONFile(
                  path.join(
                    dirPath,
                    "caches",
                    "geojsons",
                    geojsonFolder,
                    `${geojsonFolder}.geojson`,
                  ),
                  geoJSONBuffer,
                );
              }
            }
          }

          // Get tile source
          if (source.tiles) {
            for (const tile of source.tiles) {
              if (isLocalURL(tile)) {
                const dataID = tile.split("/")[2];

                const dataFolder = `${dataID}_cache`;

                const data = config.datas[dataID];

                const coverages = getTileBounds({
                  zoom: data.tileJSON.bounds,
                  minZoom: data.tileJSON.minzoom,
                  maxZoom: data.tileJSON.maxzoom,
                });

                let storePath;

                switch (data.sourceType) {
                  case "xyz": {
                    configObj.datas[dataID] = {
                      xyz: dataFolder,
                      cache: {
                        store: true,
                        forward: true,
                      },
                    };

                    seedObj.datas[dataFolder] = {
                      metadata: data.tileJSON,
                      url: `${parentServerHost}/datas/${dataID}/{z}/{x}/{y}.${data.tileJSON.format}`,
                      scheme: "xyz",
                      refreshBefore: options.refreshBefore,
                      coverages,
                      timeout,
                      concurrency,
                      maxTry,
                      storeType: "xyz",
                      storeTransparent: true,
                      skip: false,
                    };

                    if (options.exportData) {
                      storePath = path.join(
                        dirPath,
                        "caches",
                        "datas/xyzs",
                        dataFolder,
                      );
                    }

                    break;
                  }

                  case "mbtiles": {
                    configObj.datas[dataID] = {
                      mbtiles: dataFolder,
                      cache: {
                        store: true,
                        forward: true,
                      },
                    };

                    seedObj.datas[dataFolder] = {
                      metadata: data.tileJSON,
                      url: `${parentServerHost}/datas/${dataID}/{z}/{x}/{y}.${data.tileJSON.format}`,
                      scheme: "xyz",
                      refreshBefore: options.refreshBefore,
                      coverages,
                      timeout,
                      concurrency,
                      maxTry,
                      storeType: "mbtiles",
                      storeTransparent: true,
                      skip: false,
                    };

                    if (options.exportData) {
                      storePath = path.join(
                        dirPath,
                        "caches",
                        "datas/mbtiles",
                        dataFolder,
                        `${dataFolder}.mbtiles`,
                      );
                    }

                    break;
                  }

                  case "pg": {
                    configObj.datas[dataID] = {
                      pg: dataFolder,
                      cache: {
                        store: true,
                        forward: true,
                      },
                    };

                    seedObj.datas[dataFolder] = {
                      metadata: data.tileJSON,
                      url: `${parentServerHost}/datas/${dataID}/{z}/{x}/{y}.${data.tileJSON.format}`,
                      scheme: "xyz",
                      refreshBefore: options.refreshBefore,
                      coverages,
                      timeout,
                      concurrency,
                      maxTry,
                      storeType: "pg",
                      storeTransparent: true,
                      skip: false,
                    };

                    if (options.exportData) {
                      storePath = `${process.env.POSTGRESQL_BASE_URI}/${dataFolder}`;
                    }

                    break;
                  }
                }

                if (options.exportData) {
                  await exportTileDatas({
                    id: dataID,
                    storeType: data.sourceType,
                    storePath,
                    metadata: data.tileJSON,
                    coverages,
                    concurrency,
                    storeTransparent: options.storeTransparent,
                    refreshBefore:
                      options.refreshBefore?.time ||
                      options.refreshBefore?.day ||
                      options.refreshBefore?.md5,
                  });
                }
              }
            }
          }
        }
      }
    }

    // Export datas
    if (!options.datas) {
      printLog("info", "No datas to export. Skipping...");
    } else {
      for (const dataID of options.datas) {
        // Get data
        const dataFolder = `${dataID}_cache`;

        const data = config.datas[dataID];

        const coverages = getTileBounds({
          zoom: data.tileJSON.bounds,
          minZoom: data.tileJSON.minzoom,
          maxZoom: data.tileJSON.maxzoom,
        });

        let storePath;

        switch (data.sourceType) {
          case "xyz": {
            configObj.datas[dataID] = {
              xyz: dataFolder,
              cache: {
                store: true,
                forward: true,
              },
            };

            seedObj.datas[dataFolder] = {
              metadata: data.tileJSON,
              url: `${parentServerHost}/datas/${dataID}/{z}/{x}/{y}.${data.tileJSON.format}`,
              scheme: "xyz",
              refreshBefore: options.refreshBefore,
              coverages,
              timeout,
              concurrency,
              maxTry,
              storeType: "xyz",
              storeTransparent: true,
              skip: false,
            };

            if (options.exportData) {
              storePath = path.join(
                dirPath,
                "caches",
                "datas/xyzs",
                dataFolder,
              );
            }

            break;
          }

          case "mbtiles": {
            configObj.datas[dataID] = {
              mbtiles: dataFolder,
              cache: {
                store: true,
                forward: true,
              },
            };

            seedObj.datas[dataFolder] = {
              metadata: data.tileJSON,
              url: `${parentServerHost}/datas/${dataID}/{z}/{x}/{y}.${data.tileJSON.format}`,
              scheme: "xyz",
              refreshBefore: options.refreshBefore,
              coverages,
              timeout,
              concurrency,
              maxTry,
              storeType: "mbtiles",
              storeTransparent: true,
              skip: false,
            };

            if (options.exportData) {
              storePath = path.join(
                dirPath,
                "caches",
                "datas/mbtiles",
                dataFolder,
                `${dataFolder}.mbtiles`,
              );
            }

            break;
          }

          case "pg": {
            configObj.datas[dataID] = {
              pg: dataFolder,
              cache: {
                store: true,
                forward: true,
              },
            };

            seedObj.datas[dataFolder] = {
              metadata: data.tileJSON,
              url: `${parentServerHost}/datas/${dataID}/{z}/{x}/{y}.${data.tileJSON.format}`,
              scheme: "xyz",
              refreshBefore: options.refreshBefore,
              coverages,
              timeout,
              concurrency,
              maxTry,
              storeType: "pg",
              storeTransparent: true,
              skip: false,
            };

            if (options.exportData) {
              storePath = `${process.env.POSTGRESQL_BASE_URI}/${dataFolder}`;
            }

            break;
          }
        }

        if (options.exportData) {
          storePath = `${process.env.POSTGRESQL_BASE_URI}/${dataFolder}`;

          await exportTileDatas({
            id: dataID,
            storeType: data.sourceType,
            storePath,
            metadata: data.tileJSON,
            coverages,
            concurrency,
            storeTransparent: options.storeTransparent,
            refreshBefore:
              options.refreshBefore?.time ||
              options.refreshBefore?.day ||
              options.refreshBefore?.md5,
          });
        }
      }
    }

    // Export geojsons
    if (!options.geojsons) {
      printLog("info", "No GeoJSONs to export. Skipping...");
    } else {
      // Get geojson
      for (const group of options.geojsons) {
        configObj.geojsons[group] = {};

        for (const layer of Object.keys(options.geojsons[group])) {
          const geojsonFolder = `${layer}_cache`;

          configObj.geojsons[group][layer] = {
            geojson: geojsonFolder,
            cache: {
              store: true,
              forward: true,
            },
          };

          seedObj.geojsons[geojsonFolder] = {
            url: `${parentServerHost}/geojsons/${group}/${layer}.geojson`,
            refreshBefore: options.refreshBefore,
            timeout,
            maxTry,
            skip: false,
          };

          if (options.exportData) {
            const geoJSONBuffer = await getAndCacheDataGeoJSON(group, layer);

            await storeGeoJSONFile(
              path.join(
                dirPath,
                "caches",
                "geojsons",
                geojsonFolder,
                `${geojsonFolder}.geojson`,
              ),
              geoJSONBuffer,
            );
          }
        }
      }
    }

    // Export sprite
    if (!options.sprites) {
      printLog("info", "No sprites to export. Skipping...");
    } else {
      // Get sprite
      for (const spriteID of options.sprites) {
        const spriteFolder = `${spriteID}_cache`;

        configObj.sprites[spriteID] = {
          sprite: spriteFolder,
          cache: {
            store: true,
            forward: true,
          },
        };

        seedObj.sprites[spriteFolder] = {
          url: `${parentServerHost}/sprites/${spriteID}/{name}`,
          refreshBefore: options.refreshBefore,
          timeout,
          maxTry,
          skip: false,
        };

        if (options.exportData) {
          const [spriteJSONBuffer, spritePNGBuffer] = await Promise.all([
            getAndCacheDataSprite(spriteID, "sprite.json"),
            getAndCacheDataSprite(spriteID, "sprite.png"),
          ]);

          await Promise.all([
            storeSpriteFile(
              path.join(
                dirPath,
                "caches",
                "sprites",
                spriteFolder,
                "sprite.json",
              ),
              spriteJSONBuffer,
            ),
            storeSpriteFile(
              path.join(
                dirPath,
                "caches",
                "sprites",
                spriteFolder,
                "sprite.png",
              ),
              spritePNGBuffer,
            ),
          ]);
        }
      }
    }

    // Export fonts
    if (!options.fonts) {
      printLog("info", "No fonts to export. Skipping...");
    } else {
      // Get font
      for (const fontID of Object.keys(config.fonts)) {
        const fontFolder = `${fontID}_cache`;

        configObj.fonts[fontID] = {
          font: fontFolder,
          cache: {
            store: true,
            forward: true,
          },
        };

        seedObj.fonts[fontFolder] = {
          url: `${parentServerHost}/fonts/${fontID}/{range}.pbf`,
          refreshBefore: options.refreshBefore,
          timeout,
          concurrency,
          maxTry,
          skip: false,
        };

        if (options.exportData) {
          function* seedFontDataGenerator() {
            for (let idx = 0; idx < 256; idx++) {
              yield async () => {
                const rangeStart = idx * 256;
                const rangeEnd = rangeStart + 255;

                const fileName = `${rangeStart}-${rangeEnd}.pbf`;

                await storeFontFile(
                  path.join(dirPath, "caches", "fonts", fontFolder, fileName),
                  await getAndCacheDataFonts(fontID, fileName),
                );
              };
            }
          }

          // Batch run
          await runAllWithLimit(seedFontDataGenerator(), concurrency);
        }
      }
    }

    // Export config files
    await Promise.all([
      createFileWithLock(
        path.join(dirPath, "config.json"),
        JSON.stringify(configObj, null, 2),
        timeout,
      ),
      createFileWithLock(
        path.join(dirPath, "seed.json"),
        JSON.stringify(seedObj, null, 2),
        timeout,
      ),
      createFileWithLock(
        path.join(dirPath, "cleanup.json"),
        JSON.stringify(cleanUpObj, null, 2),
        timeout,
      ),
    ]);

    printLog("info", `Completed all after ${getDuration(startTime)}s!`);
  } catch (error) {
    printLog(
      "error",
      `Failed to export all after ${getDuration(startTime)}s: ${error}`,
    );
  }
}

/**
 * Export tile datas
 * @param {{ id: string, storeType: "mbtiles"|"xyz"|"pg", storePath: string, metadata: { [key: string]: any }, coverages: { zoom: number, bbox: [number, number, number, number]}[], concurrency?: number, batch?: number, storeTransparent?: boolean, refreshBefore?: string|number|boolean }} options Options
 * @returns {Promise<void>}
 */
export async function exportTileDatas({
  id,
  storeType,
  storePath,
  metadata,
  coverages,
  concurrency = DEFAULT_CONCURRENCY,
  batch = DEFAULT_TILE_BATCH_SIZE,
  storeTransparent = DEFAULT_STORE_TRANSPARENT,
  refreshBefore,
}) {
  const startTime = Date.now();

  let source;
  let closeDatabaseFunc;

  try {
    /* Calculate summary */
    const { realBBox, total, tileBounds } = getTileBounds({
      coverages,
    });

    let log = `Exporting ${total} tiles of data id "${id}" to ${storeType} with:`;
    log += `\n\tSource path: ${storePath}`;
    log += `\n\tStore transparent: ${storeTransparent}`;
    log += `\n\tConcurrency: ${concurrency} - Batch: ${batch}`;
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

      log += `\n\tRefresh before: Check MD5`;
    }

    printLog("info", log);

    let getTargetTileExtraInfo;
    let getTileExtraInfo;
    let getTileDataFunc;
    let storeTileDataFunc;
    let sqliteFilePath;
    let tileOption;

    const item = config.datas[id];
    const newMetadata = {
      ...metadata,
      bounds: realBBox,
    };

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

        /* Get tile extra info function */
        getTileExtraInfo = async (batchTileBounds, isCreated) => {
          return getMBTilesTileExtraInfo({
            source,
            tileBounds: batchTileBounds,
            isCreated,
          });
        };

        /* Assign tile option */
        tileOption = {
          statement: source.prepare(MBTILES_INSERT_TILE_QUERY),
          created: Date.now(),
          storeTransparent,
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

        /* Get tile extra info function */
        getTileExtraInfo = async (batchTileBounds, isCreated) => {
          return await getPostgreSQLTileExtraInfo({
            source,
            tileBounds: batchTileBounds,
            isCreated,
          });
        };

        /* Assign tile option */
        tileOption = {
          source,
          created: Date.now(),
          storeTransparent,
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
        sqliteFilePath = path.join(
          storePath,
          `${path.basename(storePath)}.sqlite`,
        );

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

        /* Get tile extra info function */
        getTileExtraInfo = async (batchTileBounds, isCreated) => {
          return getXYZTileExtraInfo({
            source,
            tileBounds: batchTileBounds,
            isCreated,
          });
        };

        /* Assign tile option */
        tileOption = {
          statement: source.prepare(XYZ_INSERT_MD5_QUERY),
          created: Date.now(),
          sourcePath: storePath,
          format: metadata.format,
          storeTransparent,
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

    switch (item.sourceType) {
      case "mbtiles": {
        getTargetTileExtraInfo = async (batchTileBounds) => {
          return getMBTilesTileExtraInfo({
            source: item.source,
            tileBounds: batchTileBounds,
          });
        };

        /* Get data function */
        getTileDataFunc = async (z, x, y) => {
          const tile = await getAndCacheMBTilesTileData(id, z, x, y);

          return tile.data;
        };

        break;
      }

      case "pg": {
        getTargetTileExtraInfo = async (batchTileBounds) => {
          return await getPostgreSQLTileExtraInfo({
            source: item.source,
            tileBounds: batchTileBounds,
          });
        };

        /* Get data function */
        getTileDataFunc = async (z, x, y) => {
          const tile = await getAndCachePostgreSQLTileData(id, z, x, y);

          return tile.data;
        };

        break;
      }

      case "xyz": {
        getTargetTileExtraInfo = async (batchTileBounds) => {
          return getXYZTileExtraInfo({
            source: item.md5Source,
            tileBounds: batchTileBounds,
          });
        };

        /* Get data function */
        getTileDataFunc = async (z, x, y) => {
          const tile = await getAndCacheXYZTileData(id, z, x, y);

          return tile.data;
        };

        break;
      }
    }

    let completeTasks = 0;

    /* Export and store one tile data batch */
    function* exportAndStoreTileDataGenerator(
      batchTileBounds,
      targetTileExtraInfo,
      tileExtraInfo,
    ) {
      for (const { z, x, y } of batchTileBounds) {
        for (let xCount = x[0]; xCount <= x[1]; xCount++) {
          for (let yCount = y[0]; yCount <= y[1]; yCount++) {
            completeTasks++;

            const taskNumber = completeTasks;

            yield async () => {
              const tileName = `${z}/${xCount}/${yCount}`;
              const currentTileExtraInfo = tileExtraInfo[tileName];
              const currentTargetTileExtraInfo = targetTileExtraInfo[tileName];

              try {
                if (
                  refreshTimestamp === true
                    ? currentTileExtraInfo &&
                      currentTileExtraInfo === currentTargetTileExtraInfo
                    : refreshTimestamp &&
                      currentTileExtraInfo >= refreshTimestamp
                ) {
                  return;
                }

                printLog(
                  "info",
                  `Exporting data id "${id}" - Tile "${tileName}" - ${taskNumber}/${total}...`,
                );

                try {
                  await storeTileDataFunc(
                    z,
                    xCount,
                    yCount,
                    await getTileDataFunc(z, xCount, yCount),
                  );
                } catch (error) {
                  printLog(
                    "error",
                    `Failed to export data id "${id}" - Tile "${tileName}" - ${taskNumber}/${total}: ${error}`,
                  );
                }
              } finally {
                delete tileExtraInfo[tileName];
                delete targetTileExtraInfo[tileName];
              }
            };
          }
        }
      }
    }

    /* Export and store tile datas */
    printLog("info", "Exporting and storing tile datas...");

    for (const batchTileBounds of getTileBoundsBatches(tileBounds, batch)) {
      let targetTileExtraInfo = {};
      let tileExtraInfo = {};

      try {
        if (refreshTimestamp === true) {
          [targetTileExtraInfo, tileExtraInfo] = await Promise.all([
            getTargetTileExtraInfo(batchTileBounds),
            getTileExtraInfo(batchTileBounds, false),
          ]);
        } else if (refreshTimestamp) {
          tileExtraInfo = await getTileExtraInfo(batchTileBounds, true);
        }
      } catch (error) {
        printLog(
          "error",
          `Failed to get target or exported tile extra info for a batch: ${error}`,
        );

        targetTileExtraInfo = {};
        tileExtraInfo = {};
      }

      await runAllWithLimit(
        exportAndStoreTileDataGenerator(
          batchTileBounds,
          targetTileExtraInfo,
          tileExtraInfo,
        ),
        concurrency,
        item,
      );

      if (!item.export) {
        break;
      }
    }

    printLog(
      "info",
      `Completed export ${total} tiles of data id "${id}" to ${storeType} after ${getDuration(
        startTime,
      )}s!`,
    );
  } catch (error) {
    printLog(
      "error",
      `Failed to export data id "${id}" to ${storeType} after ${getDuration(
        startTime,
      )}s: ${error}`,
    );
  } finally {
    /* Close database */
    if (source && closeDatabaseFunc) {
      await closeDatabaseFunc();
    }
  }
}
