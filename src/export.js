"use strict";

import { config } from "./configs/index.js";
import path from "path";
import {
  createFileWithLock,
  runAllWithLimit,
  createFolders,
  getTileBounds,
  isLocalURL,
  printLog,
} from "./utils/index.js";
import {
  getPostgreSQLTileExtraInfoFromCoverages,
  getMBTilesTileExtraInfoFromCoverages,
  getXYZTileExtraInfoFromCoverages,
  getAndCachePostgreSQLTileData,
  getAndCacheMBTilesTileData,
  MBTILES_INSERT_TILE_QUERY,
  updatePostgreSQLMetadata,
  storePostgreSQLTileData,
  getAndCacheXYZTileData,
  getAndCacheDataGeoJSON,
  updateMBTilesMetadata,
  getAndCacheDataSprite,
  getAndCacheDataFonts,
  getRenderedStyleJSON,
  storeMBtilesTileData,
  XYZ_INSERT_MD5_QUERY,
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
 * @param {object} options Export object
 * @param {number} concurrency Concurrency
 * @param {boolean} storeTransparent Is store transparent tile?
 * @param {string} parentServerHost Parent server host
 * @param {boolean} exportData Is export data?
 * @param {string|number|boolean} refreshBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be refreshed/Compare MD5
 * @returns {Promise<void>}
 */
export async function exportAll(
  dirPath,
  options,
  concurrency,
  storeTransparent,
  parentServerHost,
  exportData,
  refreshBefore,
) {
  const startTime = Date.now();

  try {
    concurrency = concurrency || 256;
    const timeout = 300000; // 5 minutes
    const maxTry = 5;

    let log = `Exporting all with:`;
    log += `\n\tDirectory path: ${dirPath}`;
    log += `\n\tConcurrency: ${concurrency} - Max try: ${maxTry} - Timeout: ${timeout}`;
    log += `\n\tOptions: ${JSON.stringify(options)}`;

    printLog("info", log);

    // Create folders
    await createFolders([
      `${dirPath}/caches/fonts`,
      `${dirPath}/caches/geojsons`,
      `${dirPath}/caches/mbtiles`,
      `${dirPath}/caches/pmtiles`,
      `${dirPath}/caches/sprites`,
      `${dirPath}/caches/styles`,
      `${dirPath}/caches/xyzs`,
      `${dirPath}/exports`,
      `${dirPath}/fonts`,
      `${dirPath}/geojsons`,
      `${dirPath}/mbtiles`,
      `${dirPath}/pmtiles`,
      `${dirPath}/sprites`,
      `${dirPath}/styles`,
      `${dirPath}/xyzs`,
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
          refreshBefore: {
            md5: true,
          },
          timeout: timeout,
          maxTry: maxTry,
          skip: false,
        };

        if (exportData) {
          const styleBuffer = await getStyle(style.path);

          await storeStyleFile(
            `${dirPath}/caches/styles/${styleFolder}/style.json`,
            styleBuffer,
          );
        }

        // Get font
        if (renderedStyleJSON.sprite.startsWith("fonts://")) {
          const fonts = [];

          for (const layer of renderedStyleJSON.layers) {
            if (
              layer.layout !== undefined &&
              layer.layout["text-font"] !== undefined
            ) {
              fonts.push(...layer.layout["text-font"]);
            }
          }

          for (const fontID of Array.from(new Set(fonts))) {
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
              refreshBefore: {
                md5: true,
              },
              timeout: timeout,
              concurrency: concurrency,
              maxTry: maxTry,
              skip: false,
            };

            if (exportData) {
              function* seedFontDataGenerator() {
                for (let idx = 0; idx < 256; idx++) {
                  yield async () => {
                    const rangeStart = idx * 256;
                    const rangeEnd = rangeStart + 255;

                    const fileName = `${rangeStart}-${rangeEnd}.pbf`;

                    await storeFontFile(
                      `${dirPath}/caches/fonts/${fontFolder}/${fileName}`,
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

        // Get sprite
        if (renderedStyleJSON.sprite.startsWith("sprites://")) {
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
            refreshBefore: {
              md5: true,
            },
            timeout: timeout,
            maxTry: maxTry,
            skip: false,
          };

          if (exportData) {
            const [spriteJSONBuffer, spritePNGBuffer] = await Promise.all([
              getAndCacheDataSprite(spriteID, "sprite.json"),
              getAndCacheDataSprite(spriteID, "sprite.png"),
            ]);

            await Promise.all([
              storeSpriteFile(
                `${dirPath}/caches/sprites/${spriteFolder}/sprite.json`,
                spriteJSONBuffer,
              ),
              storeSpriteFile(
                `${dirPath}/caches/sprites/${spriteFolder}/sprite.png`,
                spritePNGBuffer,
              ),
            ]);
          }
        }

        // Get source
        const renderedStyleJSON = await getRenderedStyleJSON(style.path);

        for (const sourceName of Object.keys(renderedStyleJSON.sources)) {
          // Get geojson source
          const source = renderedStyleJSON.sources[sourceName];

          if (source.data !== undefined) {
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
                refreshBefore: {
                  md5: true,
                },
                timeout: timeout,
                maxTry: maxTry,
                skip: false,
              };

              if (exportData) {
                const geoJSONBuffer = await getAndCacheDataGeoJSON(
                  parts[2],
                  parts[3],
                );

                await storeGeoJSONFile(
                  `${dirPath}/caches/geojsons/${geojsonFolder}/${geojsonFolder}.geojson`,
                  geoJSONBuffer,
                );
              }
            }
          }

          // Get tile source
          if (source.tiles !== undefined) {
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
                      refreshBefore: {
                        md5: true,
                      },
                      coverages: coverages,
                      timeout: timeout,
                      concurrency: concurrency,
                      maxTry: maxTry,
                      storeType: "xyz",
                      storeTransparent: true,
                      skip: false,
                    };

                    if (exportData) {
                      storePath = `${dirPath}/caches/datas/xyzs/${dataFolder}`;
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
                      refreshBefore: {
                        md5: true,
                      },
                      coverages: coverages,
                      timeout: timeout,
                      concurrency: concurrency,
                      maxTry: maxTry,
                      storeType: "mbtiles",
                      storeTransparent: true,
                      skip: false,
                    };

                    if (exportData) {
                      storePath = `${dirPath}/caches/datas/mbtiles/${dataFolder}/${dataFolder}.mbtiles`;
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
                      refreshBefore: {
                        md5: true,
                      },
                      coverages: coverages,
                      timeout: timeout,
                      concurrency: concurrency,
                      maxTry: maxTry,
                      storeType: "pg",
                      storeTransparent: true,
                      skip: false,
                    };

                    if (exportData) {
                      storePath = `${process.env.POSTGRESQL_BASE_URI}/${dataFolder}`;
                    }

                    break;
                  }
                }

                if (exportData) {
                  await exportTileDatas(
                    dataID,
                    data.sourceType,
                    storePath,
                    data.tileJSON,
                    coverages,
                    concurrency,
                    storeTransparent,
                    refreshBefore,
                  );
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
              refreshBefore: {
                md5: true,
              },
              coverages: coverages,
              timeout: timeout,
              concurrency: concurrency,
              maxTry: maxTry,
              storeType: "xyz",
              storeTransparent: true,
              skip: false,
            };

            if (exportData) {
              storePath = `${dirPath}/caches/datas/xyzs/${dataFolder}`;
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
              refreshBefore: {
                md5: true,
              },
              coverages: coverages,
              timeout: timeout,
              concurrency: concurrency,
              maxTry: maxTry,
              storeType: "mbtiles",
              storeTransparent: true,
              skip: false,
            };

            if (exportData) {
              storePath = `${dirPath}/caches/datas/mbtiles/${dataFolder}/${dataFolder}.mbtiles`;
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
              refreshBefore: {
                md5: true,
              },
              coverages: coverages,
              timeout: timeout,
              concurrency: concurrency,
              maxTry: maxTry,
              storeType: "pg",
              storeTransparent: true,
              skip: false,
            };

            if (exportData) {
              storePath = `${process.env.POSTGRESQL_BASE_URI}/${dataFolder}`;
            }

            break;
          }
        }

        if (exportData) {
          storePath = `${process.env.POSTGRESQL_BASE_URI}/${dataFolder}`;

          await exportTileDatas(
            dataID,
            data.sourceType,
            storePath,
            data.tileJSON,
            coverages,
            concurrency,
            storeTransparent,
            refreshBefore,
          );
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
            refreshBefore: {
              md5: true,
            },
            timeout: timeout,
            maxTry: maxTry,
            skip: false,
          };

          if (exportData) {
            const geoJSONBuffer = await getAndCacheDataGeoJSON(group, layer);

            await storeGeoJSONFile(
              `${dirPath}/caches/geojsons/${geojsonFolder}/${geojsonFolder}.geojson`,
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
          refreshBefore: {
            md5: true,
          },
          timeout: timeout,
          maxTry: maxTry,
          skip: false,
        };

        if (exportData) {
          const [spriteJSONBuffer, spritePNGBuffer] = await Promise.all([
            getAndCacheDataSprite(spriteID, "sprite.json"),
            getAndCacheDataSprite(spriteID, "sprite.png"),
          ]);

          await Promise.all([
            storeSpriteFile(
              `${dirPath}/caches/sprites/${spriteFolder}/sprite.json`,
              spriteJSONBuffer,
            ),
            storeSpriteFile(
              `${dirPath}/caches/sprites/${spriteFolder}/sprite.png`,
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
          refreshBefore: {
            md5: true,
          },
          timeout: timeout,
          concurrency: concurrency,
          maxTry: maxTry,
          skip: false,
        };

        if (exportData) {
          function* seedFontDataGenerator() {
            for (let idx = 0; idx < 256; idx++) {
              yield async () => {
                const rangeStart = idx * 256;
                const rangeEnd = rangeStart + 255;

                const fileName = `${rangeStart}-${rangeEnd}.pbf`;

                await storeFontFile(
                  `${dirPath}/caches/fonts/${fontFolder}/${fileName}`,
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
        `${dirPath}/config.json`,
        JSON.stringify(configObj, null, 2),
        timeout,
      ),
      createFileWithLock(
        `${dirPath}/seed.json`,
        JSON.stringify(seedObj, null, 2),
        timeout,
      ),
      createFileWithLock(
        `${dirPath}/cleanup.json`,
        JSON.stringify(cleanUpObj, null, 2),
        timeout,
      ),
    ]);

    printLog(
      "info",
      `Completed all after ${(Date.now() - startTime) / 1000}s!`,
    );
  } catch (error) {
    printLog(
      "error",
      `Failed to export all after ${(Date.now() - startTime) / 1000}s: ${error}`,
    );
  }
}

/**
 * Export tile datas
 * @param {string} id Data ID
 * @param {"mbtiles"|"xyz"|"pg"} storeType Store type
 * @param {string} storePath Exported path
 * @param {object} metadata Metadata object
 * @param {{ zoom: number, bbox: [number, number, number, number]}[]} coverages Specific coverages
 * @param {number} concurrency Concurrency
 * @param {boolean} storeTransparent Is store transparent tile?
 * @param {string|number|boolean} refreshBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be refreshed/Compare MD5
 * @returns {Promise<void>}
 */
export async function exportTileDatas(
  id,
  storeType,
  storePath,
  metadata,
  coverages,
  concurrency,
  storeTransparent,
  refreshBefore,
) {
  const startTime = Date.now();

  let source;
  let closeDatabaseFunc;

  try {
    /* Calculate summary */
    const { realBBox, total, tileBounds } = getTileBounds({
      coverages: coverages,
    });

    let log = `Exporting ${total} tiles of data id "${id}" to ${storeType} with:`;
    log += `\n\tSource path: ${storePath}`;
    log += `\n\tStore transparent: ${storeTransparent}`;
    log += `\n\tConcurrency: ${concurrency}`;
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

    let targetTileExtraInfo;
    let tileExtraInfo;
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

        source = await openMBTilesDB(
          storePath,
          true,
          30000, // 30 seconds
        );

        /* Update metadata */
        printLog("info", "Updating metadata...");

        updateMBTilesMetadata(source, newMetadata);

        /* Get tile extra info function */
        getTileExtraInfo = async () =>
          getMBTilesTileExtraInfoFromCoverages(source, coverages, false);

        /* Assign tile option */
        tileOption = {
          statement: source.prepare(MBTILES_INSERT_TILE_QUERY),
          created: Date.now(),
          storeTransparent: storeTransparent,
        };

        /* Store data function */
        storeTileDataFunc = async (z, x, y, data) =>
          await storeMBtilesTileData(z, x, y, data, tileOption);

        /* Close database function */
        closeDatabaseFunc = async () => closeMBTilesDB(source);

        break;
      }

      case "pg": {
        /* Create database */
        printLog("info", "Creating database...");

        source = await openPostgreSQLDB(
          storePath,
          true,
          30000, // 30 seconds
        );

        /* Update metadata */
        printLog("info", "Updating metadata...");

        await updatePostgreSQLMetadata(source, newMetadata);

        /* Get tile extra info function */
        getTileExtraInfo = async () =>
          await getPostgreSQLTileExtraInfoFromCoverages(
            source,
            coverages,
            true,
          );

        /* Assign tile option */
        tileOption = {
          source: source,
          created: Date.now(),
          storeTransparent: storeTransparent,
        };

        /* Store data function */
        storeTileDataFunc = async (z, x, y, data) =>
          await storePostgreSQLTileData(z, x, y, data, tileOption);

        /* Close database function */
        closeDatabaseFunc = async () => await closePostgreSQLDB(source);

        break;
      }

      case "xyz": {
        sqliteFilePath = `${storePath}/${path.basename(storePath)}.sqlite`;

        /* Create database */
        printLog("info", "Creating database...");

        source = await openXYZMD5DB(
          sqliteFilePath,
          true,
          30000, // 30 seconds
        );

        /* Update metadata */
        printLog("info", "Updating metadata...");

        updateXYZMetadata(source, newMetadata);

        /* Get tile extra info function */
        getTileExtraInfo = async () =>
          getXYZTileExtraInfoFromCoverages(source, coverages, true);

        /* Assign tile option */
        tileOption = {
          statement: source.prepare(XYZ_INSERT_MD5_QUERY),
          created: Date.now(),
          sourcePath: storePath,
          format: metadata.format,
          storeTransparent: storeTransparent,
        };

        /* Store data function */
        storeTileDataFunc = async (z, x, y, data) =>
          await storeXYZTileFile(z, x, y, data, tileOption);

        /* Close database function */
        closeDatabaseFunc = async () => closeXYZMD5DB(source);

        break;
      }
    }

    switch (item.sourceType) {
      case "mbtiles": {
        /* Get tile extra info */
        if (refreshTimestamp === true) {
          try {
            printLog(
              "info",
              `Get target tile extra info from "${item.path}" and tile extra info from "${storePath}"...`,
            );

            targetTileExtraInfo = getMBTilesTileExtraInfoFromCoverages(
              item.source,
              coverages,
              false,
            );

            tileExtraInfo = getTileExtraInfo();
          } catch (error) {
            printLog(
              "error",
              `Failed to get target tile extra info from "${item.path}" and tile extra info from "${storePath}": ${error}`,
            );

            targetTileExtraInfo = {};
            tileExtraInfo = {};
          }
        } else if (refreshTimestamp) {
          try {
            printLog("info", `Get tile extra info from "${storePath}"...`);

            tileExtraInfo = getTileExtraInfo();
          } catch (error) {
            printLog(
              "error",
              `Failed to get tile extra info from "${storePath}": ${error}`,
            );

            tileExtraInfo = {};
          }
        }

        /* Get data function */
        getTileDataFunc = async (z, x, y) => {
          const tile = await getAndCacheMBTilesTileData(id, z, x, y);

          return tile.data;
        };

        break;
      }

      case "pg": {
        /* Get tile extra info */
        if (refreshTimestamp === true) {
          try {
            printLog(
              "info",
              `Get target tile extra info from "${item.path}" and tile extra info from "${storePath}"...`,
            );

            targetTileExtraInfo = getPostgreSQLTileExtraInfoFromCoverages(
              item.source,
              coverages,
              false,
            );

            tileExtraInfo = getTileExtraInfo();
          } catch (error) {
            printLog(
              "error",
              `Failed to get target tile extra info from "${item.path}" and tile extra info from "${storePath}": ${error}`,
            );

            targetTileExtraInfo = {};
            tileExtraInfo = {};
          }
        } else if (refreshTimestamp) {
          try {
            printLog("info", `Get tile extra info from "${storePath}"...`);

            tileExtraInfo = getTileExtraInfo();
          } catch (error) {
            printLog(
              "error",
              `Failed to get tile extra info from "${storePath}": ${error}`,
            );

            tileExtraInfo = {};
          }
        }

        /* Get data function */
        getTileDataFunc = async (z, x, y) => {
          const tile = await getAndCachePostgreSQLTileData(id, z, x, y);

          return tile.data;
        };

        break;
      }

      case "xyz": {
        /* Get tile extra info */
        if (refreshTimestamp === true) {
          try {
            printLog(
              "info",
              `Get target tile extra info from "${item.path}" and tile extra info from "${sqliteFilePath}"...`,
            );

            targetTileExtraInfo = getXYZTileExtraInfoFromCoverages(
              item.md5Source,
              coverages,
              false,
            );

            tileExtraInfo = getTileExtraInfo();
          } catch (error) {
            printLog(
              "error",
              `Failed to get target tile extra info from "${item.path}" and tile extra info from "${sqliteFilePath}": ${error}`,
            );

            targetTileExtraInfo = {};
            tileExtraInfo = {};
          }
        } else if (refreshTimestamp) {
          try {
            printLog("info", `Get tile extra info from "${sqliteFilePath}"...`);

            tileExtraInfo = getTileExtraInfo();
          } catch (error) {
            printLog(
              "error",
              `Failed to get tile extra info from "${sqliteFilePath}": ${error}`,
            );

            tileExtraInfo = {};
          }
        }

        /* Get data function */
        getTileDataFunc = async (z, x, y) => {
          const tile = await getAndCacheXYZTileData(id, z, x, y);

          return tile.data;
        };

        break;
      }
    }

    /* Export and store tile data generator */
    function* exportAndStoreTileDataGenerator() {
      let completeTasks = 0;

      for (const { z, x, y } of tileBounds) {
        for (let xCount = x[0]; xCount <= x[1]; xCount++) {
          for (let yCount = y[0]; yCount <= y[1]; yCount++) {
            completeTasks++;

            yield async () => {
              const tileName = `${z}/${xCount}/${yCount}`;

              if (
                (refreshTimestamp === true &&
                  tileExtraInfo[tileName] &&
                  tileExtraInfo[tileName] === targetTileExtraInfo[tileName]) ||
                (refreshTimestamp &&
                  tileExtraInfo[tileName] >= refreshTimestamp)
              ) {
                return;
              }

              printLog(
                "info",
                `Exporting data id "${id}" - Tile "${tileName}" - ${completeTasks}/${total}...`,
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
                  `Failed to export data id "${id}" - Tile "${tileName}" - ${completeTasks}/${total}: ${error}`,
                );
              }
            };
          }
        }
      }
    }

    /* Export and store tile datas */
    printLog("info", "Exporting and storing tile datas...");

    await runAllWithLimit(exportAndStoreTileDataGenerator(), concurrency, item);

    printLog(
      "info",
      `Completed export ${total} tiles of data id "${id}" to ${storeType} after ${
        (Date.now() - startTime) / 1000
      }s!`,
    );
  } catch (error) {
    printLog(
      "error",
      `Failed to export data id "${id}" to ${storeType} after ${
        (Date.now() - startTime) / 1000
      }s: ${error}`,
    );
  } finally {
    /* Close database */
    if (source && closeDatabaseFunc) {
      await closeDatabaseFunc();
    }
  }
}
