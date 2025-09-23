"use strict";

import { config } from "./configs/index.js";
import {
  handleTilesConcurrency,
  createFileWithLock,
  removeEmptyFolders,
  createFolders,
  getTileBounds,
  isLocalURL,
  printLog,
} from "./utils/index.js";
import {
  getPostgreSQLTileExtraInfoFromCoverages,
  getMBTilesTileExtraInfoFromCoverages,
  getXYZTileExtraInfoFromCoverages,
  getAndCachePostgreSQLDataTile,
  getAndCacheMBTilesDataTile,
  updatePostgreSQLMetadata,
  cachePostgreSQLTileData,
  getAndCacheXYZDataTile,
  getAndCacheDataGeoJSON,
  updateMBTilesMetadata,
  getAndCacheDataSprite,
  getAndCacheDataFonts,
  getRenderedStyleJSON,
  cacheMBtilesTileData,
  updateXYZMetadata,
  closePostgreSQLDB,
  openPostgreSQLDB,
  cacheXYZTileFile,
  cacheGeoJSONFile,
  cacheSpriteFile,
  cacheStyleFile,
  cacheFontFile,
  closeMBTilesDB,
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
  refreshBefore
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

          await cacheStyleFile(
            `${dirPath}/caches/styles/${styleFolder}/style.json`,
            styleBuffer
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
              await Promise.all(
                Array.from({ length: 256 }, async (_, i) => {
                  const fileName = `${i * 256}-${i * 256 + 255}.pbf`;

                  const fontBuffer = await getAndCacheDataFonts(
                    fontID,
                    fileName
                  );

                  await cacheFontFile(
                    `${dirPath}/caches/fonts/${fontFolder}/${fileName}`,
                    fontBuffer
                  );
                })
              );
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
              cacheSpriteFile(
                `${dirPath}/caches/sprites/${spriteFolder}/sprite.json`,
                spriteJSONBuffer
              ),
              cacheSpriteFile(
                `${dirPath}/caches/sprites/${spriteFolder}/sprite.png`,
                spritePNGBuffer
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
                  parts[3]
                );

                await cacheGeoJSONFile(
                  `${dirPath}/caches/geojsons/${geojsonFolder}/${geojsonFolder}.geojson`,
                  geoJSONBuffer
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
                      await exportXYZTiles(
                        dataID,
                        `${dirPath}/caches/datas/xyzs/${dataFolder}`,
                        `${dirPath}/caches/datas/xyzs/${dataFolder}/${dataFolder}.sqlite`,
                        data.tileJSON,
                        coverages,
                        concurrency,
                        storeTransparent,
                        refreshBefore
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
                      await exportMBTilesTiles(
                        dataID,
                        `${dirPath}/caches/datas/mbtiles/${dataFolder}/${dataFolder}.mbtiles`,
                        data.tileJSON,
                        coverages,
                        concurrency,
                        storeTransparent,
                        refreshBefore
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
                      await exportPostgreSQLTiles(
                        dataID,
                        `${process.env.POSTGRESQL_BASE_URI}/${dataFolder}`,
                        data.tileJSON,
                        coverages,
                        concurrency,
                        storeTransparent,
                        refreshBefore
                      );
                    }

                    break;
                  }
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
              await exportXYZTiles(
                dataID,
                `${dirPath}/caches/datas/xyzs/${dataFolder}`,
                `${dirPath}/caches/datas/xyzs/${dataFolder}/${dataFolder}.sqlite`,
                data.tileJSON,
                coverages,
                concurrency,
                storeTransparent,
                refreshBefore
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
              await exportMBTilesTiles(
                dataID,
                `${dirPath}/caches/datas/mbtiles/${dataFolder}/${dataFolder}.mbtiles`,
                data.tileJSON,
                coverages,
                concurrency,
                storeTransparent,
                refreshBefore
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
              await exportPostgreSQLTiles(
                dataID,
                `${process.env.POSTGRESQL_BASE_URI}/${dataFolder}`,
                data.tileJSON,
                coverages,
                concurrency,
                storeTransparent,
                refreshBefore
              );
            }

            break;
          }
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

            await cacheGeoJSONFile(
              `${dirPath}/caches/geojsons/${geojsonFolder}/${geojsonFolder}.geojson`,
              geoJSONBuffer
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
            cacheSpriteFile(
              `${dirPath}/caches/sprites/${spriteFolder}/sprite.json`,
              spriteJSONBuffer
            ),
            cacheSpriteFile(
              `${dirPath}/caches/sprites/${spriteFolder}/sprite.png`,
              spritePNGBuffer
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
          await Promise.all(
            Array.from({ length: 256 }, async (_, i) => {
              const fileName = `${i * 256}-${i * 256 + 255}.pbf`;

              const fontBuffer = await getAndCacheDataFonts(fontID, fileName);

              await cacheFontFile(
                `${dirPath}/caches/fonts/${fontFolder}/${fileName}`,
                fontBuffer
              );
            })
          );
        }
      }
    }

    // Export config files
    await Promise.all([
      createFileWithLock(
        `${dirPath}/config.json`,
        JSON.stringify(configObj, null, 2),
        timeout
      ),
      createFileWithLock(
        `${dirPath}/seed.json`,
        JSON.stringify(seedObj, null, 2),
        timeout
      ),
      createFileWithLock(
        `${dirPath}/cleanup.json`,
        JSON.stringify(cleanUpObj, null, 2),
        timeout
      ),
    ]);

    printLog(
      "info",
      `Completed all after ${(Date.now() - startTime) / 1000}s!`
    );
  } catch (error) {
    printLog(
      "error",
      `Failed to export all after ${(Date.now() - startTime) / 1000}s: ${error}`
    );
  }
}

/**
 * Export MBTiles tiles
 * @param {string} id Style ID
 * @param {string} filePath Exported file path
 * @param {object} metadata Metadata object
 * @param {{ zoom: number, bbox: [number, number, number, number]}[]} coverages Specific coverages
 * @param {number} concurrency Concurrency
 * @param {boolean} storeTransparent Is store transparent tile?
 * @param {string|number|boolean} refreshBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be refreshed/Compare MD5
 * @returns {Promise<void>}
 */
export async function exportMBTilesTiles(
  id,
  filePath,
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
    const { realBBox, total, tileBounds } = getTileBounds({
      coverages: coverages,
    });

    let log = `Exporting ${total} tiles of data "${id}" to mbtiles with:`;
    log += `\n\tFile path: ${filePath}`;
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

    /* Open MBTiles SQLite database */
    const item = config.datas[id];

    source = await openMBTilesDB(
      filePath,
      true,
      30000 // 30 secs
    );

    /* Get tile extra info */
    let targetTileExtraInfo;
    let tileExtraInfo;

    if (refreshTimestamp === true) {
      try {
        printLog(
          "info",
          `Get target tile extra info from "${item.path}" and tile extra info from "${filePath}"...`
        );

        targetTileExtraInfo = getMBTilesTileExtraInfoFromCoverages(
          item.source,
          coverages,
          false
        );

        tileExtraInfo = getMBTilesTileExtraInfoFromCoverages(
          source,
          coverages,
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
        bounds: realBBox,
      },
      30000 // 30 secs
    );

    /* Export tiles */
    async function exportMBTilesTileData(z, x, y, tasks) {
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

      printLog(
        "info",
        `Exporting data "${id}" - Tile "${tileName}" - ${completeTasks}/${total}...`
      );

      try {
        // Export data
        const dataTile = await getAndCacheMBTilesDataTile(id, z, x, y);

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

    // Export tiles with concurrency
    printLog("info", "Exporting datas...");

    await handleTilesConcurrency(
      concurrency,
      exportMBTilesTileData,
      tileBounds,
      item
    );

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
    // Close MBTiles SQLite database
    if (source) {
      closeMBTilesDB(source);
    }
  }
}

/**
 * Export XYZ tiles
 * @param {string} id Style ID
 * @param {string} sourcePath Exported source path
 * @param {string} filePath Exported file path
 * @param {object} metadata Metadata object
 * @param {{ zoom: number, bbox: [number, number, number, number]}[]} coverages Specific coverages
 * @param {number} concurrency Concurrency
 * @param {boolean} storeTransparent Is store transparent tile?
 * @param {string|number|boolean} refreshBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be refreshed/Compare MD5
 * @returns {Promise<void>}
 */
export async function exportXYZTiles(
  id,
  sourcePath,
  filePath,
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
    const { realBBox, total, tileBounds } = getTileBounds({
      coverages: coverages,
    });

    let log = `Exporting ${total} tiles of data "${id}" to xyz with:`;
    log += `\n\tSource path: ${sourcePath}`;
    log += `\n\tFile path: ${filePath}`;
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

    /* Open MD5 SQLite database */
    const item = config.datas[id];

    const source = await openXYZMD5DB(
      filePath,
      true,
      30000 // 30 secs
    );

    /* Get tile extra info */
    let targetTileExtraInfo;
    let tileExtraInfo;

    if (refreshTimestamp === true) {
      try {
        printLog(
          "info",
          `Get target tile extra info from "${item.path}" and tile extra info from "${filePath}"...`
        );

        targetTileExtraInfo = getXYZTileExtraInfoFromCoverages(
          item.md5Source,
          coverages,
          false
        );

        tileExtraInfo = getXYZTileExtraInfoFromCoverages(
          source,
          coverages,
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

    /* Update XYZ metadata */
    printLog("info", "Updating XYZ metadata...");

    await updateXYZMetadata(
      source,
      {
        ...metadata,
        bounds: realBBox,
      },
      30000 // 30 secs
    );

    /* Export tile files */
    async function exportXYZTileData(z, x, y, tasks) {
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

      printLog(
        "info",
        `Exporting data "${id}" - Tile "${tileName}" - ${completeTasks}/${total}...`
      );

      try {
        // Export data
        const dataTile = await getAndCacheXYZDataTile(id, z, x, y);

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

    printLog("info", "Exporting datas...");

    await handleTilesConcurrency(
      concurrency,
      exportXYZTileData,
      tileBounds,
      item
    );

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
    /* Close MD5 SQLite database */
    if (source) {
      closeXYZMD5DB(source);
    }
  }
}

/**
 * Export PostgreSQL tiles
 * @param {string} id Style ID
 * @param {string} filePath Exported file path
 * @param {object} metadata Metadata object
 * @param {{ zoom: number, bbox: [number, number, number, number]}[]} coverages Specific coverages
 * @param {number} concurrency Concurrency
 * @param {boolean} storeTransparent Is store transparent tile?
 * @param {string|number|boolean} refreshBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be refreshed/Compare MD5
 * @returns {Promise<void>}
 */
export async function exportPostgreSQLTiles(
  id,
  filePath,
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
    const { realBBox, total, tileBounds } = getTileBounds({
      coverages: coverages,
    });

    let log = `Exporting ${total} tiles of data "${id}" to postgresql with:`;
    log += `\n\tFile path: ${filePath}`;
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

    /* Open PostgreSQL database */
    const item = config.datas[id];

    source = await openPostgreSQLDB(filePath, true);

    /* Get tile extra info */
    let targetTileExtraInfo;
    let tileExtraInfo;

    if (refreshTimestamp === true) {
      try {
        printLog(
          "info",
          `Get target tile extra info from "${item.path}" and tile extra info from "${filePath}"...`
        );

        targetTileExtraInfo = getPostgreSQLTileExtraInfoFromCoverages(
          item.source,
          coverages,
          false
        );

        tileExtraInfo = getPostgreSQLTileExtraInfoFromCoverages(
          source,
          coverages,
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

    /* Update PostgreSQL metadata */
    printLog("info", "Updating PostgreSQL metadata...");

    await updatePostgreSQLMetadata(source, {
      ...metadata,
      bounds: realBBox,
    });

    /* Export tiles */
    async function exportPostgreSQLTileData(z, x, y, tasks) {
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

      printLog(
        "info",
        `Exporting data "${id}" - Tile "${tileName}" - ${completeTasks}/${total}...`
      );

      try {
        // Export data
        const dataTile = await getAndCachePostgreSQLDataTile(id, z, x, y);

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

    printLog("info", "Exporting datas...");

    await handleTilesConcurrency(
      concurrency,
      exportPostgreSQLTileData,
      tileBounds,
      item
    );

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
    /* Close PostgreSQL database */
    if (source) {
      closePostgreSQLDB(source);
    }
  }
}
