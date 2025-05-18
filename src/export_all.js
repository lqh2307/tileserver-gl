"use strict";

import { cacheStyleFile, getRenderedStyleJSON, getStyle } from "./style.js";
import { getAndCacheDataGeoJSON, getAndCacheDataSprite } from "./data.js";
import { cacheGeoJSONFile } from "./geojson.js";
import { cacheSpriteFile } from "./sprite.js";
import { printLog } from "./logger.js";
import { cp } from "node:fs/promises";
import { config } from "./config.js";
import {
  exportPostgreSQLTiles,
  exportMBTilesTiles,
  exportXYZTiles,
} from "./export_data.js";
import {
  createCoveragesFromBBoxAndZooms,
  createFileWithLock,
  createFolders,
  isLocalURL,
} from "./utils.js";

/**
 * Export all
 * @param {string} dirPath Exported dir path
 * @param {object} options Export options object
 * @param {number} concurrency Concurrency to download
 * @param {boolean} storeTransparent Is store transparent tile?
 * @param {string} parentServerHost Parent server host
 * @param {string} exportData Is export data?
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
    let log = `Exporting all with:`;
    log += `\n\tDirectory path: ${dirPath}`;
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
    if (options.styles === undefined) {
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
          timeout: 300000,
          maxTry: 5,
          skip: false,
        };

        if (exportData === true) {
          const styleBuffer = await getStyle(style.path);

          await cacheStyleFile(
            `${dirPath}/caches/styles/${styleFolder}/style.json`,
            styleBuffer
          );
        }

        // Get source
        const renderedStyleJSON = await getRenderedStyleJSON(style.path);

        for (const sourceName of Object.keys(renderedStyleJSON.sources)) {
          // Get geojson source
          const source = renderedStyleJSON.sources[sourceName];

          if (source.data !== undefined) {
            if (isLocalURL(source.data) === true) {
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
                timeout: 300000,
                maxTry: 5,
                skip: false,
              };

              if (exportData === true) {
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
              if (isLocalURL(tile) === true) {
                const dataID = tile.split("/")[2];

                const dataFolder = `${dataID}_cache`;

                const data = config.datas[dataID];

                const coverages = createCoveragesFromBBoxAndZooms(
                  data.tileJSON.bounds,
                  data.tileJSON.minzoom,
                  data.tileJSON.maxzoom
                );

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
                      timeout: 300000,
                      concurrency: 256,
                      maxTry: 5,
                      storeType: "xyz",
                      storeTransparent: true,
                      skip: false,
                    };

                    if (exportData === true) {
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
                      timeout: 300000,
                      concurrency: 256,
                      maxTry: 5,
                      storeType: "mbtiles",
                      storeTransparent: true,
                      skip: false,
                    };

                    if (exportData === true) {
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
                      timeout: 300000,
                      concurrency: 256,
                      maxTry: 5,
                      storeType: "pg",
                      storeTransparent: true,
                      skip: false,
                    };

                    if (exportData === true) {
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

          // Get sprite
          if (renderedStyleJSON.sprite.startsWith("sprites://") === true) {
            const spriteID = source.data.split("/")[2];

            const spriteFolder = `${spriteID}_cache`;

            configObj.sprites[spriteID] = {
              sprite: spriteFolder,
              cache: {
                store: true,
                forward: true,
              },
            };

            seedObj.sprites[spriteFolder] = {
              url: `${parentServerHost}/sptites/${spriteID}/{name}`,
              refreshBefore: {
                md5: true,
              },
              timeout: 300000,
              maxTry: 5,
              skip: false,
            };

            if (exportData === true) {
              const [spriteJSONBuffer, spritePNGBuffer] = await Promise.all([
                getAndCacheDataSprite(spriteID, "sprite.json"),
                getAndCacheDataSprite(spriteID, "sprite.png"),
              ]);

              await Promise.all([
                cacheSpriteFile(
                  `${dirPath}/caches/sprites/${spriteFolder}`,
                  spriteJSONBuffer
                ),
                cacheSpriteFile(
                  `${dirPath}/caches/sprites/${spriteFolder}`,
                  spritePNGBuffer
                ),
              ]);
            }
          }
        }
      }
    }

    // Export datas
    if (options.datas === undefined) {
      printLog("info", "No datas to export. Skipping...");
    } else {
      for (const dataID of options.datas) {
        // Get data
        const dataFolder = `${dataID}_cache`;

        const data = config.datas[dataID];

        const coverages = createCoveragesFromBBoxAndZooms(
          data.tileJSON.bounds,
          data.tileJSON.minzoom,
          data.tileJSON.maxzoom
        );

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
              timeout: 300000,
              concurrency: 256,
              maxTry: 5,
              storeType: "xyz",
              storeTransparent: true,
              skip: false,
            };

            if (exportData === true) {
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
              timeout: 300000,
              concurrency: 256,
              maxTry: 5,
              storeType: "mbtiles",
              storeTransparent: true,
              skip: false,
            };

            if (exportData === true) {
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
              timeout: 300000,
              concurrency: 256,
              maxTry: 5,
              storeType: "pg",
              storeTransparent: true,
              skip: false,
            };

            if (exportData === true) {
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
    if (options.geojsons === undefined) {
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
            timeout: 300000,
            maxTry: 5,
            skip: false,
          };

          if (exportData === true) {
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
    if (options.sprites === undefined) {
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
          url: `${parentServerHost}/sptites/${spriteID}/{name}`,
          refreshBefore: {
            md5: true,
          },
          timeout: 300000,
          maxTry: 5,
          skip: false,
        };

        if (exportData === true) {
          const [spriteJSONBuffer, spritePNGBuffer] = await Promise.all([
            getAndCacheDataSprite(spriteID, "sprite.json"),
            getAndCacheDataSprite(spriteID, "sprite.png"),
          ]);

          await Promise.all([
            cacheSpriteFile(
              `${dirPath}/caches/sprites/${spriteFolder}`,
              spriteJSONBuffer
            ),
            cacheSpriteFile(
              `${dirPath}/caches/sprites/${spriteFolder}`,
              spritePNGBuffer
            ),
          ]);
        }
      }
    }

    // Export fonts
    if (options.fonts === undefined) {
      printLog("info", "No fonts to export. Skipping...");
    } else {
      // Do nothing
    }

    // Copy all fonts
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
        timeout: 300000,
        concurrency: 256,
        maxTry: 5,
        skip: false,
      };

      if (exportData === true) {
        await cp(
          config.fonts[fontID].path,
          `${dirPath}/caches/fonts/${fontFolder}`
        );
      }
    }

    // Export config files
    await Promise.all([
      createFileWithLock(
        `${dirPath}/config.json`,
        JSON.stringify(configObj, null, 2),
        30000
      ),
      createFileWithLock(
        `${dirPath}/seed.json`,
        JSON.stringify(seedObj, null, 2),
        30000
      ),
      createFileWithLock(
        `${dirPath}/cleanup.json`,
        JSON.stringify(cleanUpObj, null, 2),
        30000
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
