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
  isLocalURL,
} from "./utils.js";

/**
 * Export all
 * @param {string} dirPath Exported dir path
 * @param {object} options Export options object
 * @returns {Promise<void>}
 */
export async function exportAll(dirPath, options) {
  const startTime = Date.now();

  try {
    let log = `Exporting all with:`;
    log += `\n\tDirectory path: ${dirPath}`;
    log += `\n\tOptions: ${JSON.stringify(options)}`;

    printLog("info", log);

    // Create config object
    const configObj = {
      options: {
        listenPort: options.listenPort || 8080,
        serveFrontPage: options.serveFrontPage || true,
        serveSwagger: options.serveSwagger || true,
        taskSchedule: options.taskSchedule,
        postgreSQLBaseURI: options.postgreSQLBaseURI,
        process: options.process || 2,
        thread: options.thread || 16,
      },
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

        const [styleBuffer, renderedStyleJSON] = await Promise.all([
          getStyle(style.path),
          getRenderedStyleJSON(style.path),
        ]);

        await cacheStyleFile(
          `${dirPath}/caches/styles/${styleFolder}/style.json`,
          styleBuffer
        );

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
          url: `http://localhost:8080/styles/${styleID}/style.json`,
          refreshBefore: "2025-01-01T00:00:00",
          timeout: 300000,
          maxTry: 5,
          skip: false,
        };

        // Get source
        for (const sourceName of Object.keys(renderedStyleJSON.sources)) {
          // Get geojson source
          const source = renderedStyleJSON.sources[sourceName];

          if (source.data !== undefined) {
            if (isLocalURL(source.data) === true) {
              const parts = source.data.split("/");

              const geojsonFolder = `${parts[3]}_cache`;

              const geoJSONBuffer = await getAndCacheDataGeoJSON(
                parts[2],
                parts[3]
              );

              await cacheGeoJSONFile(
                `${dirPath}/caches/geojsons/${geojsonFolder}/${geojsonFolder}.geojson`,
                geoJSONBuffer
              );

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
                url: `http://localhost:8080/geojsons/${parts[2]}/${parts[3]}.geojson`,
                refreshBefore: "2025-01-01T00:00:00",
                timeout: 300000,
                maxTry: 5,
                skip: false,
              };
            }
          }

          // Get tile source
          if (source.tiles !== undefined) {
            for (const tile of source.tiles) {
              if (isLocalURL(tile) === true) {
                const dataID = tile.split("/")[2];

                const dataFolder = `${dataID}_cache`;

                const dataItem = config.datas[dataID];

                switch (dataItem.sourceType) {
                  case "xyz": {
                    const coverages = createCoveragesFromBBoxAndZooms(
                      dataItem.tileJSON.bounds,
                      dataItem.tileJSON.minzoom,
                      dataItem.tileJSON.maxzoom
                    );

                    await exportXYZTiles(
                      dataID,
                      `${dirPath}/caches/datas/xyzs/${dataFolder}`,
                      `${dirPath}/caches/datas/xyzs/${dataFolder}/${dataFolder}.sqlite`,
                      dataItem.tileJSON,
                      coverages,
                      256,
                      true,
                      undefined
                    );

                    configObj.datas[dataID] = {
                      xyz: dataFolder,
                      cache: {
                        store: true,
                        forward: true,
                      },
                    };

                    seedObj.datas[dataFolder] = {
                      metadata: dataItem.tileJSON,
                      url: `http://localhost:8080/datas/${dataID}/{z}/{x}/{y}.${dataItem.tileJSON.format}`,
                      scheme: "xyz",
                      skip: false,
                      refreshBefore: "2025-01-01T00:00:00",
                      coverages: coverages,
                      timeout: 300000,
                      concurrency: 256,
                      maxTry: 5,
                      storeType: "xyz",
                      storeTransparent: true,
                    };

                    break;
                  }

                  case "mbtiles": {
                    const coverages = createCoveragesFromBBoxAndZooms(
                      dataItem.tileJSON.bounds,
                      dataItem.tileJSON.minzoom,
                      dataItem.tileJSON.maxzoom
                    );

                    await exportMBTilesTiles(
                      dataID,
                      `${dirPath}/caches/datas/mbtiles/${dataFolder}/${dataFolder}.mbtiles`,
                      dataItem.tileJSON,
                      coverages,
                      256,
                      true,
                      undefined
                    );

                    configObj.datas[dataID] = {
                      mbtiles: dataFolder,
                      cache: {
                        store: true,
                        forward: true,
                      },
                    };

                    seedObj.datas[dataFolder] = {
                      metadata: dataItem.tileJSON,
                      url: `http://localhost:8080/datas/${dataID}/{z}/{x}/{y}.${dataItem.tileJSON.format}`,
                      scheme: "xyz",
                      skip: false,
                      refreshBefore: "2025-01-01T00:00:00",
                      coverages: coverages,
                      timeout: 300000,
                      concurrency: 256,
                      maxTry: 5,
                      storeType: "mbtiles",
                      storeTransparent: true,
                    };

                    break;
                  }

                  case "pg": {
                    const coverages = createCoveragesFromBBoxAndZooms(
                      dataItem.tileJSON.bounds,
                      dataItem.tileJSON.minzoom,
                      dataItem.tileJSON.maxzoom
                    );

                    await exportPostgreSQLTiles(
                      dataID,
                      `${process.env.POSTGRESQL_BASE_URI}/${dataFolder}`,
                      dataItem.tileJSON,
                      coverages,
                      256,
                      true,
                      undefined
                    );

                    configObj.datas[dataID] = {
                      pg: dataFolder,
                      cache: {
                        store: true,
                        forward: true,
                      },
                    };

                    seedObj.datas[dataFolder] = {
                      metadata: dataItem.tileJSON,
                      url: `http://localhost:8080/datas/${dataID}/{z}/{x}/{y}.${dataItem.tileJSON.format}`,
                      scheme: "xyz",
                      skip: false,
                      refreshBefore: "2025-01-01T00:00:00",
                      coverages: coverages,
                      timeout: 300000,
                      concurrency: 256,
                      maxTry: 5,
                      storeType: "pg",
                      storeTransparent: true,
                    };

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

            configObj.sprites[spriteID] = {
              sprite: spriteFolder,
              cache: {
                store: true,
                forward: true,
              },
            };

            seedObj.sprites[spriteFolder] = {
              url: `http://localhost:8080/sptites/${spriteID}/{name}`,
              refreshBefore: "2025-01-01T00:00:00",
              timeout: 300000,
              maxTry: 5,
              skip: false,
            };
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

        const dataItem = config.datas[dataID];

        switch (dataItem.sourceType) {
          case "xyz": {
            const coverages = createCoveragesFromBBoxAndZooms(
              dataItem.tileJSON.bounds,
              dataItem.tileJSON.minzoom,
              dataItem.tileJSON.maxzoom
            );

            await exportXYZTiles(
              dataID,
              `${dirPath}/caches/datas/xyzs/${dataFolder}`,
              `${dirPath}/caches/datas/xyzs/${dataFolder}/${dataFolder}.sqlite`,
              dataItem.tileJSON,
              coverages,
              256,
              true,
              undefined
            );

            configObj.datas[dataID] = {
              xyz: dataFolder,
              cache: {
                store: true,
                forward: true,
              },
            };

            seedObj.datas[dataFolder] = {
              metadata: dataItem.tileJSON,
              url: `http://localhost:8080/datas/${dataID}/{z}/{x}/{y}.${dataItem.tileJSON.format}`,
              scheme: "xyz",
              skip: false,
              refreshBefore: "2025-01-01T00:00:00",
              coverages: coverages,
              timeout: 300000,
              concurrency: 256,
              maxTry: 5,
              storeType: "xyz",
              storeTransparent: true,
            };

            break;
          }

          case "mbtiles": {
            const coverages = createCoveragesFromBBoxAndZooms(
              dataItem.tileJSON.bounds,
              dataItem.tileJSON.minzoom,
              dataItem.tileJSON.maxzoom
            );

            await exportMBTilesTiles(
              sourceID,
              `${dirPath}/caches/datas/mbtiles/${dataFolder}/${dataFolder}.mbtiles`,
              dataItem.tileJSON,
              coverages,
              256,
              true,
              undefined
            );

            configObj.datas[dataID] = {
              mbtiles: dataFolder,
              cache: {
                store: true,
                forward: true,
              },
            };

            seedObj.datas[dataFolder] = {
              metadata: dataItem.tileJSON,
              url: `http://localhost:8080/datas/${dataID}/{z}/{x}/{y}.${dataItem.tileJSON.format}`,
              scheme: "xyz",
              skip: false,
              refreshBefore: "2025-01-01T00:00:00",
              coverages: coverages,
              timeout: 300000,
              concurrency: 256,
              maxTry: 5,
              storeType: "mbtiles",
              storeTransparent: true,
            };

            break;
          }

          case "pg": {
            const coverages = createCoveragesFromBBoxAndZooms(
              dataItem.tileJSON.bounds,
              dataItem.tileJSON.minzoom,
              dataItem.tileJSON.maxzoom
            );

            await exportPostgreSQLTiles(
              dataID,
              `${process.env.POSTGRESQL_BASE_URI}/${dataFolder}`,
              dataItem.tileJSON,
              coverages,
              256,
              true,
              undefined
            );

            configObj.datas[dataID] = {
              pg: dataFolder,
              cache: {
                store: true,
                forward: true,
              },
            };

            seedObj.datas[dataFolder] = {
              metadata: dataItem.tileJSON,
              url: `http://localhost:8080/datas/${dataID}/{z}/{x}/{y}.${dataItem.tileJSON.format}`,
              scheme: "xyz",
              skip: false,
              refreshBefore: "2025-01-01T00:00:00",
              coverages: coverages,
              timeout: 300000,
              concurrency: 256,
              maxTry: 5,
              storeType: "pg",
              storeTransparent: true,
            };

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

          const geoJSONBuffer = await getAndCacheDataGeoJSON(group, layer);

          await cacheGeoJSONFile(
            `${dirPath}/caches/geojsons/${geojsonFolder}/${geojsonFolder}.geojson`,
            geoJSONBuffer
          );

          configObj.geojsons[group][layer] = {
            geojson: geojsonFolder,
            cache: {
              store: true,
              forward: true,
            },
          };

          seedObj.geojsons[geojsonFolder] = {
            url: `http://localhost:8080/geojsons/${group}/${layer}.geojson`,
            refreshBefore: "2025-01-01T00:00:00",
            timeout: 300000,
            maxTry: 5,
            skip: false,
          };
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

        configObj.sprites[spriteID] = {
          sprite: spriteFolder,
          cache: {
            store: true,
            forward: true,
          },
        };

        seedObj.sprites[spriteFolder] = {
          url: `http://localhost:8080/sptites/${spriteID}/{name}`,
          refreshBefore: "2025-01-01T00:00:00",
          timeout: 300000,
          maxTry: 5,
          skip: false,
        };
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

      await cp(
        config.fonts[fontID].path,
        `${dirPath}/caches/fonts/${fontFolder}`
      );

      configObj.fonts[fontID] = {
        font: fontFolder,
        cache: {
          store: true,
          forward: true,
        },
      };

      seedObj.fonts[fontFolder] = {
        url: `http://localhost:8080/fonts/${fontID}/{range}.pbf`,
        refreshBefore: "2025-01-01T00:00:00",
        timeout: 300000,
        concurrency: 256,
        maxTry: 5,
        skip: false,
      };
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
