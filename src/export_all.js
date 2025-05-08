"use strict";

import { cacheStyleFile, getRenderedStyleJSON, getStyle } from "./style.js";
import { getAndCacheDataGeoJSON, getAndCacheDataSprite } from "./data.js";
import { isLocalURL, createCoveragesFromBBoxAndZooms } from "./utils.js";
import { exportXYZTiles } from "./export_data.js";
import { cacheGeoJSONFile } from "./geojson.js";
import { cacheSpriteFile } from "./sprite.js";
import { printLog } from "./logger.js";
import { cp } from "node:fs/promises";
import { config } from "./config.js";

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
        const style = config.styles[styleID];

        const [styleBuffer, renderedStyleJSON] = await Promise.all([
          getStyle(style.path),
          getRenderedStyleJSON(style.path),
        ]);

        await cacheStyleFile(
          `${dirPath}/caches/styles/${styleID}_cache/style.json`,
          styleBuffer
        );

        configObj.styles[styleID] = {
          style: `${styleID}_cache`,
          cache: {
            store: true,
            forward: true,
          },
        };

        seedObj.styles[`${styleID}_cache`] = {
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

              const geoJSONBuffer = await getAndCacheDataGeoJSON(
                parts[2],
                parts[3]
              );

              await cacheGeoJSONFile(
                `${dirPath}/caches/geojsons/${parts[3]}_cache/${parts[3]}_cache.geojson`,
                geoJSONBuffer
              );
            }
          }

          // Get tile source
          if (source.tiles !== undefined) {
            for (const tile of source.tiles) {
              if (isLocalURL(tile) === true) {
                const dataID = tile.split("/")[2];
                const dataItem = config.datas[dataID];

                switch (dataItem.sourceType) {
                  case "xyz": {
                    await exportXYZTiles(
                      dataID,
                      `${dirPath}/caches/datas/xyzs/${dataID}_cache`,
                      `${dirPath}/caches/datas/xyzs/${dataID}_cache/${dataID}_cache.sqlite`,
                      dataItem.tileJSON,
                      createCoveragesFromBBoxAndZooms(
                        dataItem.tileJSON.bounds,
                        dataItem.tileJSON.minzoom,
                        dataItem.tileJSON.maxzoom
                      ),
                      256,
                      true,
                      undefined
                    );

                    configObj.datas[dataID] = {
                      xyz: `${dataID}_cache`,
                      cache: {
                        store: true,
                        forward: true,
                      },
                    };

                    break;
                  }

                  case "mbtiles": {
                    await exportMBTilesTiles(
                      dataID,
                      `${dirPath}/caches/datas/mbtiles/${dataID}_cache/${dataID}_cache.mbtiles`,
                      dataItem.tileJSON,
                      createCoveragesFromBBoxAndZooms(
                        dataItem.tileJSON.bounds,
                        dataItem.tileJSON.minzoom,
                        dataItem.tileJSON.maxzoom
                      ),
                      256,
                      true,
                      undefined
                    );

                    configObj.datas[dataID] = {
                      mbtiles: `${dataID}_cache`,
                      cache: {
                        store: true,
                        forward: true,
                      },
                    };

                    break;
                  }

                  case "pg": {
                    await exportPostgreSQLTiles(
                      dataID,
                      `${process.env.POSTGRESQL_BASE_URI}/${dataID}_cache`,
                      dataItem.tileJSON,
                      createCoveragesFromBBoxAndZooms(
                        dataItem.tileJSON.bounds,
                        dataItem.tileJSON.minzoom,
                        dataItem.tileJSON.maxzoom
                      ),
                      256,
                      true,
                      undefined
                    );

                    configObj.datas[dataID] = {
                      pg: `${dataID}_cache`,
                      cache: {
                        store: true,
                        forward: true,
                      },
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

            const [spriteJSONBuffer, spritePNGBuffer] = await Promise.all([
              getAndCacheDataSprite(spriteID, "sprite.json"),
              getAndCacheDataSprite(spriteID, "sprite.png"),
            ]);

            await Promise.all([
              cacheSpriteFile(
                `${dirPath}/caches/sprites/${spriteID}_cache`,
                spriteJSONBuffer
              ),
              cacheSpriteFile(
                `${dirPath}/caches/sprites/${spriteID}_cache`,
                spritePNGBuffer
              ),
            ]);
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
        const dataItem = config.datas[dataID];

        switch (dataItem.sourceType) {
          case "xyz": {
            await exportXYZTiles(
              dataID,
              `${dirPath}/caches/datas/xyzs/${dataID}_cache`,
              `${dirPath}/caches/datas/xyzs/${dataID}_cache/${dataID}_cache.sqlite`,
              dataItem.tileJSON,
              createCoveragesFromBBoxAndZooms(
                dataItem.tileJSON.bounds,
                dataItem.tileJSON.minzoom,
                dataItem.tileJSON.maxzoom
              ),
              256,
              true,
              undefined
            );

            configObj.datas[dataID] = {
              xyz: `${dataID}_cache`,
              cache: {
                store: true,
                forward: true,
              },
            };

            break;
          }

          case "mbtiles": {
            await exportMBTilesTiles(
              sourceID,
              `${dataID}/caches/datas/mbtiles/${dataID}_cache/${dataID}_cache.mbtiles`,
              createCoveragesFromBBoxAndZooms(
                dataItem.tileJSON.bounds,
                dataItem.tileJSON.minzoom,
                dataItem.tileJSON.maxzoom
              ),
              req.body.coverages,
              256,
              true,
              undefined
            );

            configObj.datas[dataID] = {
              mbtiles: `${dataID}_cache`,
              cache: {
                store: true,
                forward: true,
              },
            };

            break;
          }

          case "pg": {
            await exportPostgreSQLTiles(
              dataID,
              `${process.env.POSTGRESQL_BASE_URI}/${dataID}_cache`,
              dataItem.tileJSON,
              createCoveragesFromBBoxAndZooms(
                dataItem.tileJSON.bounds,
                dataItem.tileJSON.minzoom,
                dataItem.tileJSON.maxzoom
              ),
              256,
              true,
              undefined
            );

            configObj.datas[dataID] = {
              pg: `${dataID}_cache`,
              cache: {
                store: true,
                forward: true,
              },
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
          const geoJSONBuffer = await getAndCacheDataGeoJSON(group, layer);

          await cacheGeoJSONFile(
            `${dirPath}/caches/geojsons/${layer}_cache/${layer}_cache.geojson`,
            geoJSONBuffer
          );

          configObj.geojsons[group][layer] = {
            geojson: `${layer}_cache`,
            cache: {
              store: true,
              forward: true,
            },
          };

          seedObj.geojsons[`${layer}_cache`] = {
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
        const [spriteJSONBuffer, spritePNGBuffer] = await Promise.all([
          getAndCacheDataSprite(spriteID, "sprite.json"),
          getAndCacheDataSprite(spriteID, "sprite.png"),
        ]);

        await Promise.all([
          cacheSpriteFile(
            `${dirPath}/caches/sprites/${spriteID}_cache`,
            spriteJSONBuffer
          ),
          cacheSpriteFile(
            `${dirPath}/caches/sprites/${spriteID}_cache`,
            spritePNGBuffer
          ),
        ]);

        configObj.sprites[spriteID] = {
          sprite: `${spriteID}_cache`,
          cache: {
            store: true,
            forward: true,
          },
        };

        seedObj.sprites[`${spriteID}_cache`] = {
          url: `http://localhost:8080/sptites/spriteID`,
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

    for (const fontID of Object.keys(config.fonts)) {
      await cp(
        config.fonts[fontID].path,
        `${dirPath}/caches/fonts/${fontID}_cache`
      );

      configObj.fonts[fontID] = {
        font: `${fontID}_cache`,
        cache: {
          store: true,
          forward: true,
        },
      };

      seedObj.fonts[`${fontID}_cache`] = {
        url: `http://localhost:8080/fonts/${fontID}/{range}.pbf`,
        refreshBefore: "2025-01-01T00:00:00",
        timeout: 300000,
        concurrency: 100,
        maxTry: 5,
        skip: false,
      };
    }

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
