"use strict";

import { printLog } from "./logger.js";
import {
  cleanUpPostgreSQLTiles,
  cleanUpMBTilesTiles,
  cleanUpXYZTiles,
  cleanUpGeoJSON,
  cleanUpSprite,
  cleanUpStyle,
  cleanUpFont,
  cleanUp,
} from "./cleanup.js";
import {
  seedPostgreSQLTiles,
  seedMBTilesTiles,
  seedXYZTiles,
  seedGeoJSON,
  seedSprite,
  seedStyle,
  seedFont,
  seed,
} from "./seed.js";
import os from "os";

/**
 * Run cleanup and seed tasks
 * @param {{ cleanUpSprites: boolean, cleanUpFonts: boolean, cleanUpStyles: boolean, cleanUpGeoJSONs: boolean, cleanUpDatas: boolean, seedSprites: boolean, seedFonts: boolean, seedStyles: boolean, seedGeoJSONs: boolean, seedDatas: boolean }} opts Options
 * @returns {Promise<void>}
 */
export async function runTasks(opts) {
  try {
    printLog("info", "Starting seed and cleanup tasks...");

    if (
      opts.cleanUpSprites ||
      opts.cleanUpFonts ||
      opts.cleanUpStyles ||
      opts.cleanUpGeoJSONs ||
      opts.cleanUpDatas ||
      opts.seedSprites ||
      opts.seedFonts ||
      opts.seedStyles ||
      opts.seedGeoJSONs ||
      opts.seedDatas
    ) {
      /* Cleanup sprites */
      if (opts.cleanUpSprites) {
        try {
          if (!cleanUp.sprites) {
            printLog("info", "No sprites in cleanup. Skipping...");
          } else {
            const ids = Object.keys(cleanUp.sprites);

            printLog("info", `Starting cleanup ${ids.length} sprites...`);

            const startTime = Date.now();

            for (const id of ids) {
              const item = cleanUp.sprites[id];

              if (item.skip) {
                printLog("info", `Skipping cleanup sprite "${id}"...`);

                continue;
              }

              try {
                await cleanUpSprite(
                  id,
                  item.cleanUpBefore?.time || item.cleanUpBefore?.day
                );
              } catch (error) {
                printLog(
                  "error",
                  `Failed to cleanup sprite "${id}": ${error}. Skipping...`
                );
              }
            }

            printLog(
              "info",
              `Completed cleanup ${ids.length} sprites after: ${
                (Date.now() - startTime) / 1000
              }s!`
            );
          }
        } catch (error) {
          printLog("error", `Failed to cleanup sprites: ${error}. Exited!`);
        }
      }

      /* Cleanup fonts */
      if (opts.cleanUpFonts) {
        try {
          if (!cleanUp.fonts) {
            printLog("info", "No fonts in cleanup. Skipping...");
          } else {
            const ids = Object.keys(cleanUp.fonts);

            printLog("info", `Starting cleanup ${ids.length} fonts...`);

            const startTime = Date.now();

            for (const id of ids) {
              const item = cleanUp.fonts[id];

              if (item.skip) {
                printLog("info", `Skipping cleanup font "${id}"...`);

                continue;
              }

              try {
                await cleanUpFont(
                  id,
                  item.cleanUpBefore?.time || item.cleanUpBefore?.day
                );
              } catch (error) {
                printLog(
                  "error",
                  `Failed to cleanup font "${id}": ${error}. Skipping...`
                );
              }
            }

            printLog(
              "info",
              `Completed cleanup ${ids.length} fonts after: ${
                (Date.now() - startTime) / 1000
              }s!`
            );
          }
        } catch (error) {
          printLog("error", `Failed to cleanup fonts: ${error}. Exited!`);
        }
      }

      /* Cleanup styles */
      if (opts.cleanUpStyles) {
        try {
          if (!cleanUp.styles) {
            printLog("info", "No styles in cleanup. Skipping...");
          } else {
            const ids = Object.keys(cleanUp.styles);

            printLog("info", `Starting cleanup ${ids.length} styles...`);

            const startTime = Date.now();

            for (const id of ids) {
              const item = cleanUp.styles[id];

              if (item.skip) {
                printLog("info", `Skipping cleanup style "${id}"...`);

                continue;
              }

              try {
                await cleanUpStyle(
                  id,
                  item.cleanUpBefore?.time || item.cleanUpBefore?.day
                );
              } catch (error) {
                printLog(
                  "error",
                  `Failed to cleanup style "${id}": ${error}. Skipping...`
                );
              }
            }

            printLog(
              "info",
              `Completed cleanup ${ids.length} styles after: ${
                (Date.now() - startTime) / 1000
              }s!`
            );
          }
        } catch (error) {
          printLog("error", `Failed to cleanup styles: ${error}. Exited!`);
        }
      }

      /* Cleanup geojsons */
      if (opts.cleanUpGeoJSONs) {
        try {
          if (!cleanUp.geojsons) {
            printLog("info", "No geojsons in cleanup. Skipping...");
          } else {
            const ids = Object.keys(cleanUp.geojsons);

            printLog("info", `Starting cleanup ${ids.length} geojsons...`);

            const startTime = Date.now();

            for (const id of ids) {
              const item = cleanUp.geojsons[id];

              if (item.skip) {
                printLog("info", `Skipping cleanup geojson "${id}"...`);

                continue;
              }

              try {
                await cleanUpGeoJSON(
                  id,
                  item.cleanUpBefore?.time || item.cleanUpBefore?.day
                );
              } catch (error) {
                printLog(
                  "error",
                  `Failed to cleanup geojson "${id}": ${error}. Skipping...`
                );
              }
            }

            printLog(
              "info",
              `Completed cleanup ${ids.length} geojsons after: ${
                (Date.now() - startTime) / 1000
              }s!`
            );
          }
        } catch (error) {
          printLog("error", `Failed to cleanup geojsons: ${error}. Exited!`);
        }
      }

      /* Cleanup datas */
      if (opts.cleanUpDatas) {
        try {
          if (!cleanUp.datas) {
            printLog("info", "No datas in cleanup. Skipping...");
          } else {
            const ids = Object.keys(cleanUp.datas);

            printLog("info", `Starting cleanup ${ids.length} datas...`);

            const startTime = Date.now();

            for (const id of ids) {
              const seedDataItem = seed.datas[id];
              const cleanUpDataItem = cleanUp.datas[id];

              if (cleanUpDataItem.skip) {
                printLog("info", `Skipping cleanup data "${id}"...`);

                continue;
              }

              try {
                switch (seedDataItem.storeType) {
                  case "xyz": {
                    await cleanUpXYZTiles(
                      id,
                      seedDataItem.metadata.format,
                      cleanUpDataItem.coverages,
                      cleanUpDataItem.cleanUpBefore?.time ||
                        cleanUpDataItem.cleanUpBefore?.day
                    );

                    break;
                  }

                  case "mbtiles": {
                    await cleanUpMBTilesTiles(
                      id,
                      cleanUpDataItem.coverages,
                      cleanUpDataItem.cleanUpBefore?.time ||
                        cleanUpDataItem.cleanUpBefore?.day
                    );

                    break;
                  }

                  case "pg": {
                    await cleanUpPostgreSQLTiles(
                      id,
                      cleanUpDataItem.coverages,
                      cleanUpDataItem.cleanUpBefore?.time ||
                        cleanUpDataItem.cleanUpBefore?.day
                    );

                    break;
                  }
                }
              } catch (error) {
                printLog(
                  "error",
                  `Failed to cleanup data "${id}": ${error}. Skipping...`
                );
              }
            }

            printLog(
              "info",
              `Completed cleanup ${ids.length} datas after: ${
                (Date.now() - startTime) / 1000
              }s!`
            );
          }
        } catch (error) {
          printLog("error", `Failed to cleanup datas: ${error}. Exited!`);
        }
      }

      /* Run seed sprites */
      if (opts.seedSprites) {
        try {
          if (!seed.sprites) {
            printLog("info", "No sprites in seed. Skipping...");
          } else {
            const ids = Object.keys(seed.sprites);

            printLog("info", `Starting seed ${ids.length} sprites...`);

            const startTime = Date.now();

            for (const id of ids) {
              const item = seed.sprites[id];

              if (item.skip) {
                printLog("info", `Skipping seed font "${id}"...`);

                continue;
              }

              try {
                await seedSprite(
                  id,
                  item.url,
                  item.maxTry || 5,
                  item.timeout ?? 60000,
                  item.refreshBefore?.time ||
                    item.refreshBefore?.day ||
                    item.refreshBefore?.md5,
                  item.headers
                );
              } catch (error) {
                printLog(
                  "error",
                  `Failed to seed font "${id}": ${error}. Skipping...`
                );
              }
            }

            printLog(
              "info",
              `Completed seed ${ids.length} sprites after: ${
                (Date.now() - startTime) / 1000
              }s!`
            );
          }
        } catch (error) {
          printLog("error", `Failed to seed sprites: ${error}. Exited!`);
        }
      }

      /* Run seed fonts */
      if (opts.seedFonts) {
        try {
          if (!seed.fonts) {
            printLog("info", "No fonts in seed. Skipping...");
          } else {
            const ids = Object.keys(seed.fonts);

            printLog("info", `Starting seed ${ids.length} fonts...`);

            const startTime = Date.now();

            for (const id of ids) {
              const item = seed.fonts[id];

              if (item.skip) {
                printLog("info", `Skipping seed font "${id}"...`);

                continue;
              }

              try {
                await seedFont(
                  id,
                  item.url,
                  item.concurrency || os.cpus().length,
                  item.maxTry || 5,
                  item.timeout ?? 60000,
                  item.refreshBefore?.time ||
                    item.refreshBefore?.day ||
                    item.refreshBefore?.md5,
                  item.headers
                );
              } catch (error) {
                printLog(
                  "error",
                  `Failed to seed font "${id}": ${error}. Skipping...`
                );
              }
            }

            printLog(
              "info",
              `Completed seed ${ids.length} fonts after: ${
                (Date.now() - startTime) / 1000
              }s!`
            );
          }
        } catch (error) {
          printLog("error", `Failed to seed fonts: ${error}. Exited!`);
        }
      }

      /* Run seed styles */
      if (opts.seedStyles) {
        try {
          if (!seed.styles) {
            printLog("info", "No styles in seed. Skipping...");
          } else {
            const ids = Object.keys(seed.styles);

            printLog("info", `Starting seed ${ids.length} styles...`);

            const startTime = Date.now();

            for (const id of ids) {
              const item = seed.styles[id];

              if (item.skip) {
                printLog("info", `Skipping seed style "${id}"...`);

                continue;
              }

              try {
                await seedStyle(
                  id,
                  item.url,
                  item.maxTry || 5,
                  item.timeout ?? 60000,
                  item.refreshBefore?.time ||
                    item.refreshBefore?.day ||
                    item.refreshBefore?.md5,
                  item.headers
                );
              } catch (error) {
                printLog(
                  "error",
                  `Failed to seed style "${id}": ${error}. Skipping...`
                );
              }
            }

            printLog(
              "info",
              `Completed seed ${ids.length} styles after: ${
                (Date.now() - startTime) / 1000
              }s!`
            );
          }
        } catch (error) {
          printLog("error", `Failed to seed styles: ${error}. Exited!`);
        }
      }

      /* Run seed geojsons */
      if (opts.seedGeoJSONs) {
        try {
          if (!seed.geojsons) {
            printLog("info", "No geojsons in seed. Skipping...");
          } else {
            const ids = Object.keys(seed.geojsons);

            printLog("info", `Starting seed ${ids.length} geojsons...`);

            const startTime = Date.now();

            for (const id of ids) {
              const item = seed.geojsons[id];

              if (item.skip) {
                printLog("info", `Skipping seed geojson "${id}"...`);

                continue;
              }

              try {
                await seedGeoJSON(
                  id,
                  item.url,
                  item.maxTry || 5,
                  item.timeout ?? 60000,
                  item.refreshBefore?.time ||
                    item.refreshBefore?.day ||
                    item.refreshBefore?.md5,
                  item.headers
                );
              } catch (error) {
                printLog(
                  "error",
                  `Failed to seed geojson "${id}": ${error}. Skipping...`
                );
              }
            }

            printLog(
              "info",
              `Completed seed ${ids.length} geojsons after: ${
                (Date.now() - startTime) / 1000
              }s!`
            );
          }
        } catch (error) {
          printLog("error", `Failed to seed geojsons: ${error}. Exited!`);
        }
      }

      /* Run seed datas */
      if (opts.seedDatas) {
        try {
          if (!seed.datas) {
            printLog("info", "No datas in seed. Skipping...");
          } else {
            const ids = Object.keys(seed.datas);

            printLog("info", `Starting seed ${ids.length} datas...`);

            const startTime = Date.now();

            for (const id of ids) {
              const item = seed.datas[id];

              if (item.skip) {
                printLog("info", `Skipping seed data "${id}"...`);

                continue;
              }

              try {
                switch (item.storeType) {
                  case "xyz": {
                    await seedXYZTiles(
                      id,
                      item.metadata,
                      item.url,
                      item.scheme,
                      item.coverages,
                      item.concurrency || os.cpus().length,
                      item.maxTry || 5,
                      item.timeout ?? 60000,
                      item.storeTransparent ?? true,
                      item.refreshBefore?.time ||
                        item.refreshBefore?.day ||
                        item.refreshBefore?.md5,
                      item.headers
                    );

                    break;
                  }

                  case "mbtiles": {
                    await seedMBTilesTiles(
                      id,
                      item.metadata,
                      item.url,
                      item.scheme,
                      item.coverages,
                      item.concurrency || os.cpus().length,
                      item.maxTry || 5,
                      item.timeout ?? 60000,
                      item.storeTransparent ?? true,
                      item.refreshBefore?.time ||
                        item.refreshBefore?.day ||
                        item.refreshBefore?.md5,
                      item.headers
                    );

                    break;
                  }

                  case "pg": {
                    await seedPostgreSQLTiles(
                      id,
                      item.metadata,
                      item.url,
                      item.scheme,
                      item.coverages,
                      item.concurrency || os.cpus().length,
                      item.maxTry || 5,
                      item.timeout ?? 60000,
                      item.storeTransparent ?? true,
                      item.refreshBefore?.time ||
                        item.refreshBefore?.day ||
                        item.refreshBefore?.md5,
                      item.headers
                    );

                    break;
                  }
                }
              } catch (error) {
                printLog(
                  "error",
                  `Failed to seed data "${id}": ${error}. Skipping...`
                );
              }
            }

            printLog(
              "info",
              `Completed seed ${ids.length} datas after: ${
                (Date.now() - startTime) / 1000
              }s!`
            );
          }
        } catch (error) {
          printLog("error", `Failed to seed datas: ${error}. Exited!`);
        }
      }
    } else {
      printLog("info", "No task assigned. Skipping...");
    }
  } catch (error) {
    printLog("error", `Failed to run tasks: ${error}`);
  } finally {
    printLog("info", "Completed seed and cleanup tasks!");
  }
}
