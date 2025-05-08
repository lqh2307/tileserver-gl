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
 * Run clean up and seed tasks
 * @param {{ cleanUpSprites: boolean, cleanUpFonts: boolean, cleanUpStyles: boolean, cleanUpGeoJSONs: boolean, cleanUpDatas: boolean, seedSprites: boolean, seedFonts: boolean, seedStyles: boolean, seedGeoJSONs: boolean, seedDatas: boolean }} opts Options
 * @returns {Promise<void>}
 */
export async function runTasks(opts) {
  try {
    printLog("info", "Starting seed and clean up tasks...");

    if (
      opts.cleanUpSprites === true ||
      opts.cleanUpFonts === true ||
      opts.cleanUpStyles === true ||
      opts.cleanUpGeoJSONs === true ||
      opts.cleanUpDatas === true ||
      opts.seedSprites === true ||
      opts.seedFonts === true ||
      opts.seedStyles === true ||
      opts.seedGeoJSONs === true ||
      opts.seedDatas === true
    ) {
      /* Clean up sprites */
      if (opts.cleanUpSprites === true) {
        try {
          if (cleanUp.sprites === undefined) {
            printLog("info", "No sprites in cleanup. Skipping...");
          } else {
            const ids = Object.keys(cleanUp.sprites);

            printLog("info", `Starting clean up ${ids.length} sprites...`);

            const startTime = Date.now();

            for (const id of ids) {
              const cleanUpSpriteItem = cleanUp.sprites[id];

              if (cleanUpSpriteItem.skip === true) {
                printLog("info", `Skipping clean up sprite "${id}"...`);

                continue;
              }

              try {
                await cleanUpSprite(
                  id,
                  cleanUpSpriteItem.cleanUpBefore?.time ||
                    cleanUpSpriteItem.cleanUpBefore?.day
                );
              } catch (error) {
                printLog(
                  "error",
                  `Failed to clean up sprite "${id}": ${error}. Skipping...`
                );
              }
            }

            printLog(
              "info",
              `Completed clean up ${ids.length} sprites after: ${
                (Date.now() - startTime) / 1000
              }s!`
            );
          }
        } catch (error) {
          printLog("error", `Failed to clean up sprites: ${error}. Exited!`);
        }
      }

      /* Clean up fonts */
      if (opts.cleanUpFonts === true) {
        try {
          if (cleanUp.fonts === undefined) {
            printLog("info", "No fonts in cleanup. Skipping...");
          } else {
            const ids = Object.keys(cleanUp.fonts);

            printLog("info", `Starting clean up ${ids.length} fonts...`);

            const startTime = Date.now();

            for (const id of ids) {
              const cleanUpFontItem = cleanUp.fonts[id];

              if (cleanUpFontItem.skip === true) {
                printLog("info", `Skipping clean up font "${id}"...`);

                continue;
              }

              try {
                await cleanUpFont(
                  id,
                  cleanUpFontItem.cleanUpBefore?.time ||
                    cleanUpFontItem.cleanUpBefore?.day
                );
              } catch (error) {
                printLog(
                  "error",
                  `Failed to clean up font "${id}": ${error}. Skipping...`
                );
              }
            }

            printLog(
              "info",
              `Completed clean up ${ids.length} fonts after: ${
                (Date.now() - startTime) / 1000
              }s!`
            );
          }
        } catch (error) {
          printLog("error", `Failed to clean up fonts: ${error}. Exited!`);
        }
      }

      /* Clean up styles */
      if (opts.cleanUpStyles === true) {
        try {
          if (cleanUp.styles === undefined) {
            printLog("info", "No styles in cleanup. Skipping...");
          } else {
            const ids = Object.keys(cleanUp.styles);

            printLog("info", `Starting clean up ${ids.length} styles...`);

            const startTime = Date.now();

            for (const id of ids) {
              const cleanUpStyleItem = cleanUp.styles[id];

              if (cleanUpStyleItem.skip === true) {
                printLog("info", `Skipping clean up style "${id}"...`);

                continue;
              }

              try {
                await cleanUpStyle(
                  id,
                  cleanUpStyleItem.cleanUpBefore?.time ||
                    cleanUpStyleItem.cleanUpBefore?.day
                );
              } catch (error) {
                printLog(
                  "error",
                  `Failed to clean up style "${id}": ${error}. Skipping...`
                );
              }
            }

            printLog(
              "info",
              `Completed clean up ${ids.length} styles after: ${
                (Date.now() - startTime) / 1000
              }s!`
            );
          }
        } catch (error) {
          printLog("error", `Failed to clean up styles: ${error}. Exited!`);
        }
      }

      /* Clean up geojsons */
      if (opts.cleanUpGeoJSONs === true) {
        try {
          if (cleanUp.geojsons === undefined) {
            printLog("info", "No geojsons in cleanup. Skipping...");
          } else {
            const ids = Object.keys(cleanUp.geojsons);

            printLog("info", `Starting clean up ${ids.length} geojsons...`);

            const startTime = Date.now();

            for (const id of ids) {
              const cleanUpGeoJSONItem = cleanUp.geojsons[id];

              if (cleanUpGeoJSONItem.skip === true) {
                printLog("info", `Skipping clean up geojson "${id}"...`);

                continue;
              }

              try {
                await cleanUpGeoJSON(
                  id,
                  cleanUpGeoJSONItem.cleanUpBefore?.time ||
                    cleanUpGeoJSONItem.cleanUpBefore?.day
                );
              } catch (error) {
                printLog(
                  "error",
                  `Failed to clean up geojson "${id}": ${error}. Skipping...`
                );
              }
            }

            printLog(
              "info",
              `Completed clean up ${ids.length} geojsons after: ${
                (Date.now() - startTime) / 1000
              }s!`
            );
          }
        } catch (error) {
          printLog("error", `Failed to clean up geojsons: ${error}. Exited!`);
        }
      }

      /* Clean up datas */
      if (opts.cleanUpDatas === true) {
        try {
          if (cleanUp.datas === undefined) {
            printLog("info", "No datas in cleanup. Skipping...");
          } else {
            const ids = Object.keys(cleanUp.datas);

            printLog("info", `Starting clean up ${ids.length} datas...`);

            const startTime = Date.now();

            for (const id of ids) {
              const seedDataItem = seed.datas[id];
              const cleanUpDataItem = cleanUp.datas[id];

              if (cleanUpDataItem.skip === true) {
                printLog("info", `Skipping clean up data "${id}"...`);

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
                  `Failed to clean up data "${id}": ${error}. Skipping...`
                );
              }
            }

            printLog(
              "info",
              `Completed clean up ${ids.length} datas after: ${
                (Date.now() - startTime) / 1000
              }s!`
            );
          }
        } catch (error) {
          printLog("error", `Failed to clean up datas: ${error}. Exited!`);
        }
      }

      /* Run seed sprites */
      if (opts.seedSprites === true) {
        try {
          if (seed.sprites === undefined) {
            printLog("info", "No sprites in seed. Skipping...");
          } else {
            const ids = Object.keys(seed.sprites);

            printLog("info", `Starting seed ${ids.length} sprites...`);

            const startTime = Date.now();

            for (const id of ids) {
              const seedSpriteItem = seed.sprites[id];

              if (seedSpriteItem.skip === true) {
                printLog("info", `Skipping seed font "${id}"...`);

                continue;
              }

              try {
                await seedSprite(
                  id,
                  seedSpriteItem.url,
                  seedSpriteItem.maxTry || 5,
                  seedSpriteItem.timeout || 60000,
                  seedSpriteItem.refreshBefore?.time ||
                    seedSpriteItem.refreshBefore?.day
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
      if (opts.seedFonts === true) {
        try {
          if (seed.fonts === undefined) {
            printLog("info", "No fonts in seed. Skipping...");
          } else {
            const ids = Object.keys(seed.fonts);

            printLog("info", `Starting seed ${ids.length} fonts...`);

            const startTime = Date.now();

            for (const id of ids) {
              const seedFontItem = seed.fonts[id];

              if (seedFontItem.skip === true) {
                printLog("info", `Skipping seed font "${id}"...`);

                continue;
              }

              try {
                await seedFont(
                  id,
                  seedFontItem.url,
                  seedFontItem.concurrency || os.cpus().length,
                  seedFontItem.maxTry || 5,
                  seedFontItem.timeout || 60000,
                  seedFontItem.refreshBefore?.time ||
                    seedFontItem.refreshBefore?.day
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
      if (opts.seedStyles === true) {
        try {
          if (seed.styles === undefined) {
            printLog("info", "No styles in seed. Skipping...");
          } else {
            const ids = Object.keys(seed.styles);

            printLog("info", `Starting seed ${ids.length} styles...`);

            const startTime = Date.now();

            for (const id of ids) {
              const seedStyleItem = seed.styles[id];

              if (seedStyleItem.skip === true) {
                printLog("info", `Skipping seed style "${id}"...`);

                continue;
              }

              try {
                await seedStyle(
                  id,
                  seedStyleItem.url,
                  seedStyleItem.maxTry || 5,
                  seedStyleItem.timeout || 60000,
                  seedStyleItem.refreshBefore?.time ||
                    seedStyleItem.refreshBefore?.day
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
      if (opts.seedGeoJSONs === true) {
        try {
          if (seed.geojsons === undefined) {
            printLog("info", "No geojsons in seed. Skipping...");
          } else {
            const ids = Object.keys(seed.geojsons);

            printLog("info", `Starting seed ${ids.length} geojsons...`);

            const startTime = Date.now();

            for (const id of ids) {
              const seedGeoJSONItem = seed.geojsons[id];

              if (seedGeoJSONItem.skip === true) {
                printLog("info", `Skipping seed geojson "${id}"...`);

                continue;
              }

              try {
                await seedGeoJSON(
                  id,
                  seedGeoJSONItem.url,
                  seedGeoJSONItem.maxTry || 5,
                  seedGeoJSONItem.timeout || 60000,
                  seedGeoJSONItem.refreshBefore?.time ||
                    seedGeoJSONItem.refreshBefore?.day ||
                    seedGeoJSONItem.refreshBefore?.md5
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
      if (opts.seedDatas === true) {
        try {
          if (seed.datas === undefined) {
            printLog("info", "No datas in seed. Skipping...");
          } else {
            const ids = Object.keys(seed.datas);

            printLog("info", `Starting seed ${ids.length} datas...`);

            const startTime = Date.now();

            for (const id of ids) {
              const seedDataItem = seed.datas[id];

              if (seedDataItem.skip === true) {
                printLog("info", `Skipping seed data "${id}"...`);

                continue;
              }

              try {
                switch (seedDataItem.storeType) {
                  case "xyz": {
                    await seedXYZTiles(
                      id,
                      seedDataItem.metadata,
                      seedDataItem.url,
                      seedDataItem.scheme,
                      seedDataItem.coverages,
                      seedDataItem.concurrency || os.cpus().length,
                      seedDataItem.maxTry || 5,
                      seedDataItem.timeout || 60000,
                      seedDataItem.storeTransparent || true,
                      seedDataItem.refreshBefore?.time ||
                        seedDataItem.refreshBefore?.day ||
                        seedDataItem.refreshBefore?.md5
                    );

                    break;
                  }

                  case "mbtiles": {
                    await seedMBTilesTiles(
                      id,
                      seedDataItem.metadata,
                      seedDataItem.url,
                      seedDataItem.scheme,
                      seedDataItem.coverages,
                      seedDataItem.concurrency || os.cpus().length,
                      seedDataItem.maxTry || 5,
                      seedDataItem.timeout || 60000,
                      seedDataItem.storeTransparent || true,
                      seedDataItem.refreshBefore?.time ||
                        seedDataItem.refreshBefore?.day ||
                        seedDataItem.refreshBefore?.md5
                    );

                    break;
                  }

                  case "pg": {
                    await seedPostgreSQLTiles(
                      id,
                      seedDataItem.metadata,
                      seedDataItem.url,
                      seedDataItem.scheme,
                      seedDataItem.coverages,
                      seedDataItem.concurrency || os.cpus().length,
                      seedDataItem.maxTry || 5,
                      seedDataItem.timeout || 60000,
                      seedDataItem.storeTransparent || true,
                      seedDataItem.refreshBefore?.time ||
                        seedDataItem.refreshBefore?.day ||
                        seedDataItem.refreshBefore?.md5
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
    printLog("info", "Completed seed and clean up tasks!");
  }
}
