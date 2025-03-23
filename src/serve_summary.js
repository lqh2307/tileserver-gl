"use strict";

import { countPostgreSQLTiles, getPostgreSQLSize } from "./tile_postgresql.js";
import { countMBTilesTiles, getMBTilesSize } from "./tile_mbtiles.js";
import { getTilesBoundsFromCoverages, isExistFolder } from "./utils.js";
import { countXYZTiles, getXYZSize } from "./tile_xyz.js";
import { getPMTilesSize } from "./tile_pmtiles.js";
import { StatusCodes } from "http-status-codes";
import { getGeoJSONSize } from "./geojson.js";
import { getSpriteSize } from "./sprite.js";
import { getStyleSize } from "./style.js";
import { getFontSize } from "./font.js";
import { printLog } from "./logger.js";
import { config } from "./config.js";
import { seed } from "./seed.js";
import express from "express";

/**
 * Get summary handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function serveSummaryHandler() {
  return async (req, res, next) => {
    try {
      let result;

      if (req.query.type === "seed") {
        result = {
          styles: {},
          geojsons: {},
          datas: {},
          sprites: {},
          fonts: {},
        };

        await Promise.all([
          ...Object.keys(seed.styles || {}).map(async (id) => {
            if (
              (await isExistFolder(
                `${process.env.DATA_DIR}/caches/styles/${id}`
              )) === true
            ) {
              result.styles[id] = {
                actual: 1,
                expect: 1,
              };
            } else {
              result.styles[id] = {
                actual: 0,
                expect: 1,
              };
            }
          }),
          ...Object.keys(seed.geojsons || {}).map(async (id) => {
            if (
              (await isExistFolder(
                `${process.env.DATA_DIR}/caches/geojsons/${id}`
              )) === true
            ) {
              result.geojsons[id] = {
                actual: 1,
                expect: 1,
              };
            } else {
              result.geojsons[id] = {
                actual: 0,
                expect: 1,
              };
            }
          }),
          ...Object.keys(seed.datas || {}).map(async (id) => {
            const item = seed.datas[id];

            switch (item.storeType) {
              case "mbtiles": {
                try {
                  result.datas[id] = {
                    actual: await countMBTilesTiles(
                      `${process.env.DATA_DIR}/caches/mbtiles/${id}/${id}.mbtiles`
                    ),
                    expect: getTilesBoundsFromCoverages(
                      item.coverages,
                      item.scheme
                    ).grandTotal,
                  };
                } catch (error) {
                  if (error.code !== "ENOENT") {
                    throw error;
                  } else {
                    result.datas[id] = {
                      actual: 0,
                      expect: getTilesBoundsFromCoverages(
                        item.coverages,
                        item.scheme
                      ).grandTotal,
                    };
                  }
                }

                break;
              }

              case "xyz": {
                try {
                  result.datas[id] = {
                    actual: await countXYZTiles(
                      `${process.env.DATA_DIR}/caches/xyzs/${id}`
                    ),
                    expect: getTilesBoundsFromCoverages(
                      item.coverages,
                      item.scheme
                    ).grandTotal,
                  };
                } catch (error) {
                  if (error.code !== "ENOENT") {
                    throw error;
                  } else {
                    result.datas[id] = {
                      actual: 0,
                      expect: getTilesBoundsFromCoverages(
                        item.coverages,
                        item.scheme
                      ).grandTotal,
                    };
                  }
                }

                break;
              }

              case "pg": {
                result.datas[id] = {
                  actual: await countPostgreSQLTiles(
                    `${process.env.POSTGRESQL_BASE_URI}/${id}`
                  ),
                  expect: getTilesBoundsFromCoverages(
                    item.coverages,
                    item.scheme
                  ).grandTotal,
                };

                break;
              }
            }
          }),
          ...Object.keys(seed.sprites || {}).map(async (id) => {
            if (
              (await isExistFolder(
                `${process.env.DATA_DIR}/caches/sprites/${id}`
              )) === true
            ) {
              result.sprites[id] = {
                actual: 1,
                expect: 1,
              };
            } else {
              result.sprites[id] = {
                actual: 0,
                expect: 1,
              };
            }
          }),
          ...Object.keys(seed.fonts || {}).map(async (id) => {
            if (
              (await isExistFolder(
                `${process.env.DATA_DIR}/caches/fonts/${id}`
              )) === true
            ) {
              result.fonts[id] = {
                actual: 1,
                expect: 1,
              };
            } else {
              result.fonts[id] = {
                actual: 0,
                expect: 1,
              };
            }
          }),
        ]);
      } else {
        result = {
          styles: {
            count: 0,
            size: 0,
            rendereds: {
              count: 0,
            },
          },
          geojsonGroups: {
            count: 0,
            geojsons: {
              count: 0,
              size: 0,
            },
          },
          datas: {
            count: 0,
            size: 0,
            mbtiles: {
              count: 0,
              size: 0,
            },
            pmtiles: {
              count: 0,
              size: 0,
            },
            xyzs: {
              count: 0,
              size: 0,
            },
            pgs: {
              count: 0,
              size: 0,
            },
          },
          sprites: {
            count: 0,
            size: 0,
          },
          fonts: {
            count: 0,
            size: 0,
          },
        };

        await Promise.all([
          ...Object.keys(config.repo.styles).map(async (id) => {
            const item = config.repo.styles[id];

            try {
              result.styles.size += await getStyleSize(item.path);
            } catch (error) {
              if (!(item.cache !== undefined && error.code === "ENOENT")) {
                throw error;
              }
            }

            result.styles.count += 1;

            // Rendereds info
            if (item.rendered !== undefined) {
              result.styles.rendereds.count += 1;
            }
          }),
          ...Object.keys(config.repo.geojsons).map(async (id) => {
            for (const layer in config.repo.geojsons[id]) {
              const item = config.repo.geojsons[id][layer];

              try {
                result.geojsonGroups.geojsons.size += await getGeoJSONSize(
                  item.path
                );
              } catch (error) {
                if (!(item.cache !== undefined && error.code === "ENOENT")) {
                  throw error;
                }
              }

              result.geojsonGroups.geojsons.count += 1;
            }

            result.geojsonGroups.count += 1;
          }),
          ...Object.keys(config.repo.datas).map(async (id) => {
            const item = config.repo.datas[id];

            switch (item.sourceType) {
              case "mbtiles": {
                try {
                  result.datas.mbtiles.size += await getMBTilesSize(item.path);
                } catch (error) {
                  if (!(item.cache !== undefined && error.code === "ENOENT")) {
                    throw error;
                  }
                }

                result.datas.mbtiles.count += 1;

                break;
              }

              case "pmtiles": {
                if (
                  item.path.startsWith("https://") !== true &&
                  item.path.startsWith("http://") !== true
                ) {
                  result.datas.pmtiles.size += await getPMTilesSize(item.path);
                }

                result.datas.pmtiles.count += 1;

                break;
              }

              case "xyz": {
                try {
                  result.datas.xyzs.size += await getXYZSize(item.path);
                } catch (error) {
                  if (!(item.cache !== undefined && error.code === "ENOENT")) {
                    throw error;
                  }
                }

                result.datas.xyzs.count += 1;

                break;
              }

              case "pg": {
                result.datas.pgs.size += await getPostgreSQLSize(
                  item.source,
                  id
                );
                result.datas.pgs.count += 1;

                break;
              }
            }
          }),
          ...Object.keys(config.repo.sprites).map(async (id) => {
            const item = config.repo.sprites[id];

            try {
              result.sprites.size += await getSpriteSize(item.path);
            } catch (error) {
              if (!(item.cache !== undefined && error.code === "ENOENT")) {
                throw error;
              }
            }

            result.sprites.count += 1;
          }),
          ...Object.keys(config.repo.fonts).map(async (id) => {
            const item = config.repo.fonts[id];

            try {
              result.fonts.size += await getFontSize(item.path);
            } catch (error) {
              if (!(item.cache !== undefined && error.code === "ENOENT")) {
                throw error;
              }
            }

            result.fonts.count += 1;
          }),
        ]);

        result.datas.count =
          result.datas.mbtiles.count +
          result.datas.pmtiles.count +
          result.datas.xyzs.count +
          result.datas.pgs.count;
        result.datas.size =
          result.datas.mbtiles.size +
          result.datas.pmtiles.size +
          result.datas.xyzs.size +
          result.datas.pgs.size;
      }

      res.header("content-type", "application/json");

      return res.status(StatusCodes.OK).send(result);
    } catch (error) {
      printLog("error", `Failed to get summary: ${error}`);

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

export const serve_summary = {
  init: () => {
    const app = express().disable("x-powered-by");

    /**
     * @swagger
     * tags:
     *   - name: Summary
     *     description: Summary related endpoints
     * /summary:
     *   get:
     *     tags:
     *       - Summary
     *     summary: Get summary
     *     parameters:
     *       - in: query
     *         name: type
     *         schema:
     *           type: string
     *           enum: [service, seed]
     *           example: service
     *         required: false
     *         description: Summary type
     *     responses:
     *       200:
     *         description: Summary
     *         content:
     *           application/json:
     *             schema:
     *               type: object
     *       404:
     *         description: Not found
     *       503:
     *         description: Server is starting up
     *         content:
     *           text/plain:
     *             schema:
     *               type: string
     *               example: Starting...
     *       500:
     *         description: Internal server error
     */
    app.get("/", serveSummaryHandler());

    return app;
  },
};
