"use strict";

import { StatusCodes } from "http-status-codes";
import { printLog } from "./logger.js";
import { createReadStream } from "fs";
import { config } from "./config.js";
import { stat } from "fs/promises";
import { seed } from "./seed.js";
import path from "path";
import os from "os";
import {
  getXYZTileExtraInfoFromCoverages,
  calculatXYZTileExtraInfo,
  getXYZMetadata,
  openXYZMD5DB,
} from "./tile_xyz.js";
import {
  getMBTilesTileExtraInfoFromCoverages,
  calculateMBTilesTileExtraInfo,
  getMBTilesMetadata,
  openMBTilesDB,
} from "./tile_mbtiles.js";
import {
  createTileMetadataFromTemplate,
  calculateMD5OfFile,
  processCoverages,
  compileTemplate,
  getRequestHost,
  getJSONSchema,
  validateJSON,
  isExistFile,
  gzipAsync,
} from "./utils.js";
import {
  getPMTilesMetadata,
  getPMTilesTile,
  openPMTiles,
} from "./tile_pmtiles.js";
import {
  getPostgreSQLTileExtraInfoFromCoverages,
  calculatePostgreSQLTileExtraInfo,
  getPostgreSQLMetadata,
  openPostgreSQLDB,
} from "./tile_postgresql.js";
import {
  exportPostgreSQLTiles,
  exportMBTilesTiles,
  exportXYZTiles,
} from "./export_data.js";
import {
  getAndCachePostgreSQLDataTile,
  getAndCacheMBTilesDataTile,
  getAndCacheXYZDataTile,
  validateTileMetadata,
} from "./data.js";

/**
 * Serve data handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function serveDataHandler() {
  return async (req, res, next) => {
    const id = req.params.id;

    try {
      const item = config.datas[id];

      if (item === undefined) {
        return res.status(StatusCodes.NOT_FOUND).send("Data does not exist");
      }

      const compiled = await compileTemplate(
        item.tileJSON.format === "pbf" ? "vector_data" : "raster_data",
        {
          id: id,
          name: item.tileJSON.name,
          base_url: getRequestHost(req),
        }
      );

      return res.status(StatusCodes.OK).send(compiled);
    } catch (error) {
      printLog("error", `Failed to serve data "${id}": ${error}`);

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

/**
 * Get data tile handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function getDataTileHandler() {
  return async (req, res, next) => {
    const id = req.params.id;
    const item = config.datas[id];

    /* Check data is exist? */
    if (item === undefined) {
      return res.status(StatusCodes.NOT_FOUND).send("Data does not exist");
    }

    /* Check data tile format */
    if (
      req.params.format !== item.tileJSON.format ||
      ["jpeg", "jpg", "pbf", "png", "webp", "gif"].includes(
        req.params.format
      ) === false
    ) {
      return res
        .status(StatusCodes.BAD_REQUEST)
        .send("Data tile format is not support");
    }

    /* Get tile name */
    const z = Number(req.params.z);
    const x = Number(req.params.x);
    const y = Number(req.params.y);
    const tileName = `${z}/${x}/${y}`;

    /* Get and cache tile data */
    try {
      let dataTile;

      switch (item.sourceType) {
        case "mbtiles": {
          dataTile = await getAndCacheMBTilesDataTile(id, z, x, y);

          break;
        }

        case "pmtiles": {
          dataTile = await getPMTilesTile(item.source, z, x, y);

          break;
        }

        case "xyz": {
          dataTile = await getAndCacheXYZDataTile(id, z, x, y);

          break;
        }

        case "pg": {
          dataTile = await getAndCachePostgreSQLDataTile(id, z, x, y);

          break;
        }
      }

      /* Gzip pbf data tile */
      if (
        dataTile.headers["content-type"] === "application/x-protobuf" &&
        dataTile.headers["content-encoding"] === undefined
      ) {
        dataTile.data = await gzipAsync(dataTile.data);

        dataTile.headers["content-encoding"] = "gzip";
      }

      res.set(dataTile.headers);

      return res.status(StatusCodes.OK).send(dataTile.data);
    } catch (error) {
      printLog(
        "error",
        `Failed to get data "${id}" - Tile "${tileName}": ${error}`
      );

      if (error.message === "Tile does not exist") {
        return res.status(StatusCodes.NO_CONTENT).send(error.message);
      } else {
        return res
          .status(StatusCodes.INTERNAL_SERVER_ERROR)
          .send("Internal server error");
      }
    }
  };
}

/**
 * Get data tileJSON handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function getDataHandler() {
  return async (req, res, next) => {
    const id = req.params.id;

    try {
      const item = config.datas[id];

      if (item === undefined) {
        return res.status(StatusCodes.NOT_FOUND).send("Data does not exist");
      }

      const requestHost = getRequestHost(req);

      res.header("content-type", "application/json");

      return res.status(StatusCodes.OK).send({
        ...item.tileJSON,
        tilejson: "2.2.0",
        scheme: "xyz",
        id: id,
        tiles: [
          `${requestHost}/datas/${id}/{z}/{x}/{y}.${item.tileJSON.format}`,
        ],
      });
    } catch (error) {
      printLog("error", `Failed to get data "${id}": ${error}`);

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

/**
 * Export data handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function exportDataHandler() {
  return async (req, res, next) => {
    const id = req.params.id;

    try {
      const item = config.datas[id];

      if (item === undefined) {
        return res.status(StatusCodes.NOT_FOUND).send("Data does not exist");
      }

      if (req.query.cancel === "true") {
        /* Check export is not running? (export === true is not running) */
        if (item.export === true) {
          printLog(
            "warn",
            "No export is currently running. Skipping cancel export..."
          );

          return res.status(StatusCodes.NOT_FOUND).send("OK");
        } else {
          printLog("info", "Canceling export...");

          item.export = true;

          return res.status(StatusCodes.OK).send("OK");
        }
      } else {
        /* Check export is running? (export === false is not running) */
        if (item.export === false) {
          printLog("warn", "A export is already running. Skipping export...");

          return res.status(StatusCodes.CONFLICT).send("OK");
        } else {
          /* Export data */
          try {
            validateJSON(await getJSONSchema("data_export"), req.body);
          } catch (error) {
            return res
              .status(StatusCodes.BAD_REQUEST)
              .send(`Options is invalid: ${error}`);
          }

          item.export = false;

          const refreshBefore =
            req.body.refreshBefore?.time ||
            req.body.refreshBefore?.day ||
            req.body.refreshBefore?.md5;

          switch (req.body.storeType) {
            case "xyz": {
              exportXYZTiles(
                id,
                `${process.env.DATA_DIR}/exports/datas/xyzs/${req.body.id}`,
                `${process.env.DATA_DIR}/exports/datas/xyzs/${req.body.id}/${req.body.id}.sqlite`,
                req.body.metadata,
                req.body.coverages,
                req.body.concurrency || os.cpus().length,
                req.body.storeTransparent ?? true,
                refreshBefore
              )
                .catch((error) => {
                  printLog("error", `Failed to export data "${id}": ${error}`);
                })
                .finally(() => {
                  item.export = true;
                });

              break;
            }

            case "mbtiles": {
              exportMBTilesTiles(
                id,
                `${process.env.DATA_DIR}/exports/datas/mbtiles/${req.body.id}/${req.body.id}.mbtiles`,
                req.body.metadata,
                req.body.coverages,
                req.body.concurrency || os.cpus().length,
                req.body.storeTransparent ?? true,
                refreshBefore
              )
                .catch((error) => {
                  printLog("error", `Failed to export data "${id}": ${error}`);
                })
                .finally(() => {
                  item.export = true;
                });

              break;
            }

            case "pg": {
              exportPostgreSQLTiles(
                id,
                `${process.env.POSTGRESQL_BASE_URI}/${req.body.id}`,
                req.body.metadata,
                req.body.coverages,
                req.body.concurrency || os.cpus().length,
                req.body.storeTransparent ?? true,
                refreshBefore
              )
                .catch((error) => {
                  printLog("error", `Failed to export data "${id}": ${error}`);
                })
                .finally(() => {
                  item.export = true;
                });

              break;
            }
          }

          return res.status(StatusCodes.CREATED).send("OK");
        }
      }
    } catch (error) {
      printLog("error", `Failed to export data "${id}": ${error}`);

      if (error instanceof SyntaxError) {
        return res
          .status(StatusCodes.BAD_REQUEST)
          .send("Options parameter is invalid");
      } else {
        return res
          .status(StatusCodes.INTERNAL_SERVER_ERROR)
          .send("Internal server error");
      }
    }
  };
}

/**
 * Get data MD5 handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function getDataMD5Handler() {
  return async (req, res, next) => {
    const id = req.params.id;

    try {
      const item = config.datas[id];

      /* Check data is used? */
      if (item === undefined) {
        return res.status(StatusCodes.NOT_FOUND).send("Data does not exist");
      }

      /* Calculate MD5 and Add to header */
      let md5;

      switch (item.sourceType) {
        case "mbtiles": {
          md5 = await calculateMD5OfFile(item.path);

          break;
        }

        case "pmtiles": {
          // Do nothing

          md5 = "";

          break;
        }

        case "xyz": {
          // Do nothing

          md5 = "";

          break;
        }

        case "pg": {
          // Do nothing

          md5 = "";

          break;
        }
      }

      res.set({
        etag: md5,
      });

      return res.status(StatusCodes.OK).send();
    } catch (error) {
      printLog("error", `Failed to get md5 of data "${id}": ${error}`);

      if (error.message === "File does not exist") {
        return res.status(StatusCodes.NO_CONTENT).send(error.message);
      } else {
        return res
          .status(StatusCodes.INTERNAL_SERVER_ERROR)
          .send("Internal server error");
      }
    }
  };
}

/**
 * Download data handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function downloadDataHandler() {
  return async (req, res, next) => {
    const id = req.params.id;

    try {
      const item = config.datas[id];

      /* Check data is used? */
      if (item === undefined) {
        return res.status(StatusCodes.NOT_FOUND).send("Data does not exist");
      }

      if ((await isExistFile(item.path)) === true) {
        const stats = await stat(item.path);
        const fileName = path.basename(item.path);

        res.set({
          "content-length": stats.size,
          "content-disposition": `attachment; filename="${fileName}`,
          "content-type": "application/octet-stream",
        });

        const readStream = createReadStream(item.path);

        readStream.pipe(res);

        readStream.on("error", (error) => {
          throw error;
        });
      } else {
        throw new Error("File does not exist");
      }
    } catch (error) {
      printLog("error", `Failed to get data "${id}": ${error}`);

      if (error.message === "File does not exist") {
        return res.status(StatusCodes.NO_CONTENT).send(error.message);
      } else {
        return res
          .status(StatusCodes.INTERNAL_SERVER_ERROR)
          .send("Internal server error");
      }
    }
  };
}

/**
 * Get tile extra info handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function getDataTileExtraInfoHandler() {
  return async (req, res, next) => {
    const id = req.params.id;
    const item = config.datas[id];

    /* Check data is exist? */
    if (item === undefined) {
      return res.status(StatusCodes.NOT_FOUND).send("Data does not exist");
    }

    /* Get tile extra info */
    try {
      try {
        validateJSON(await getJSONSchema("coverages"), req.body);
      } catch (error) {
        return res
          .status(StatusCodes.BAD_REQUEST)
          .send(`Coverages is invalid: ${error}`);
      }

      let extraInfo;
      const targetCoverages = processCoverages(req.body, item.tileJSON.bounds);

      switch (item.sourceType) {
        case "mbtiles": {
          extraInfo = getMBTilesTileExtraInfoFromCoverages(
            item.source,
            targetCoverages,
            req.query.type === "created"
          );

          break;
        }

        case "pmtiles": {
          // Do nothing

          extraInfo = {};

          break;
        }

        case "xyz": {
          extraInfo = getXYZTileExtraInfoFromCoverages(
            item.md5Source,
            targetCoverages,
            req.query.type === "created"
          );

          break;
        }

        case "pg": {
          extraInfo = await getPostgreSQLTileExtraInfoFromCoverages(
            item.source,
            targetCoverages,
            req.query.type === "created"
          );

          break;
        }
      }

      const headers = {
        "content-type": "application/json",
      };

      if (req.query.compression === "true") {
        extraInfo = await gzipAsync(JSON.stringify(extraInfo));

        headers["content-encoding"] = "gzip";
      }

      res.set(headers);

      return res.status(StatusCodes.OK).send(extraInfo);
    } catch (error) {
      printLog("error", `Failed to get tile extra info "${id}": ${error}`);

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

/**
 * Calculate tile extra info handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function calculateDataExtraInfoHandler() {
  return async (req, res, next) => {
    const id = req.params.id;
    const item = config.datas[id];

    /* Check data is exist? */
    if (item === undefined) {
      return res.status(StatusCodes.NOT_FOUND).send("Data does not exist");
    }

    /* Calculate tile extra info */
    printLog("info", `Calculating tile extra info "${id}"...`);

    try {
      switch (item.sourceType) {
        case "mbtiles": {
          calculateMBTilesTileExtraInfo(item.source)
            .then(() => {
              printLog("info", `Done to calculate tile extra info "${id}"!`);
            })
            .catch((error) => {
              printLog(
                "error",
                `Failed to calculate tile extra info "${id}": ${error}`
              );
            });

          break;
        }

        case "pmtiles": {
          // Do nothing

          break;
        }

        case "xyz": {
          calculatXYZTileExtraInfo(
            item.source,
            item.md5Source,
            item.tileJSON.format
          )
            .then(() => {
              printLog("info", `Done to calculate tile extra info "${id}"!`);
            })
            .catch((error) => {
              printLog(
                "error",
                `Failed to calculate tile extra info "${id}": ${error}`
              );
            });

          break;
        }

        case "pg": {
          calculatePostgreSQLTileExtraInfo(item.source)
            .then(() => {
              printLog("info", `Done to calculate tile extra info "${id}"!`);
            })
            .catch((error) => {
              printLog(
                "error",
                `Failed to calculate tile extra info "${id}": ${error}`
              );
            });

          break;
        }
      }

      return res.status(StatusCodes.OK).send("OK");
    } catch (error) {
      printLog(
        "error",
        `Failed to calculate tile extra info "${id}": ${error}`
      );

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

/**
 * Get data tile list handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function getDatasListHandler() {
  return async (req, res, next) => {
    try {
      const requestHost = getRequestHost(req);

      const result = await Promise.all(
        Object.keys(config.datas).map(async (id) => {
          return {
            id: id,
            name: config.datas[id].tileJSON.name,
            url: `${requestHost}/datas/${id}.json`,
          };
        })
      );

      if (req.query.compression === "true") {
        result = await gzipAsync(JSON.stringify(result));

        res.set({
          "content-encoding": "gzip",
        });
      }

      return res.status(StatusCodes.OK).send(result);
    } catch (error) {
      printLog("error", `Failed to get datas": ${error}`);

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

export const serve_data = {
  /**
   * Register data handlers
   * @param {Express} app Express object
   * @returns {void}
   */
  init: (app) => {
    /**
     * @swagger
     * tags:
     *   - name: Data
     *     description: Data related endpoints
     * /datas/datas.json:
     *   get:
     *     tags:
     *       - Data
     *     summary: Get all datas
     *     parameters:
     *       - in: query
     *         name: compression
     *         schema:
     *           type: boolean
     *         required: false
     *         description: Compressed response
     *     responses:
     *       200:
     *         description: List of all datas
     *         content:
     *           application/json:
     *             schema:
     *               type: array
     *               items:
     *                 type: object
     *                 properties:
     *                   id:
     *                     type: string
     *                   name:
     *                     type: string
     *                   url:
     *                     type: string
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
    app.get("/datas/datas.json", getDatasListHandler());

    /**
     * @swagger
     * tags:
     *   - name: Data
     *     description: Data related endpoints
     * /datas/{id}.json:
     *   get:
     *     tags:
     *       - Data
     *     summary: Get data by ID
     *     parameters:
     *       - in: path
     *         name: id
     *         required: true
     *         schema:
     *           type: string
     *           example: id
     *         description: Data ID
     *     responses:
     *       200:
     *         description: Data information
     *         content:
     *           application/json:
     *             schema:
     *               type: object
     *       400:
     *         description: Invalid params
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
    app.get("/datas/:id.json", getDataHandler());

    /**
     * @swagger
     * tags:
     *   - name: Data
     *     description: Data related endpoints
     * /datas/{id}/export:
     *   get:
     *     tags:
     *       - Data
     *     summary: Cancel export data
     *     parameters:
     *       - in: query
     *         name: cancel
     *         schema:
     *           type: boolean
     *         required: false
     *         description: Cancel export
     *     responses:
     *       200:
     *         description: Data export is canceled
     *         content:
     *           text/plain:
     *             schema:
     *               type: string
     *               example: OK
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
     *   post:
     *     tags:
     *       - Data
     *     summary: Export data
     *     requestBody:
     *       required: true
     *       content:
     *         application/json:
     *             schema:
     *               type: object
     *               example: {}
     *       description: Data export options
     *     responses:
     *       201:
     *         description: Data export is started
     *         content:
     *           text/plain:
     *             schema:
     *               type: string
     *               example: OK
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
    app.get("/datas/:id/export", exportDataHandler());
    app.post("/datas/:id/export", exportDataHandler());

    /**
     * @swagger
     * tags:
     *   - name: Data
     *     description: Data related endpoints
     * /datas/{id}/extra-info:
     *   get:
     *     tags:
     *       - Data
     *     summary: Calculate tile extra info
     *     parameters:
     *       - in: path
     *         name: id
     *         required: true
     *         schema:
     *           type: string
     *           example: id
     *         description: Data ID
     *     responses:
     *       200:
     *         description: Tile extra info
     *         content:
     *           application/json:
     *             schema:
     *               type: object
     *       204:
     *         description: No content
     *       400:
     *         description: Invalid params
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
     *   post:
     *     tags:
     *       - Data
     *     summary: Get tile extra info
     *     parameters:
     *       - in: path
     *         name: id
     *         required: true
     *         schema:
     *           type: string
     *           example: id
     *         description: Data ID
     *       - in: query
     *         name: type
     *         schema:
     *           type: string
     *           enum: [hash, created]
     *           example: hash
     *         required: false
     *         description: Tile extra info type
     *       - in: query
     *         name: compression
     *         schema:
     *           type: boolean
     *         required: false
     *         description: Compressed response
     *     requestBody:
     *       required: true
     *       content:
     *         application/json:
     *             schema:
     *               type: object
     *               example: {}
     *       description: Coverages object
     *     responses:
     *       200:
     *         description: Tile extra info
     *         content:
     *           application/json:
     *             schema:
     *               type: object
     *       204:
     *         description: No content
     *       400:
     *         description: Invalid params
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
    app.get("/datas/:id/extra-info", calculateDataExtraInfoHandler());
    app.post("/datas/:id/extra-info", getDataTileExtraInfoHandler());

    /**
     * @swagger
     * tags:
     *   - name: Data
     *     description: Data related endpoints
     * /datas/{id}/md5:
     *   get:
     *     tags:
     *       - Data
     *     summary: Get data md5
     *     parameters:
     *       - in: path
     *         name: id
     *         schema:
     *           type: string
     *           example: id
     *         required: true
     *         description: ID of the data
     *     responses:
     *       200:
     *         description: Data md5
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
    app.get("/datas/:id/md5", getDataMD5Handler());

    /**
     * @swagger
     * tags:
     *   - name: Data
     *     description: Data related endpoints
     * /datas/{id}/download:
     *   get:
     *     tags:
     *       - Data
     *     summary: Download data file
     *     parameters:
     *       - in: path
     *         name: id
     *         schema:
     *           type: string
     *           example: id
     *         required: true
     *         description: ID of the data
     *     responses:
     *       200:
     *         description: Data file
     *         content:
     *           application/octet-stream:
     *             schema:
     *               type: string
     *               format: binary
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
    app.get("/datas/:id/download", downloadDataHandler());

    /**
     * @swagger
     * tags:
     *   - name: Data
     *     description: Data related endpoints
     * /datas/{id}/{z}/{x}/{y}.{format}:
     *   get:
     *     tags:
     *       - Data
     *     summary: Get data tile
     *     parameters:
     *       - in: path
     *         name: id
     *         required: true
     *         schema:
     *           type: string
     *           example: id
     *         description: Data ID
     *       - in: path
     *         name: z
     *         required: true
     *         schema:
     *           type: integer
     *           example: 0
     *         description: Zoom level
     *       - in: path
     *         name: x
     *         required: true
     *         schema:
     *           type: integer
     *           example: 0
     *         description: Tile X coordinate
     *       - in: path
     *         name: y
     *         required: true
     *         schema:
     *           type: integer
     *           example: 0
     *         description: Tile Y coordinate
     *       - in: path
     *         name: format
     *         required: true
     *         schema:
     *           type: string
     *           enum: [jpeg, jpg, pbf, png, webp, gif]
     *           example: png
     *         description: Tile format
     *     responses:
     *       200:
     *         description: Data tile
     *         content:
     *           application/octet-stream:
     *             schema:
     *               type: string
     *               format: binary
     *       204:
     *         description: No content
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
    app.get("/datas/:id/:z/:x/:y.:format", getDataTileHandler());

    /* Serve data */
    if (process.env.SERVE_FRONT_PAGE !== "false") {
      /**
       * @swagger
       * tags:
       *   - name: Data
       *     description: Data related endpoints
       * /datas/{id}:
       *   get:
       *     tags:
       *       - Data
       *     summary: Serve data page
       *     parameters:
       *       - in: path
       *         name: id
       *         schema:
       *           type: string
       *           example: id
       *         required: true
       *         description: ID of the data
       *     responses:
       *       200:
       *         description: Data page
       *         content:
       *           text/html:
       *             schema:
       *               type: string
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
      app.get("/datas/:id", serveDataHandler());
    }
  },

  /**
   * Add data
   * @returns {void}
   */
  add: async () => {
    if (config.datas === undefined) {
      printLog("info", "No datas in config. Skipping...");
    } else {
      const ids = Object.keys(config.datas);

      printLog("info", `Loading ${ids.length} datas...`);

      const repos = {};

      await Promise.all(
        ids.map(async (id) => {
          try {
            const item = config.datas[id];
            const dataInfo = {};

            /* Load data */
            if (item.mbtiles !== undefined) {
              dataInfo.sourceType = "mbtiles";

              if (item.cache !== undefined) {
                /* Get MBTiles cache options */
                const cacheSource = seed.datas?.[item.mbtiles];

                if (
                  cacheSource === undefined ||
                  cacheSource.storeType !== "mbtiles"
                ) {
                  throw new Error(
                    `Cache mbtiles data "${item.mbtiles}" is invalid`
                  );
                }

                if (item.cache.forward === true) {
                  dataInfo.sourceURL = cacheSource.url;
                  dataInfo.scheme = cacheSource.scheme;
                  dataInfo.storeCache = item.cache.store;
                  dataInfo.storeTransparent = cacheSource.storeTransparent;
                }

                /* Get MBTiles path */
                dataInfo.path = `${process.env.DATA_DIR}/caches/mbtiles/${item.mbtiles}/${item.mbtiles}.mbtiles`;

                /* Open MBTiles */
                dataInfo.source = await openMBTilesDB(
                  dataInfo.path,
                  true,
                  30000 // 30 secs
                );

                /* Get MBTiles metadata */
                dataInfo.tileJSON = createTileMetadataFromTemplate({
                  ...cacheSource.metadata,
                  cacheCoverages: processCoverages(
                    cacheSource.coverages,
                    cacheSource.metadata.bounds
                  ),
                });
              } else {
                /* Get MBTiles path */
                dataInfo.path = `${process.env.DATA_DIR}/mbtiles/${item.mbtiles}`;

                /* Open MBTiles */
                dataInfo.source = await openMBTilesDB(
                  dataInfo.path,
                  true,
                  30000 // 30 secs
                );

                /* Get MBTiles metadata */
                dataInfo.tileJSON = await getMBTilesMetadata(dataInfo.source);
              }
            } else if (item.pmtiles !== undefined) {
              dataInfo.sourceType = "pmtiles";

              if (
                item.pmtiles.startsWith("https://") === true ||
                item.pmtiles.startsWith("http://") === true
              ) {
                /* Get PMTiles path */
                dataInfo.path = item.pmtiles;

                /* Open PMTiles */
                dataInfo.source = openPMTiles(dataInfo.path);

                /* Get PMTiles metadata */
                dataInfo.tileJSON = await getPMTilesMetadata(dataInfo.source);
              } else {
                /* Get PMTiles path */
                dataInfo.path = `${process.env.DATA_DIR}/pmtiles/${item.pmtiles}`;

                /* Open PMTiles */
                dataInfo.source = openPMTiles(dataInfo.path);

                /* Get PMTiles metadata */
                dataInfo.tileJSON = await getPMTilesMetadata(dataInfo.source);
              }
            } else if (item.xyz !== undefined) {
              dataInfo.sourceType = "xyz";

              if (item.cache !== undefined) {
                /* Get XYZ cache options */
                const cacheSource = seed.datas?.[item.xyz];

                if (
                  cacheSource === undefined ||
                  cacheSource.storeType !== "xyz"
                ) {
                  throw new Error(`Cache xyz data "${item.xyz}" is invalid`);
                }

                if (item.cache.forward === true) {
                  dataInfo.sourceURL = cacheSource.url;
                  dataInfo.scheme = cacheSource.scheme;
                  dataInfo.storeCache = item.cache.store;
                  dataInfo.storeTransparent = cacheSource.storeTransparent;
                }

                /* Get XYZ path */
                dataInfo.path = `${process.env.DATA_DIR}/caches/xyzs/${item.xyz}`;

                dataInfo.source = dataInfo.path;

                /* Open XYZ MD5 */
                dataInfo.md5Source = await openXYZMD5DB(
                  `${dataInfo.path}/${item.xyz}.sqlite`,
                  true
                );

                /* Get XYZ metadata */
                dataInfo.tileJSON = createTileMetadataFromTemplate({
                  ...cacheSource.metadata,
                  cacheCoverages: processCoverages(
                    cacheSource.coverages,
                    cacheSource.metadata.bounds
                  ),
                });
              } else {
                /* Get XYZ path */
                dataInfo.path = `${process.env.DATA_DIR}/xyzs/${item.xyz}`;

                dataInfo.source = dataInfo.path;

                /* Open XYZ MD5 */
                const md5Source = await openXYZMD5DB(
                  `${dataInfo.path}/${item.xyz}.sqlite`,
                  true,
                  30000 // 30 secs
                );

                /* Get XYZ metadata */
                dataInfo.tileJSON = await getXYZMetadata(
                  dataInfo.source,
                  md5Source
                );
              }
            } else if (item.pg !== undefined) {
              dataInfo.sourceType = "pg";

              if (item.cache !== undefined) {
                /* Get PostgreSQL cache options */
                const cacheSource = seed.datas?.[item.pg];

                if (
                  cacheSource === undefined ||
                  cacheSource.storeType !== "pg"
                ) {
                  throw new Error(`Cache pg data "${item.pg}" is invalid`);
                }

                if (item.cache.forward === true) {
                  dataInfo.sourceURL = cacheSource.url;
                  dataInfo.scheme = cacheSource.scheme;
                  dataInfo.storeCache = item.cache.store;
                  dataInfo.storeTransparent = cacheSource.storeTransparent;
                }

                /* Get XYZ path */
                dataInfo.path = `${process.env.POSTGRESQL_BASE_URI}/${id}`;

                /* Open PostgreSQL */
                dataInfo.source = await openPostgreSQLDB(dataInfo.path, true);

                /* Get PostgreSQL metadata */
                dataInfo.tileJSON = createTileMetadataFromTemplate({
                  ...cacheSource.metadata,
                  cacheCoverages: processCoverages(
                    cacheSource.coverages,
                    cacheSource.metadata.bounds
                  ),
                });
              } else {
                /* Get XYZ path */
                dataInfo.path = `${process.env.POSTGRESQL_BASE_URI}/${id}`;

                /* Open PostgreSQL */
                dataInfo.source = await openPostgreSQLDB(dataInfo.path, false);

                /* Get PostgreSQL metadata */
                dataInfo.tileJSON = await getPostgreSQLMetadata(
                  dataInfo.source
                );
              }
            }

            /* Validate tile metadata */
            validateTileMetadata(dataInfo.tileJSON);

            /* Add to repo */
            repos[id] = dataInfo;
          } catch (error) {
            printLog(
              "error",
              `Failed to load data "${id}": ${error}. Skipping...`
            );
          }
        })
      );

      config.datas = repos;
    }
  },
};
