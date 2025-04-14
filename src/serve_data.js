"use strict";

import { StatusCodes } from "http-status-codes";
import { printLog } from "./logger.js";
import { config } from "./config.js";
import { seed } from "./seed.js";
import {
  getXYZTileHashFromCoverages,
  calculatXYZTileHash,
  updateXYZMetadata,
  getXYZTileFromURL,
  cacheXYZTileFile,
  getXYZMetadata,
  openXYZMD5DB,
  validateXYZ,
  getXYZTile,
} from "./tile_xyz.js";
import {
  getMBTilesTileHashFromCoverages,
  calculateMBTilesTileHash,
  updateMBTilesMetadata,
  getMBTilesTileFromURL,
  cacheMBtilesTileData,
  downloadMBTilesFile,
  getMBTilesMetadata,
  validateMBTiles,
  getMBTilesTile,
  openMBTilesDB,
} from "./tile_mbtiles.js";
import {
  createTileMetadataFromTemplate,
  compileTemplate,
  getRequestHost,
  getJSONSchema,
  validateJSON,
  isExistFile,
  gzipAsync,
} from "./utils.js";
import {
  getPMTilesMetadata,
  validatePMTiles,
  getPMTilesTile,
  openPMTiles,
} from "./tile_pmtiles.js";
import {
  getPostgreSQLTileHashFromCoverages,
  calculatePostgreSQLTileHash,
  updatePostgreSQLMetadata,
  getPostgreSQLTileFromURL,
  cachePostgreSQLTileData,
  getPostgreSQLMetadata,
  validatePostgreSQL,
  getPostgreSQLTile,
  openPostgreSQLDB,
} from "./tile_postgresql.js";

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

    /* Get tile data */
    try {
      let dataTile;

      if (item.sourceType === "mbtiles") {
        try {
          dataTile = getMBTilesTile(item.source, z, x, y);
        } catch (error) {
          if (
            item.sourceURL !== undefined &&
            error.message === "Tile does not exist"
          ) {
            const tmpY = item.scheme === "tms" ? (1 << z) - 1 - y : y;

            const targetURL = item.sourceURL
              .replace("{z}", `${z}`)
              .replace("{x}", `${x}`)
              .replace("{y}", `${tmpY}`);

            printLog(
              "info",
              `Forwarding data "${id}" - Tile "${tileName}" - To "${targetURL}"...`
            );

            /* Get data */
            dataTile = await getMBTilesTileFromURL(
              targetURL,
              60000 // 1 mins
            );

            /* Cache */
            if (item.storeCache === true) {
              printLog("info", `Caching data "${id}" - Tile "${tileName}"...`);

              cacheMBtilesTileData(
                item.source,
                z,
                x,
                tmpY,
                dataTile.data,
                item.storeTransparent
              ).catch((error) =>
                printLog(
                  "error",
                  `Failed to cache data "${id}" - Tile "${tileName}": ${error}`
                )
              );
            }
          } else {
            throw error;
          }
        }
      } else if (item.sourceType === "pmtiles") {
        dataTile = await getPMTilesTile(item.source, z, x, y);
      } else if (item.sourceType === "xyz") {
        try {
          dataTile = await getXYZTile(
            item.source,
            z,
            x,
            y,
            item.tileJSON.format
          );
        } catch (error) {
          if (
            item.sourceURL !== undefined &&
            error.message === "Tile does not exist"
          ) {
            const tmpY = item.scheme === "tms" ? (1 << z) - 1 - y : y;

            const targetURL = item.sourceURL
              .replace("{z}", `${z}`)
              .replace("{x}", `${x}`)
              .replace("{y}", `${tmpY}`);

            printLog(
              "info",
              `Forwarding data "${id}" - Tile "${tileName}" - To "${targetURL}"...`
            );

            /* Get data */
            dataTile = await getXYZTileFromURL(
              targetURL,
              60000 // 1 mins
            );

            /* Cache */
            if (item.storeCache === true) {
              printLog("info", `Caching data "${id}" - Tile "${tileName}"...`);

              cacheXYZTileFile(
                item.source,
                item.md5Source,
                z,
                x,
                tmpY,
                item.tileJSON.format,
                dataTile.data,
                item.storeTransparent
              ).catch((error) =>
                printLog(
                  "error",
                  `Failed to cache data "${id}" - Tile "${tileName}": ${error}`
                )
              );
            }
          } else {
            throw error;
          }
        }
      } else if (item.sourceType === "pg") {
        try {
          dataTile = await getPostgreSQLTile(item.source, z, x, y);
        } catch (error) {
          if (
            item.sourceURL !== undefined &&
            error.message === "Tile does not exist"
          ) {
            const tmpY = item.scheme === "tms" ? (1 << z) - 1 - y : y;

            const targetURL = item.sourceURL
              .replace("{z}", `${z}`)
              .replace("{x}", `${x}`)
              .replace("{y}", `${tmpY}`);

            printLog(
              "info",
              `Forwarding data "${id}" - Tile "${tileName}" - To "${targetURL}"...`
            );

            /* Get data */
            dataTile = await getPostgreSQLTileFromURL(
              targetURL,
              60000 // 1 mins
            );

            /* Cache */
            if (item.storeCache === true) {
              printLog("info", `Caching data "${id}" - Tile "${tileName}"...`);

              cachePostgreSQLTileData(
                item.source,
                z,
                x,
                tmpY,
                dataTile.data,
                item.storeTransparent
              ).catch((error) =>
                printLog(
                  "error",
                  `Failed to cache data "${id}" - Tile "${tileName}": ${error}`
                )
              );
            }
          } else {
            throw error;
          }
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
 * Get data tile MD5s handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function getDataTileMD5sHandler() {
  return async (req, res, next) => {
    const id = req.params.id;
    const item = config.datas[id];

    /* Check data is exist? */
    if (item === undefined) {
      return res.status(StatusCodes.NOT_FOUND).send("Data does not exist");
    }

    /* Get data tile MD5s */
    try {
      try {
        validateJSON(await getJSONSchema("coverages"), req.body);
      } catch (error) {
        return res
          .status(StatusCodes.BAD_REQUEST)
          .send(`Coverages is invalid: ${error}`);
      }

      let md5s;

      if (item.sourceType === "mbtiles") {
        md5s = getMBTilesTileHashFromCoverages(item.source, req.body);
      } else if (item.sourceType === "pmtiles") {
        md5s = {};
      } else if (item.sourceType === "xyz") {
        md5s = getXYZTileHashFromCoverages(item.md5Source, req.body);
      } else if (item.sourceType === "pg") {
        md5s = await getPostgreSQLTileHashFromCoverages(item.source, req.body);
      }

      res.header("content-type", "application/json");

      return res.status(StatusCodes.OK).send(md5s);
    } catch (error) {
      printLog("error", `Failed to get data tile md5s "${id}": ${error}`);

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

/**
 * Calculate data tile MD5s handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function calculateDataTileMD5sHandler() {
  return async (req, res, next) => {
    const id = req.params.id;
    const item = config.datas[id];

    /* Check data is exist? */
    if (item === undefined) {
      return res.status(StatusCodes.NOT_FOUND).send("Data does not exist");
    }

    /* Calculate data tile MD5s */
    try {
      if (item.sourceType === "mbtiles") {
        setTimeout(() => calculateMBTilesTileHash(item.source), 0);
      } else if (item.sourceType === "pmtiles") {
      } else if (item.sourceType === "xyz") {
        setTimeout(
          () =>
            calculatXYZTileHash(
              item.source,
              item.md5Source,
              item.tileJSON.format
            ),
          0
        );
      } else if (item.sourceType === "pg") {
        setTimeout(() => calculatePostgreSQLTileHash(item.source), 0);
      }

      return res.status(StatusCodes.OK).send("OK");
    } catch (error) {
      printLog("error", `Failed to calculate data tile md5s "${id}": ${error}`);

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

      return res.status(StatusCodes.OK).send(result);
    } catch (error) {
      printLog("error", `Failed to get datas": ${error}`);

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

/**
 * Get data tileJSON list handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function getDataTileJSONsListHandler() {
  return async (req, res, next) => {
    try {
      const requestHost = getRequestHost(req);

      const result = await Promise.all(
        Object.keys(config.datas).map(async (id) => {
          const item = config.datas[id];

          return {
            ...item.tileJSON,
            tilejson: "2.2.0",
            scheme: "xyz",
            id: id,
            tiles: [
              `${requestHost}/datas/${id}/{z}/{x}/{y}.${item.tileJSON.format}`,
            ],
          };
        })
      );

      return res.status(StatusCodes.OK).send(result);
    } catch (error) {
      printLog("error", `Failed to get data tileJSONs": ${error}`);

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
     * /datas/tilejsons.json:
     *   get:
     *     tags:
     *       - Data
     *     summary: Get all data tileJSONs
     *     responses:
     *       200:
     *         description: List of all data tileJSONs
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
    app.get("/datas/tilejsons.json", getDataTileJSONsListHandler());

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
     * /datas/{id}/md5s:
     *   post:
     *     tags:
     *       - Data
     *     summary: Get data tile MD5s
     *     parameters:
     *       - in: path
     *         name: id
     *         required: true
     *         schema:
     *           type: string
     *           example: id
     *         description: Data ID
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
     *         description: Data tile MD5s
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
     *   get:
     *     tags:
     *       - Data
     *     summary: Calculate data tile MD5s
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
     *         description: Data tile MD5s
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
    app.get("/datas/:id/md5s", calculateDataTileMD5sHandler());
    app.post("/datas/:id/md5s", getDataTileMD5sHandler());

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

    if (process.env.SERVE_FRONT_PAGE !== "false") {
      /* Serve data */
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

              if (
                item.mbtiles.startsWith("https://") === true ||
                item.mbtiles.startsWith("http://") === true
              ) {
                /* Get MBTiles path */
                dataInfo.path = `${process.env.DATA_DIR}/mbtiles/${id}/${id}.mbtiles`;

                /* Download MBTiles file if not exist */
                if ((await isExistFile(dataInfo.path)) === false) {
                  printLog(
                    "info",
                    `Downloading MBTiles file "${dataInfo.path}" - From "${item.mbtiles}"...`
                  );

                  await downloadMBTilesFile(
                    item.mbtiles,
                    dataInfo.path,
                    5,
                    3600000 // 1 hour
                  );
                }

                /* Open MBTiles */
                dataInfo.source = await openMBTilesDB(dataInfo.path, true);

                /* Get MBTiles metadata */
                dataInfo.tileJSON = await getMBTilesMetadata(dataInfo.source);
              } else {
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
                  dataInfo.source = await openMBTilesDB(dataInfo.path, true);

                  /* Get MBTiles metadata */
                  dataInfo.tileJSON = createTileMetadataFromTemplate({
                    ...cacheSource.metadata,
                    cacheCoverages: cacheSource.coverages,
                  });

                  /* Update MBTiles metadata */
                  await updateMBTilesMetadata(
                    dataInfo.source,
                    dataInfo.tileJSON,
                    60000 // 1 mins
                  );
                } else {
                  /* Get MBTiles path */
                  dataInfo.path = `${process.env.DATA_DIR}/mbtiles/${item.mbtiles}`;

                  /* Open MBTiles */
                  dataInfo.source = await openMBTilesDB(dataInfo.path, true);

                  /* Get MBTiles metadata */
                  dataInfo.tileJSON = await getMBTilesMetadata(dataInfo.source);
                }
              }

              /* Validate MBTiles */
              validateMBTiles(dataInfo.tileJSON);
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

              /* Validate PMTiles */
              validatePMTiles(dataInfo.tileJSON);
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
                  cacheCoverages: cacheSource.coverages,
                });

                /* Update XYZ metadata */
                await updateXYZMetadata(
                  dataInfo.md5Source,
                  dataInfo.tileJSON,
                  60000 // 1 mins
                );
              } else {
                /* Get XYZ path */
                dataInfo.path = `${process.env.DATA_DIR}/xyzs/${item.xyz}`;

                dataInfo.source = dataInfo.path;

                /* Open XYZ MD5 */
                const md5Source = await openXYZMD5DB(
                  `${dataInfo.path}/${item.xyz}.sqlite`,
                  true
                );

                /* Get XYZ metadata */
                dataInfo.tileJSON = await getXYZMetadata(
                  dataInfo.source,
                  md5Source
                );
              }

              /* Validate XYZ */
              validateXYZ(dataInfo.tileJSON);
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
                  cacheCoverages: cacheSource.coverages,
                });

                /* Update PostgreSQL metadata */
                await updatePostgreSQLMetadata(
                  dataInfo.source,
                  dataInfo.tileJSON,
                  60000 // 1 mins
                );
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

              /* Validate PostgreSQL */
              validatePostgreSQL(dataInfo.tileJSON);
            }

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
