"use strict";

import { config, seed } from "../configs/index.js";
import { StatusCodes } from "http-status-codes";
import { createReadStream } from "fs";
import path from "path";
import {
  detectContentTypeFromFormat,
  compileHandleBarsTemplate,
  createTileMetadata,
  calculateMD5OfFile,
  getRequestHost,
  getTileBounds,
  getJSONSchema,
  validateJSON,
  getFileSize,
  isExistFile,
  gzipAsync,
  printLog,
} from "../utils/index.js";
import {
  getPostgreSQLTileExtraInfoFromCoverages,
  getMBTilesTileExtraInfoFromCoverages,
  getXYZTileExtraInfoFromCoverages,
  calculatePostgreSQLTileExtraInfo,
  calculateMBTilesTileExtraInfo,
  getAndCachePostgreSQLDataTile,
  getAndCacheMBTilesDataTile,
  calculateXYZTileExtraInfo,
  getAndCacheXYZDataTile,
  getPostgreSQLMetadata,
  validateTileMetadata,
  getMBTilesMetadata,
  getPMTilesMetadata,
  openPostgreSQLDB,
  getXYZMetadata,
  getPMTilesTile,
  openMBTilesDB,
  openXYZMD5DB,
  openPMTiles,
  ALL_FORMATS,
} from "../resources/index.js";

/**
 * Serve data handler
 * @returns {(req: Request, res: Response, next: NextFunction) => Promise<any>}
 */
function serveDataHandler() {
  return async (req, res) => {
    const id = req.params.id;

    try {
      const item = config.datas[id];

      if (!item) {
        return res.status(StatusCodes.NOT_FOUND).send("Data does not exist");
      }

      const compiled = await compileHandleBarsTemplate(
        item.tileJSON.format === "pbf" ? "vector_data" : "raster_data",
        {
          id: id,
          name: item.tileJSON.name,
          base_url: getRequestHost(req),
        },
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
 * @returns {(req: Request, res: Response, next: NextFunction) => Promise<any>}
 */
function getDataTileHandler() {
  return async (req, res) => {
    const id = req.params.id;
    const item = config.datas[id];

    /* Check data is exist? */
    if (!item) {
      return res.status(StatusCodes.NOT_FOUND).send("Data does not exist");
    }

    /* Check data tile format */
    if (
      req.params.format !== item.tileJSON.format ||
      !ALL_FORMATS.has(req.params.format)
    ) {
      return res
        .status(StatusCodes.BAD_REQUEST)
        .send("Data tile format is not support");
    }

    /* Get and cache tile data */
    try {
      let dataTile;

      switch (item.sourceType) {
        case "mbtiles": {
          dataTile = await getAndCacheMBTilesDataTile(
            id,
            +req.params.z,
            +req.params.x,
            +req.params.y,
          );

          break;
        }

        case "pmtiles": {
          dataTile = await getPMTilesTile(
            item.source,
            +req.params.z,
            +req.params.x,
            +req.params.y,
          );

          break;
        }

        case "xyz": {
          dataTile = await getAndCacheXYZDataTile(
            id,
            +req.params.z,
            +req.params.x,
            +req.params.y,
          );

          break;
        }

        case "pg": {
          dataTile = await getAndCachePostgreSQLDataTile(
            id,
            +req.params.z,
            +req.params.x,
            +req.params.y,
          );

          break;
        }
      }

      /* Gzip pbf data tile */
      if (
        dataTile.headers["content-type"] === "application/x-protobuf" &&
        !dataTile.headers["content-encoding"]
      ) {
        dataTile.data = await gzipAsync(dataTile.data);

        dataTile.headers["content-encoding"] = "gzip";
      }

      res.set(dataTile.headers);

      return res.status(StatusCodes.OK).send(dataTile.data);
    } catch (error) {
      printLog(
        "error",
        `Failed to get data "${id}" - Tile "${req.params.z}/${req.params.x}/${req.params.y}": ${error}`,
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
 * @returns {(req: Request, res: Response, next: NextFunction) => Promise<any>}
 */
function getDataHandler() {
  return async (req, res) => {
    const id = req.params.id;

    try {
      const item = config.datas[id];

      if (!item) {
        return res.status(StatusCodes.NOT_FOUND).send("Data does not exist");
      }

      const data = {
        ...item.tileJSON,
        tilejson: "2.2.0",
        scheme: "xyz",
        id: id,
        tiles: [
          `${getRequestHost(req)}/datas/${id}/{z}/{x}/{y}.${
            item.tileJSON.format
          }`,
        ],
      };

      res.header("content-type", "application/json");

      return res.status(StatusCodes.OK).send(data);
    } catch (error) {
      printLog("error", `Failed to get data "${id}": ${error}`);

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

/**
 * Get data MD5 handler
 * @returns {(req: Request, res: Response, next: NextFunction) => Promise<any>}
 */
function getDataMD5Handler() {
  return async (req, res) => {
    const id = req.params.id;

    try {
      const item = config.datas[id];

      /* Check data is used? */
      if (!item) {
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
 * @returns {(req: Request, res: Response, next: NextFunction) => Promise<any>}
 */
function downloadDataHandler() {
  return async (req, res) => {
    const id = req.params.id;

    try {
      const item = config.datas[id];

      /* Check data is used? */
      if (!item) {
        return res.status(StatusCodes.NOT_FOUND).send("Data does not exist");
      }

      if (await isExistFile(item.path)) {
        const fileName = path.basename(item.path);

        res.set({
          "content-length": await getFileSize(item.path),
          "content-disposition": `attachment; filename="${fileName}"`,
          "content-type": detectContentTypeFromFormat(item.tileJSON.format),
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
 * @returns {(req: Request, res: Response, next: NextFunction) => Promise<any>}
 */
function getDataTileExtraInfoHandler() {
  return async (req, res) => {
    const id = req.params.id;
    const item = config.datas[id];

    /* Check data is exist? */
    if (!item) {
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
      const isCreated = req.query.type === "created";

      switch (item.sourceType) {
        case "mbtiles": {
          extraInfo = getMBTilesTileExtraInfoFromCoverages(
            item.source,
            req.body,
            isCreated,
            item.tileJSON.bounds,
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
            req.body,
            isCreated,
            item.tileJSON.bounds,
          );

          break;
        }

        case "pg": {
          extraInfo = await getPostgreSQLTileExtraInfoFromCoverages(
            item.source,
            req.body,
            isCreated,
            item.tileJSON.bounds,
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

      return res.status(StatusCodes.CREATED).send(extraInfo);
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
 * @returns {(req: Request, res: Response, next: NextFunction) => Promise<any>}
 */
function calculateDataExtraInfoHandler() {
  return async (req, res) => {
    const id = req.params.id;
    const item = config.datas[id];

    /* Check data is exist? */
    if (!item) {
      return res.status(StatusCodes.NOT_FOUND).send("Data does not exist");
    }

    /* Calculate tile extra info */
    printLog("info", `Calculating tile extra info "${id}"...`);

    try {
      let calculateTileExtraInfoFunc;

      switch (item.sourceType) {
        case "mbtiles": {
          calculateTileExtraInfoFunc = async () =>
            calculateMBTilesTileExtraInfo(item.source);

          break;
        }

        case "pmtiles": {
          calculateTileExtraInfoFunc = async () => {};

          break;
        }

        case "xyz": {
          calculateTileExtraInfoFunc = async () =>
            await calculateXYZTileExtraInfo(item.source, item.md5Source);

          break;
        }

        case "pg": {
          calculateTileExtraInfoFunc = async () =>
            await calculatePostgreSQLTileExtraInfo(item.source);

          break;
        }
      }

      calculateTileExtraInfoFunc()
        .then(() => {
          printLog("info", `Done to calculate tile extra info "${id}"!`);
        })
        .catch((error) => {
          printLog(
            "error",
            `Failed to calculate tile extra info "${id}": ${error}`,
          );
        });

      return res.status(StatusCodes.OK).send("OK");
    } catch (error) {
      printLog(
        "error",
        `Failed to calculate tile extra info "${id}": ${error}`,
      );

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

/**
 * Get data tile list handler
 * @returns {(req: Request, res: Response, next: NextFunction) => Promise<any>}
 */
function getDatasListHandler() {
  return async (req, res) => {
    try {
      const requestHost = getRequestHost(req);

      const result = await Promise.all(
        Object.keys(config.datas).map(async (id) => {
          return {
            id: id,
            name: config.datas[id].tileJSON.name,
            url: `${requestHost}/datas/${id}.json`,
          };
        }),
      );

      const headers = {
        "content-type": "application/json",
      };

      if (req.query.compression === "true") {
        result = await gzipAsync(JSON.stringify(result));

        headers["content-encoding"] = "gzip";
      }

      res.set(headers);

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
    if (!config.datas) {
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

              if (item.cache) {
                /* Get MBTiles cache options */
                const cacheSource = seed.datas?.[item.mbtiles];

                if (!cacheSource || cacheSource.storeType !== "mbtiles") {
                  throw new Error(
                    `Cache mbtiles data "${item.mbtiles}" is invalid`,
                  );
                }

                if (item.cache.forward) {
                  dataInfo.sourceURL = cacheSource.url;
                  dataInfo.headers = cacheSource.headers;
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
                  30000, // 30 seconds
                );

                /* Get MBTiles metadata */
                dataInfo.tileJSON = createTileMetadata({
                  ...cacheSource.metadata,
                  cacheCoverages: getTileBounds({
                    coverages: cacheSource.coverages,
                  }).targetCoverages,
                  ...(item.tilejson ?? {}),
                });
              } else {
                /* Get MBTiles path */
                dataInfo.path = `${process.env.DATA_DIR}/mbtiles/${item.mbtiles}`;

                /* Open MBTiles */
                dataInfo.source = await openMBTilesDB(
                  dataInfo.path,
                  true,
                  30000, // 30 seconds
                );

                /* Get MBTiles metadata */
                dataInfo.tileJSON = {
                  ...(await getMBTilesMetadata(dataInfo.source)),
                  ...(item.tilejson ?? {}),
                };
              }
            } else if (item.pmtiles !== undefined) {
              dataInfo.sourceType = "pmtiles";

              if (
                ["https://", "http://"].some((scheme) =>
                  item.pmtiles.startsWith(scheme),
                )
              ) {
                /* Get PMTiles path */
                dataInfo.path = item.pmtiles;

                /* Open PMTiles */
                dataInfo.source = openPMTiles(dataInfo.path);

                /* Get PMTiles metadata */
                dataInfo.tileJSON = {
                  ...(await getPMTilesMetadata(dataInfo.source)),
                  ...(item.tilejson ?? {}),
                };
              } else {
                /* Get PMTiles path */
                dataInfo.path = `${process.env.DATA_DIR}/pmtiles/${item.pmtiles}`;

                /* Open PMTiles */
                dataInfo.source = openPMTiles(dataInfo.path);

                /* Get PMTiles metadata */
                dataInfo.tileJSON = {
                  ...(await getPMTilesMetadata(dataInfo.source)),
                  ...(item.tilejson ?? {}),
                };
              }
            } else if (item.xyz !== undefined) {
              dataInfo.sourceType = "xyz";

              if (item.cache) {
                /* Get XYZ cache options */
                const cacheSource = seed.datas?.[item.xyz];

                if (!cacheSource || cacheSource.storeType !== "xyz") {
                  throw new Error(`Cache xyz data "${item.xyz}" is invalid`);
                }

                if (item.cache.forward) {
                  dataInfo.sourceURL = cacheSource.url;
                  dataInfo.headers = cacheSource.headers;
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
                  true,
                );

                /* Get XYZ metadata */
                dataInfo.tileJSON = createTileMetadata({
                  ...cacheSource.metadata,
                  cacheCoverages: getTileBounds({
                    coverages: cacheSource.coverages,
                  }).targetCoverages,
                  ...(item.tilejson ?? {}),
                });
              } else {
                /* Get XYZ path */
                dataInfo.path = `${process.env.DATA_DIR}/xyzs/${item.xyz}`;

                dataInfo.source = dataInfo.path;

                /* Open XYZ MD5 */
                const md5Source = await openXYZMD5DB(
                  `${dataInfo.path}/${item.xyz}.sqlite`,
                  true,
                  30000, // 30 seconds
                );

                /* Get XYZ metadata */
                dataInfo.tileJSON = dataInfo.tileJSON = {
                  ...(await getXYZMetadata(dataInfo.source, md5Source)),
                  ...(item.tilejson ?? {}),
                };
              }
            } else if (item.pg !== undefined) {
              dataInfo.sourceType = "pg";

              if (item.cache) {
                /* Get PostgreSQL cache options */
                const cacheSource = seed.datas?.[item.pg];

                if (!cacheSource || cacheSource.storeType !== "pg") {
                  throw new Error(`Cache pg data "${item.pg}" is invalid`);
                }

                if (item.cache.forward) {
                  dataInfo.sourceURL = cacheSource.url;
                  dataInfo.headers = cacheSource.headers;
                  dataInfo.scheme = cacheSource.scheme;
                  dataInfo.storeCache = item.cache.store;
                  dataInfo.storeTransparent = cacheSource.storeTransparent;
                }

                /* Get XYZ path */
                dataInfo.path = `${process.env.POSTGRESQL_BASE_URI}/${id}`;

                /* Open PostgreSQL */
                dataInfo.source = await openPostgreSQLDB(
                  dataInfo.path,
                  true,
                  30000, // 30 seconds
                );

                /* Get PostgreSQL metadata */
                dataInfo.tileJSON = createTileMetadata({
                  ...cacheSource.metadata,
                  cacheCoverages: getTileBounds({
                    coverages: cacheSource.coverages,
                  }).targetCoverages,
                  ...(item.tilejson ?? {}),
                });
              } else {
                /* Get XYZ path */
                dataInfo.path = `${process.env.POSTGRESQL_BASE_URI}/${id}`;

                /* Open PostgreSQL */
                dataInfo.source = await openPostgreSQLDB(
                  dataInfo.path,
                  false,
                  30000, // 30 seconds
                );

                /* Get PostgreSQL metadata */
                dataInfo.tileJSON = {
                  ...(await getPostgreSQLMetadata(dataInfo.source)),
                  ...(item.tilejson ?? {}),
                };
              }
            }

            /* Validate tile metadata */
            if (item.validate) {
              validateTileMetadata(dataInfo.tileJSON);
            }

            /* Add to repo */
            repos[id] = dataInfo;
          } catch (error) {
            printLog(
              "error",
              `Failed to load data "${id}": ${error}. Skipping...`,
            );
          }
        }),
      );

      config.datas = repos;
    }
  },
};
