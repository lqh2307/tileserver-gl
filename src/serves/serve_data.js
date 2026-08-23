"use strict";

import { config, seed } from "../configs/index.js";
import { StatusCodes } from "http-status-codes";
import path from "node:path";
import {
  DEFAULT_QUERY_TIMEOUT,
  DEFAULT_CONCURRENCY,
} from "../defaults/index.js";
import {
  compileHandleBarsTemplate,
  validateTileMetadata,
  createTileMetadata,
  isFileNotModified,
  getXYZFromLonLatZ,
  ALL_TILE_FORMATS,
  sendTextResponse,
  runAllWithLimit,
  getRequestHost,
  getTileBounds,
  getJSONSchema,
  validateJSON,
  HTTP_SCHEMES,
  inflateAsync,
  unzipAsync,
  gzipAsync,
  printLog,
} from "../utils/index.js";
import {
  calculatePostgreSQLTileExtraInfo,
  calculateMBTilesTileExtraInfo,
  getAndCachePostgreSQLTileData,
  getPostgreSQLTileExtraInfo,
  getAndCacheMBTilesTileData,
  calculateXYZTileExtraInfo,
  getMBTilesTileExtraInfo,
  getAndCacheXYZTileData,
  getPostgreSQLMetadata,
  getXYZTileExtraInfo,
  getMBTilesMetadata,
  getPMTilesMetadata,
  closePostgreSQLDB,
  openPostgreSQLDB,
  closeMBTilesDB,
  getXYZMetadata,
  getPMTilesTile,
  getMBTilesMD5,
  openMBTilesDB,
  closeXYZMD5DB,
  openXYZMD5DB,
  openPMTiles,
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
        return sendTextResponse(
          res,
          StatusCodes.NOT_FOUND,
          `Data id "${id}" does not exist`,
        );
      }

      const compiled = await compileHandleBarsTemplate(
        item.tileJSON.format === "pbf" ? "vector_data" : "raster_data",
        {
          id,
          name: item.tileJSON.name,
          base_url: getRequestHost(req),
        },
      );

      return res.status(StatusCodes.OK).send(compiled);
    } catch (error) {
      printLog("error", `Failed to serve data id "${id}": ${error}`);

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

/**
 * Get tile data handler
 * @returns {(req: Request, res: Response, next: NextFunction) => Promise<any>}
 */
function getTileDataHandler() {
  return async (req, res) => {
    const id = req.params.id;
    const item = config.datas[id];

    /* Check data is exist? */
    if (!item) {
      return sendTextResponse(
        res,
        StatusCodes.NOT_FOUND,
        `Data id "${id}" does not exist`,
      );
    }

    /* Check tile data format */
    if (
      req.params.format !== item.tileJSON.format ||
      !ALL_TILE_FORMATS.has(req.params.format)
    ) {
      return sendTextResponse(
        res,
        StatusCodes.BAD_REQUEST,
        `Data tile format "${req.params.format}" is not support`,
      );
    }

    const z = +req.params.z;
    const x = +req.params.x;
    const y = +req.params.y;

    /* Get and cache tile data */
    try {
      if (
        await isFileNotModified(
          req,
          res,
          item.sourceType === "xyz"
            ? path.join(
                item.path,
                String(z),
                String(x),
                `${y}.${req.params.format}`,
              )
            : item.sourceType === "mbtiles" || item.sourceType === "pmtiles"
              ? item.path
              : undefined,
        )
      ) {
        return res.status(StatusCodes.NOT_MODIFIED).end();
      }

      let tileData;

      switch (item.sourceType) {
        case "mbtiles": {
          tileData = await getAndCacheMBTilesTileData(id, z, x, y);

          break;
        }

        case "pmtiles": {
          tileData = await getPMTilesTile(item.source, z, x, y);

          break;
        }

        case "xyz": {
          tileData = await getAndCacheXYZTileData(id, z, x, y);

          break;
        }

        case "pg": {
          tileData = await getAndCachePostgreSQLTileData(id, z, x, y);

          break;
        }
      }

      /* Gzip pbf tile data */
      if (tileData.headers["content-type"] === "application/x-protobuf") {
        const acceptsGzip = req.acceptsEncodings("gzip") === "gzip";
        const contentEncoding = tileData.headers["content-encoding"];

        res.vary("Accept-Encoding");

        if (!contentEncoding && acceptsGzip) {
          tileData.data = await gzipAsync(tileData.data);

          tileData.headers["content-encoding"] = "gzip";
        } else if (contentEncoding && !acceptsGzip) {
          tileData.data =
            contentEncoding === "gzip"
              ? await unzipAsync(tileData.data)
              : await inflateAsync(tileData.data);

          delete tileData.headers["content-encoding"];
        }
      }

      res.set(tileData.headers);

      return res.status(StatusCodes.OK).send(tileData.data);
    } catch (error) {
      printLog(
        "error",
        `Failed to get data id "${id}" - Tile "${req.params.z}/${req.params.x}/${req.params.y}": ${error}`,
      );

      if (error.message.includes("Not Found")) {
        return sendTextResponse(res, StatusCodes.NO_CONTENT, error.message);
      } else {
        return res
          .status(StatusCodes.INTERNAL_SERVER_ERROR)
          .send("Internal server error");
      }
    }
  };
}

/**
 * Get tile dataJSON handler
 * @returns {(req: Request, res: Response, next: NextFunction) => Promise<any>}
 */
function getDataHandler() {
  return async (req, res) => {
    const id = req.params.id;

    try {
      const item = config.datas[id];

      if (!item) {
        return sendTextResponse(
          res,
          StatusCodes.NOT_FOUND,
          `Data id "${id}" does not exist`,
        );
      }

      res.set("content-type", "application/json");

      return res.status(StatusCodes.OK).send({
        ...item.tileJSON,
        tilejson: "2.2.0",
        scheme: "xyz",
        id,
        tiles: [
          `${getRequestHost(req)}/datas/${id}/{z}/{x}/{y}.${
            item.tileJSON.format
          }`,
        ],
      });
    } catch (error) {
      printLog("error", `Failed to get data id "${id}": ${error}`);

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
        return sendTextResponse(
          res,
          StatusCodes.NOT_FOUND,
          `Data id "${id}" does not exist`,
        );
      }

      /* Calculate MD5 and Add to header */
      let md5;

      switch (item.sourceType) {
        case "mbtiles": {
          md5 = await getMBTilesMD5(item.path);

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

      res.set("etag", md5);

      return res.status(StatusCodes.OK).send();
    } catch (error) {
      printLog("error", `Failed to get md5 of data id "${id}": ${error}`);

      if (error.message.includes("Not Found")) {
        return sendTextResponse(res, StatusCodes.NO_CONTENT, error.message);
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
function getTileDataExtraInfoHandler() {
  return async (req, res) => {
    const id = req.params.id;
    const item = config.datas[id];

    /* Check data is exist? */
    if (!item) {
      return sendTextResponse(
        res,
        StatusCodes.NOT_FOUND,
        `Data id "${id}" does not exist`,
      );
    }

    /* Get tile extra info */
    try {
      const tileBounds = req.body?.tileBounds;

      try {
        validateJSON(await getJSONSchema("tile_bounds"), req.body);
      } catch (error) {
        return sendTextResponse(
          res,
          StatusCodes.BAD_REQUEST,
          `Tile bounds is invalid: ${error}`,
        );
      }

      let extraInfo;
      const isCreated = req.query.type === "created";

      switch (item.sourceType) {
        case "mbtiles": {
          extraInfo = getMBTilesTileExtraInfo({
            source: item.source,
            tileBounds,
            isCreated,
          });

          break;
        }

        case "pmtiles": {
          // Do nothing

          extraInfo = {};

          break;
        }

        case "xyz": {
          extraInfo = getXYZTileExtraInfo({
            source: item.md5Source,
            tileBounds,
            isCreated,
          });

          break;
        }

        case "pg": {
          extraInfo = await getPostgreSQLTileExtraInfo({
            source: item.source,
            tileBounds,
            isCreated,
          });

          break;
        }
      }

      const headers = {
        // "content-disposition": `attachment; filename="extra-info.json"`,
        "content-type": "application/json",
      };

      if (req.query.compression === "true") {
        extraInfo = await gzipAsync(JSON.stringify(extraInfo));

        headers["content-encoding"] = "gzip";
      }

      res.set(headers);

      return res.status(StatusCodes.OK).send(extraInfo);
    } catch (error) {
      printLog(
        "error",
        `Failed to get tile extra info of data id "${id}": ${error}`,
      );

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
      return sendTextResponse(
        res,
        StatusCodes.NOT_FOUND,
        `Data id "${id}" does not exist`,
      );
    }

    /* Calculate tile extra info */
    printLog("info", `Calculating tile extra info "${id}"...`);

    try {
      let calculateTileExtraInfoFunc;

      switch (item.sourceType) {
        case "mbtiles": {
          calculateTileExtraInfoFunc = async () => {
            return calculateMBTilesTileExtraInfo(item.source);
          };

          break;
        }

        case "pmtiles": {
          calculateTileExtraInfoFunc = async () => {};

          break;
        }

        case "xyz": {
          calculateTileExtraInfoFunc = async () => {
            return await calculateXYZTileExtraInfo(item.source, item.md5Source);
          };

          break;
        }

        case "pg": {
          calculateTileExtraInfoFunc = async () => {
            return await calculatePostgreSQLTileExtraInfo(item.source);
          };

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
            `Failed to calculate tile extra info for data id "${id}": ${error}`,
          );
        });

      return res.status(StatusCodes.OK).send("OK");
    } catch (error) {
      printLog(
        "error",
        `Failed to calculate tile extra info for data id "${id}": ${error}`,
      );

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

/**
 * Get tile data list handler
 * @returns {(req: Request, res: Response, next: NextFunction) => Promise<any>}
 */
function getDatasListHandler() {
  return async (req, res) => {
    try {
      const requestHost = getRequestHost(req);

      const result = await Promise.all(
        Object.keys(config.datas).map(async (id) => {
          const { name, center, format } = config.datas[id].tileJSON;

          const data = {
            id,
            name,
            url: `${requestHost}/datas/${id}.json`,
          };

          if (format !== "pbf") {
            const [x, y, z] = getXYZFromLonLatZ(
              center[0],
              center[1],
              center[2],
            );

            data.thumbnail = `${requestHost}/datas/${id}/${z}/${x}/${y}.${format}`;
          }

          return data;
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
     *           schema:
     *             $ref: '#/components/schemas/TileBounds'
     *       description: Exact tile bounds object, limited to 100000 tiles
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
    app.post("/datas/:id/extra-info", getTileDataExtraInfoHandler());

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
     * /datas/{id}/{z}/{x}/{y}.{format}:
     *   get:
     *     tags:
     *       - Data
     *     summary: Get tile data
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
     *           enum: [jpeg, jpg, pbf, png, webp]
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
    app.get("/datas/:id/:z/:x/:y.:format", getTileDataHandler());

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

      await runAllWithLimit(
        ids.map((id) => {
          return async () => {
            const dataInfo = {};

            try {
              const item = config.datas[id];

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
                  dataInfo.path = path.join(
                    process.env.DATA_DIR,
                    "caches/mbtiles",
                    item.mbtiles,
                    `${item.mbtiles}.mbtiles`,
                  );

                  /* Open MBTiles */
                  dataInfo.source = await openMBTilesDB(
                    dataInfo.path,
                    true,
                    DEFAULT_QUERY_TIMEOUT,
                  );

                  /* Get MBTiles metadata */
                  dataInfo.tileJSON = createTileMetadata({
                    ...cacheSource.metadata,
                    cacheCoverages: getTileBounds({
                      coverages: cacheSource.coverages,
                      limitedBBox: cacheSource.metadata.bounds,
                    }).targetCoverages,
                    ...(item.tilejson ?? {}),
                  });
                } else {
                  /* Get MBTiles path */
                  dataInfo.path = path.join(
                    process.env.DATA_DIR,
                    "mbtiles",
                    item.mbtiles,
                  );

                  /* Open MBTiles */
                  dataInfo.source = await openMBTilesDB(
                    dataInfo.path,
                    true,
                    DEFAULT_QUERY_TIMEOUT,
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
                  HTTP_SCHEMES.some((scheme) => {
                    return item.pmtiles.startsWith(scheme);
                  })
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
                  dataInfo.path = path.join(
                    process.env.DATA_DIR,
                    "pmtiles",
                    item.pmtiles,
                  );

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
                  dataInfo.path = path.join(
                    process.env.DATA_DIR,
                    "caches/xyzs",
                    item.xyz,
                  );

                  dataInfo.source = dataInfo.path;

                  /* Open XYZ MD5 */
                  dataInfo.md5Source = await openXYZMD5DB(
                    path.join(dataInfo.path, `${item.xyz}.sqlite`),
                    true,
                  );

                  /* Get XYZ metadata */
                  dataInfo.tileJSON = createTileMetadata({
                    ...cacheSource.metadata,
                    cacheCoverages: getTileBounds({
                      coverages: cacheSource.coverages,
                      limitedBBox: cacheSource.metadata.bounds,
                    }).targetCoverages,
                    ...(item.tilejson ?? {}),
                  });
                } else {
                  /* Get XYZ path */
                  dataInfo.path = path.join(
                    process.env.DATA_DIR,
                    "xyzs",
                    item.xyz,
                  );

                  dataInfo.source = dataInfo.path;

                  /* Open XYZ MD5 */
                  dataInfo.md5Source = await openXYZMD5DB(
                    path.join(dataInfo.path, `${item.xyz}.sqlite`),
                    true,
                    DEFAULT_QUERY_TIMEOUT,
                  );

                  /* Get XYZ metadata */
                  dataInfo.tileJSON = {
                    ...(await getXYZMetadata(
                      dataInfo.source,
                      dataInfo.md5Source,
                    )),
                    ...(item.tilejson ?? {}),
                  };
                }
              } else if (item.pg !== undefined) {
                dataInfo.sourceType = "pg";
                dataInfo.database = item.pg;

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
                  dataInfo.path = `${process.env.POSTGRESQL_BASE_URI}/${item.pg}`;

                  /* Open PostgreSQL */
                  dataInfo.source = await openPostgreSQLDB(
                    dataInfo.path,
                    true,
                    DEFAULT_QUERY_TIMEOUT,
                    {
                      pool: true,
                    },
                  );

                  /* Get PostgreSQL metadata */
                  dataInfo.tileJSON = createTileMetadata({
                    ...cacheSource.metadata,
                    cacheCoverages: getTileBounds({
                      coverages: cacheSource.coverages,
                      limitedBBox: cacheSource.metadata.bounds,
                    }).targetCoverages,
                    ...(item.tilejson ?? {}),
                  });
                } else {
                  /* Get XYZ path */
                  dataInfo.path = `${process.env.POSTGRESQL_BASE_URI}/${item.pg}`;

                  /* Open PostgreSQL */
                  dataInfo.source = await openPostgreSQLDB(
                    dataInfo.path,
                    false,
                    DEFAULT_QUERY_TIMEOUT,
                    {
                      pool: true,
                    },
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
              try {
                if (dataInfo.sourceType === "mbtiles" && dataInfo.source) {
                  closeMBTilesDB(dataInfo.source);
                } else if (
                  dataInfo.sourceType === "xyz" &&
                  dataInfo.md5Source
                ) {
                  closeXYZMD5DB(dataInfo.md5Source);
                } else if (dataInfo.sourceType === "pg" && dataInfo.source) {
                  await closePostgreSQLDB(dataInfo.source);
                }
              } catch (closeError) {
                printLog(
                  "warn",
                  `Failed to close data id "${id}" after load error: ${closeError}`,
                );
              }

              printLog(
                "error",
                `Failed to load data id "${id}": ${error}. Skipping...`,
              );
            }
          };
        }),
        DEFAULT_CONCURRENCY,
      );

      config.datas = repos;
    }
  },
};
