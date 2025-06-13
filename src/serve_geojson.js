"use strict";

import { validateAndGetGeometryTypes, getGeoJSON } from "./geojson.js";
import { getAndCacheDataGeoJSON } from "./data.js";
import { StatusCodes } from "http-status-codes";
import { printLog } from "./logger.js";
import { createReadStream } from "fs";
import { config } from "./config.js";
import { stat } from "fs/promises";
import { seed } from "./seed.js";
import path from "path";
import {
  calculateMD5OfFile,
  compileTemplate,
  getRequestHost,
  isExistFile,
  gzipAsync,
} from "./utils.js";

/**
 * Serve GeoJSON group handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function serveGeoJSONGroupHandler() {
  return async (req, res, next) => {
    const id = req.params.id;

    try {
      if (config.geojsons[id] === undefined) {
        return res
          .status(StatusCodes.NOT_FOUND)
          .send("GeoJSON group does not exist");
      }

      const compiled = await compileTemplate("geojson_data", {
        group: id,
        base_url: getRequestHost(req),
      });

      return res.status(StatusCodes.OK).send(compiled);
    } catch (error) {
      printLog("error", `Failed to serve GeoJSON group "${id}": ${error}`);

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

/**
 * Serve GeoJSON handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function serveGeoJSONHandler() {
  return async (req, res, next) => {
    const id = req.params.id;

    try {
      const item = config.geojsons[id];

      if (item === undefined) {
        return res
          .status(StatusCodes.NOT_FOUND)
          .send("GeoJSON group does not exist");
      }

      if (item[req.params.layer] === undefined) {
        return res
          .status(StatusCodes.NOT_FOUND)
          .send("GeoJSON layer does not exist");
      }

      const compiled = await compileTemplate("geojson_data", {
        group: id,
        layer: req.params.layer,
        base_url: getRequestHost(req),
      });

      return res.status(StatusCodes.OK).send(compiled);
    } catch (error) {
      printLog(
        "error",
        `Failed to serve GeoJSON group "${id}" - Layer "${req.params.layer}": ${error}`
      );

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

/**
 * Get geoJSON group info handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function getGeoJSONGroupInfoHandler() {
  return async (req, res, next) => {
    const id = req.params.id;

    try {
      const item = config.geojsons[id];

      /* Check GeoJSON group is used? */
      if (item === undefined) {
        return res
          .status(StatusCodes.NOT_FOUND)
          .send("GeoJSON group does not exist");
      }

      const requestHost = getRequestHost(req);

      const geojsons = {};

      for (const layer in item) {
        geojsons[layer] = {
          url: `${requestHost}/geojsons/${id}/${layer}.geojson`,
          geometryTypes: item[layer].geometryTypes,
        };
      }

      res.header("content-type", "application/json");

      return res.status(StatusCodes.OK).send({
        id: id,
        name: id,
        geojsons: geojsons,
      });
    } catch (error) {
      printLog("error", `Failed to get GeoJSON group info "${id}": ${error}`);

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

/**
 * Get geoJSON info handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function getGeoJSONInfoHandler() {
  return async (req, res, next) => {
    const id = req.params.id;

    try {
      const item = config.geojsons[id];

      /* Check GeoJSON is used? */
      if (item === undefined) {
        return res
          .status(StatusCodes.NOT_FOUND)
          .send("GeoJSON group does not exist");
      }

      res.header("content-type", "application/json");

      return res.status(StatusCodes.OK).send({
        group: id,
        layer: req.params.layer,
        url: `${getRequestHost(req)}/geojsons/${id}/${
          req.params.layer
        }.geojson`,
        geometryTypes: item[req.params.layer].geometryTypes,
      });
    } catch (error) {
      printLog(
        "error",
        `Failed to get GeoJSON group info "${id}" - Layer "${req.params.layer}": ${error}`
      );

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

/**
 * Get geoJSON handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function getGeoJSONHandler() {
  return async (req, res, next) => {
    const id = req.params.id;

    try {
      const item = config.geojsons[id];

      /* Check GeoJSON is used? */
      if (item === undefined) {
        return res
          .status(StatusCodes.NOT_FOUND)
          .send("GeoJSON group does not exist");
      }

      const geoJSONLayer = item[req.params.layer];

      /* Check GeoJSON layer is used? */
      if (geoJSONLayer === undefined) {
        return res
          .status(StatusCodes.NOT_FOUND)
          .send("GeoJSON layer does not exist");
      }

      /* Get and cache GeoJSON */
      let geoJSON = await getAndCacheDataGeoJSON(id, req.params.layer);

      const headers = {
        "content-type": "application/json",
      };

      if (req.query.compression === "true") {
        geoJSON = await gzipAsync(geoJSON);

        headers["content-encoding"] = "gzip";
      }

      res.set(headers);

      return res.status(StatusCodes.OK).send(geoJSON);
    } catch (error) {
      printLog(
        "error",
        `Failed to get GeoJSON group "${id}" - Layer "${req.params.layer}": ${error}`
      );

      if (error.message === "JSON does not exist") {
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
 * Get geoJSON MD5 handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function getGeoJSONMD5Handler() {
  return async (req, res, next) => {
    const id = req.params.id;

    try {
      const item = config.geojsons[id];

      /* Check GeoJSON is used? */
      if (item === undefined) {
        return res
          .status(StatusCodes.NOT_FOUND)
          .send("GeoJSON group does not exist");
      }

      const geoJSONLayer = item[req.params.layer];

      /* Check GeoJSON layer is used? */
      if (geoJSONLayer === undefined) {
        return res
          .status(StatusCodes.NOT_FOUND)
          .send("GeoJSON layer does not exist");
      }

      /* Calculate MD5 and Add to header */
      res.set({
        etag: await calculateMD5OfFile(geoJSONLayer.path),
      });

      return res.status(StatusCodes.OK).send();
    } catch (error) {
      printLog(
        "error",
        `Failed to get md5 of GeoJSON group "${id}" - Layer "${req.params.layer}": ${error}`
      );

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
 * Download geoJSON handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function downloadGeoJSONHandler() {
  return async (req, res, next) => {
    const id = req.params.id;

    try {
      const item = config.geojsons[id];

      /* Check GeoJSON is used? */
      if (item === undefined) {
        return res
          .status(StatusCodes.NOT_FOUND)
          .send("GeoJSON group does not exist");
      }

      const geoJSONLayer = item[req.params.layer];

      /* Check GeoJSON layer is used? */
      if (geoJSONLayer === undefined) {
        return res
          .status(StatusCodes.NOT_FOUND)
          .send("GeoJSON layer does not exist");
      }

      if ((await isExistFile(geoJSONLayer.path))) {
        const stats = await stat(geoJSONLayer.path);
        const fileName = path.basename(geoJSONLayer.path);

        res.set({
          "content-length": stats.size,
          "content-disposition": `attachment; filename="${fileName}`,
          "content-type": "application/json",
        });

        const readStream = createReadStream(geoJSONLayer.path);

        readStream.pipe(res);

        readStream.on("error", (error) => {
          throw error;
        });
      } else {
        throw new Error("File does not exist");
      }
    } catch (error) {
      printLog(
        "error",
        `Failed to get GeoJSON group "${id}" - Layer "${req.params.layer}": ${error}`
      );

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
 * Get GeoJSON group list handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function getGeoJSONGroupsListHandler() {
  return async (req, res, next) => {
    try {
      const requestHost = getRequestHost(req);

      const result = await Promise.all(
        Object.keys(config.geojsons).map(async (id) => {
          return {
            id: id,
            name: id,
            url: `${requestHost}/geojsons/${id}.json`,
          };
        })
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
      printLog("error", `Failed to get GeoJSON groups": ${error}`);

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

export const serve_geojson = {
  /**
   * Register geojson handlers
   * @param {Express} app Express object
   * @returns {void}
   */
  init: (app) => {
    /**
     * @swagger
     * tags:
     *   - name: GeoJSON
     *     description: GeoJSON related endpoints
     * /geojsons/geojsons.json:
     *   get:
     *     tags:
     *       - GeoJSON
     *     summary: Get all GeoJSON groups
     *     parameters:
     *       - in: query
     *         name: compression
     *         schema:
     *           type: boolean
     *         required: false
     *         description: Compressed response
     *     responses:
     *       200:
     *         description: List of all GeoJSON groups
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
    app.get("/geojsons/geojsons.json", getGeoJSONGroupsListHandler());

    /**
     * @swagger
     * tags:
     *   - name: GeoJSON
     *     description: GeoJSON related endpoints
     * /geojsons/{id}.json:
     *   get:
     *     tags:
     *       - GeoJSON
     *     summary: Get geoJSON group info
     *     parameters:
     *       - in: path
     *         name: id
     *         schema:
     *           type: string
     *           example: id
     *         required: true
     *         description: ID of the GeoJSON group
     *     responses:
     *       200:
     *         description: GeoJSON group info
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
    app.get("/geojsons/:id.json", getGeoJSONGroupInfoHandler());

    /**
     * @swagger
     * tags:
     *   - name: GeoJSON
     *     description: GeoJSON related endpoints
     * /geojsons/{id}/{layer}.json:
     *   get:
     *     tags:
     *       - GeoJSON
     *     summary: Get geoJSON info
     *     parameters:
     *       - in: path
     *         name: id
     *         schema:
     *           type: string
     *           example: id
     *         required: true
     *         description: ID of the GeoJSON
     *       - in: path
     *         name: layer
     *         schema:
     *           type: string
     *           example: layer
     *         required: true
     *         description: Layer of the GeoJSON
     *     responses:
     *       200:
     *         description: GeoJSON info
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
    app.get("/geojsons/:id/:layer.json", getGeoJSONInfoHandler());

    /**
     * @swagger
     * tags:
     *   - name: GeoJSON
     *     description: GeoJSON related endpoints
     * /geojsons/{id}/{layer}.geojson:
     *   get:
     *     tags:
     *       - GeoJSON
     *     summary: Get geoJSON
     *     parameters:
     *       - in: path
     *         name: id
     *         schema:
     *           type: string
     *           example: id
     *         required: true
     *         description: ID of the GeoJSON
     *       - in: path
     *         name: layer
     *         schema:
     *           type: string
     *           example: layer
     *         required: true
     *         description: Layer of the GeoJSON
     *       - in: query
     *         name: compression
     *         schema:
     *           type: boolean
     *         required: false
     *         description: Compressed response
     *     responses:
     *       200:
     *         description: GeoJSON
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
    app.get("/geojsons/:id/:layer.geojson", getGeoJSONHandler());

    /**
     * @swagger
     * tags:
     *   - name: GeoJSON
     *     description: GeoJSON related endpoints
     * /geojsons/{id}/{layer}/md5:
     *   get:
     *     tags:
     *       - GeoJSON
     *     summary: Get geoJSON MD5
     *     parameters:
     *       - in: path
     *         name: id
     *         schema:
     *           type: string
     *           example: id
     *         required: true
     *         description: ID of the GeoJSON
     *       - in: path
     *         name: layer
     *         schema:
     *           type: string
     *           example: layer
     *         required: true
     *         description: Layer of the GeoJSON
     *     responses:
     *       200:
     *         description: GeoJSON MD5
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
    app.get("/geojsons/:id/:layer/md5", getGeoJSONMD5Handler());

    /**
     * @swagger
     * tags:
     *   - name: GeoJSON
     *     description: GeoJSON related endpoints
     * /geojsons/{id}/{layer}/download:
     *   get:
     *     tags:
     *       - GeoJSON
     *     summary: Download geoJSON file
     *     parameters:
     *       - in: path
     *         name: id
     *         schema:
     *           type: string
     *           example: id
     *         required: true
     *         description: ID of the GeoJSON
     *       - in: path
     *         name: layer
     *         schema:
     *           type: string
     *           example: layer
     *         required: true
     *         description: Layer of the GeoJSON
     *     responses:
     *       200:
     *         description: GeoJSON file
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
    app.get("/geojsons/:id/:layer/download", downloadGeoJSONHandler());

    /* Serve GeoJSON */
    if (process.env.SERVE_FRONT_PAGE !== "false") {
      /**
       * @swagger
       * tags:
       *   - name: GeoJSON
       *     description: GeoJSON related endpoints
       * /geojsons/{id}/{layer}:
       *   get:
       *     tags:
       *       - GeoJSON
       *     summary: Serve GeoJSON page
       *     parameters:
       *       - in: path
       *         name: id
       *         schema:
       *           type: string
       *           example: id
       *         required: true
       *         description: ID of the geojson
       *       - in: path
       *         name: layer
       *         schema:
       *           type: string
       *           example: layer
       *         required: true
       *         description: Layer of the GeoJSON
       *     responses:
       *       200:
       *         description: GeoJSON page
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
      app.get("/geojsons/:id/:layer", serveGeoJSONHandler());

      /**
       * @swagger
       * tags:
       *   - name: GeoJSON
       *     description: GeoJSON related endpoints
       * /geojsons/{id}:
       *   get:
       *     tags:
       *       - GeoJSON
       *     summary: Serve GeoJSON group page
       *     parameters:
       *       - in: path
       *         name: id
       *         schema:
       *           type: string
       *           example: id
       *         required: true
       *         description: ID of the GeoJSON group
       *     responses:
       *       200:
       *         description: GeoJSON group page
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
      app.get("/geojsons/:id", serveGeoJSONGroupHandler());
    }
  },

  /**
   * Add geojson
   * @returns {void}
   */
  add: async () => {
    if (config.geojsons === undefined) {
      printLog("info", "No GeoJSON groups in config. Skipping...");
    } else {
      const ids = Object.keys(config.geojsons);

      printLog("info", `Loading ${ids.length} GeoJSON groups...`);

      const repos = {};

      await Promise.all(
        ids.map(async (id) => {
          try {
            if (config.geojsons[id] === undefined) {
              printLog(
                "info",
                `No geojson group in GeoJSON groups id "${id}". Skipping...`
              );
            } else {
              const layers = Object.keys(config.geojsons[id]);

              printLog(
                "info",
                `Loading ${layers.length} GeoJSON in GeoJSON groups id "${id}"...`
              );

              const geojsonsInfo = {};

              /* Get GeoJSON infos */
              await Promise.all(
                layers.map(async (layer) => {
                  const item = config.geojsons[id][layer];

                  /* Get GeoJSON path */
                  const geojsonInfo = {};

                  if (item.cache !== undefined) {
                    geojsonInfo.path = `${process.env.DATA_DIR}/caches/geojsons/${item.geojson}/${item.geojson}.geojson`;

                    const cacheSource = seed.geojsons?.[item.geojson];

                    if (cacheSource === undefined) {
                      throw new Error(
                        `Cache GeoJSON "${item.geojson}" is invalid`
                      );
                    }

                    if (item.cache.forward) {
                      geojsonInfo.sourceURL = cacheSource.url;
                      geojsonInfo.storeCache = item.cache.store;
                    }
                  } else {
                    geojsonInfo.path = `${process.env.DATA_DIR}/geojsons/${item.geojson}`;
                  }

                  /* Load GeoJSON */
                  try {
                    /* Open GeoJSON */
                    const geoJSON = JSON.parse(
                      await getGeoJSON(geojsonInfo.path)
                    );

                    /* Validate and Get GeoJSON info */
                    geojsonInfo.geometryTypes =
                      validateAndGetGeometryTypes(geoJSON);

                    geojsonsInfo[layer] = geojsonInfo;
                  } catch (error) {
                    if (
                      item.cache !== undefined &&
                      error.message === "JSON does not exist"
                    ) {
                      geojsonInfo.geometryTypes = ["polygon", "line", "circle"];

                      geojsonsInfo[layer] = geojsonInfo;
                    } else {
                      throw error;
                    }
                  }
                })
              );

              /* Add to repo */
              repos[id] = geojsonsInfo;
            }
          } catch (error) {
            printLog(
              "error",
              `Failed to load GeoJSON group "${id}": ${error}. Skipping...`
            );
          }
        })
      );

      config.geojsons = repos;
    }
  },
};
