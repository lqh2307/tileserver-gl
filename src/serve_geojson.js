"use strict";

import { StatusCodes } from "http-status-codes";
import { printLog } from "./logger.js";
import { config } from "./config.js";
import { seed } from "./seed.js";
import express from "express";
import {
  validateAndGetGeometryTypes,
  downloadGeoJSONFile,
  getGeoJSONFromURL,
  cacheGeoJSONFile,
  getGeoJSON,
} from "./geojson.js";
import {
  compileTemplate,
  getRequestHost,
  calculateMD5,
  isExistFile,
} from "./utils.js";

/**
 * Serve GeoJSON group handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function serveGeoJSONGroupHandler() {
  return async (req, res, next) => {
    const id = req.params.id;

    try {
      const item = config.repo.geojsons[id];

      if (item === undefined) {
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
      const item = config.repo.geojsons[id];

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
      const item = config.repo.geojsons[id];

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
      const item = config.repo.geojsons[id];

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
      const item = config.repo.geojsons[id];

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

      let geoJSON;

      /* Get geoJSON and Cache if not exist (if use cache) */
      try {
        geoJSON = await getGeoJSON(geoJSONLayer.path, false);
      } catch (error) {
        if (
          geoJSONLayer.sourceURL !== undefined &&
          error.message === "GeoJSON does not exist"
        ) {
          printLog(
            "info",
            `Forwarding GeoJSON "${id}" - To "${geoJSONLayer.sourceURL}"...`
          );

          geoJSON = await getGeoJSONFromURL(
            geoJSONLayer.sourceURL,
            60000, // 1 mins
            false
          );

          if (geoJSONLayer.storeCache === true) {
            printLog(
              "info",
              `Caching GeoJSON "${id}" - File "${geoJSONLayer.path}"...`
            );

            cacheGeoJSONFile(geoJSONLayer.path, geoJSON).catch((error) =>
              printLog(
                "error",
                `Failed to cache GeoJSON "${id}" - File "${geoJSONLayer.path}": ${error}`
              )
            );
          }
        } else {
          throw error;
        }
      }

      res.header("content-type", "application/json");

      return res.status(StatusCodes.OK).send(geoJSON);
    } catch (error) {
      printLog(
        "error",
        `Failed to get GeoJSON group "${id}" - Layer "${req.params.layer}": ${error}`
      );

      if (error.message === "GeoJSON does not exist") {
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
      const item = config.repo.geojsons[id];

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

      /* Get geoJSON MD5 and Add to header */
      const geoJSONData = await getGeoJSON(geoJSONLayer.path, false);

      res.set({
        etag: calculateMD5(geoJSONData),
      });

      return res.status(StatusCodes.OK).send();
    } catch (error) {
      printLog(
        "error",
        `Failed to get md5 GeoJSON group "${id}" - Layer "${req.params.layer}": ${error}`
      );

      if (error.message === "GeoJSON does not exist") {
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
        Object.keys(config.repo.geojsons).map(async (id) => {
          return {
            id: id,
            name: id,
            url: `${requestHost}/geojsons/${id}.json`,
          };
        })
      );

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
  init: () => {
    const app = express().disable("x-powered-by");

    if (process.env.SERVE_FRONT_PAGE !== "false") {
      /**
       * @swagger
       * tags:
       *   - name: GeoJSON
       *     description: GeoJSON related endpoints
       * /geojsons/{id}/:
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
      app.get("/:id/$", serveGeoJSONGroupHandler());

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
      app.get("/:id/:layer/$", serveGeoJSONHandler());
    }

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
    app.get("/geojsons.json", getGeoJSONGroupsListHandler());

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
    app.get("/:id.json", getGeoJSONGroupInfoHandler());

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
    app.get("/:id/:layer.json", getGeoJSONInfoHandler());

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
    app.get("/:id/:layer.geojson", getGeoJSONHandler());

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
    app.get("/:id:/:layer/md5", getGeoJSONMD5Handler());

    return app;
  },

  add: async () => {
    if (config.geojsons === undefined) {
      printLog("info", "No GeoJSON groups in config. Skipping...");
    } else {
      const ids = Object.keys(config.geojsons);

      printLog("info", `Loading ${ids.length} GeoJSON groups...`);

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

                  if (
                    item.geojson.startsWith("https://") === true ||
                    item.geojson.startsWith("http://") === true
                  ) {
                    geojsonInfo.path = `${process.env.DATA_DIR}/geojsons/${id}/geojson.geojson`;

                    /* Download GeoJSON file */
                    if ((await isExistFile(geojsonInfo.path)) === false) {
                      printLog(
                        "info",
                        `Downloading GeoJSON file "${geojsonInfo.path}" - From "${item.geojson}"...`
                      );

                      await downloadGeoJSONFile(
                        item.geojson,
                        geojsonInfo.path,
                        5,
                        300000 // 5 mins
                      );
                    }
                  } else {
                    if (item.cache !== undefined) {
                      geojsonInfo.path = `${process.env.DATA_DIR}/caches/geojsons/${item.geojson}/${item.geojson}.geojson`;

                      const cacheSource = seed.geojsons?.[item.geojson];

                      if (cacheSource === undefined) {
                        throw new Error(
                          `Cache GeoJSON "${item.geojson}" is invalid`
                        );
                      }

                      if (item.cache.forward === true) {
                        geojsonInfo.sourceURL = cacheSource.url;
                        geojsonInfo.storeCache = item.cache.store;
                      }
                    } else {
                      geojsonInfo.path = `${process.env.DATA_DIR}/geojsons/${item.geojson}`;
                    }
                  }

                  /* Load GeoJSON */
                  try {
                    /* Open GeoJSON */
                    const geoJSON = await getGeoJSON(geojsonInfo.path, true);

                    /* Validate and Get GeoJSON info */
                    geojsonInfo.geometryTypes =
                      validateAndGetGeometryTypes(geoJSON);

                    geojsonsInfo[layer] = geojsonInfo;
                  } catch (error) {
                    if (
                      item.cache !== undefined &&
                      error.message === "GeoJSON does not exist"
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
              config.repo.geojsons[id] = geojsonsInfo;
            }
          } catch (error) {
            printLog(
              "error",
              `Failed to load GeoJSON group "${id}": ${error}. Skipping...`
            );
          }
        })
      );
    }
  },
};
