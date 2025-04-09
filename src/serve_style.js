"use strict";

import { StatusCodes } from "http-status-codes";
import { printLog } from "./logger.js";
import { config } from "./config.js";
import { seed } from "./seed.js";
import {
  compileTemplate,
  isLocalTileURL,
  getRequestHost,
  getJSONSchema,
  validateJSON,
  isExistFile,
} from "./utils.js";
import {
  getStyleJSONFromURL,
  downloadStyleFile,
  cacheStyleFile,
  validateStyle,
  getStyle,
} from "./style.js";
import {
  createRenderedMetadata,
  renderPostgreSQLTiles,
  renderMBTilesTiles,
  renderXYZTiles,
  renderImage,
} from "./image.js";
import os from "os";

/**
 * Serve style handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function serveStyleHandler() {
  return async (req, res, next) => {
    const id = req.params.id;

    try {
      const item = config.styles[id];

      if (item === undefined) {
        return res.status(StatusCodes.NOT_FOUND).send("Style does not exist");
      }

      const compiled = await compileTemplate("viewer", {
        id: id,
        name: item.name,
        base_url: getRequestHost(req),
      });

      return res.status(StatusCodes.OK).send(compiled);
    } catch (error) {
      printLog("error", `Failed to serve style "${id}": ${error}`);

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

/**
 * Serve WMTS handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function serveWMTSHandler() {
  return async (req, res, next) => {
    const id = req.params.id;

    try {
      const item = config.styles[id].rendered;

      if (item === undefined) {
        return res.status(StatusCodes.NOT_FOUND).send("WMTS does not exist");
      }

      const compiled = await compileTemplate("wmts", {
        id: id,
        name: item.tileJSON.name,
        base_url: getRequestHost(req),
      });

      res.header("content-type", "text/xml");

      return res.status(StatusCodes.OK).send(compiled);
    } catch (error) {
      printLog("error", `Failed to serve WMTS "${id}": ${error}`);

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

/**
 * Get styleJSON handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function getStyleHandler() {
  return async (req, res, next) => {
    const id = req.params.id;

    try {
      const item = config.styles[id];

      /* Check style is used? */
      if (item === undefined) {
        return res.status(StatusCodes.NOT_FOUND).send("Style does not exist");
      }

      let styleJSON;

      /* Get styleJSON and cache if not exist if use cache */
      try {
        styleJSON = await getStyle(item.path, false);
      } catch (error) {
        if (
          item.sourceURL !== undefined &&
          error.message === "Style does not exist"
        ) {
          printLog(
            "info",
            `Forwarding style "${id}" - To "${item.sourceURL}"...`
          );

          styleJSON = await getStyleJSONFromURL(
            item.sourceURL,
            60000, // 1 mins
            false
          );

          if (item.storeCache === true) {
            printLog("info", `Caching style "${id}" - File "${item.path}"...`);

            cacheStyleFile(item.path, styleJSON).catch((error) =>
              printLog(
                "error",
                `Failed to cache style "${id}" - File "${item.path}": ${error}`
              )
            );
          }
        } else {
          throw error;
        }
      }

      if (req.query.raw !== "true") {
        styleJSON = JSON.parse(styleJSON);

        const requestHost = getRequestHost(req);

        /* Fix sprite url */
        if (styleJSON.sprite !== undefined) {
          if (styleJSON.sprite.startsWith("sprites://") === true) {
            styleJSON.sprite = styleJSON.sprite.replace(
              "sprites://",
              `${requestHost}/sprites/`
            );
          }
        }

        /* Fix fonts url */
        if (styleJSON.glyphs !== undefined) {
          if (styleJSON.glyphs.startsWith("fonts://") === true) {
            styleJSON.glyphs = styleJSON.glyphs.replace(
              "fonts://",
              `${requestHost}/fonts/`
            );
          }
        }

        /* Fix source urls */
        await Promise.all(
          Object.keys(styleJSON.sources).map(async (id) => {
            const source = styleJSON.sources[id];

            // Fix tileJSON URL
            if (source.url !== undefined) {
              if (isLocalTileURL(source.url) === true) {
                const sourceID = source.url.split("/")[2];

                source.url = `${requestHost}/datas/${sourceID}.json`;
              }
            }

            // Fix tileJSON URLs
            if (source.urls !== undefined) {
              const urls = new Set(
                source.urls.map((url) => {
                  if (isLocalTileURL(url) === true) {
                    const sourceID = url.split("/")[2];

                    url = `${requestHost}/datas/${sourceID}.json`;
                  }

                  return url;
                })
              );

              source.urls = Array.from(urls);
            }

            // Fix tile URL
            if (source.tiles !== undefined) {
              const tiles = new Set(
                source.tiles.map((tile) => {
                  if (isLocalTileURL(tile) === true) {
                    const sourceID = tile.split("/")[2];
                    const sourceData = config.datas[sourceID];

                    tile = `${requestHost}/datas/${sourceID}/{z}/{x}/{y}.${sourceData.tileJSON.format}`;
                  }

                  return tile;
                })
              );

              source.tiles = Array.from(tiles);
            }
          })
        );
      }

      res.header("content-type", "application/json");

      return res.status(StatusCodes.OK).send(styleJSON);
    } catch (error) {
      printLog("error", `Failed to get style "${id}": ${error}`);

      if (error.message === "Style does not exist") {
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
 * Render style handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function renderStyleHandler() {
  return async (req, res, next) => {
    const id = req.params.id;

    try {
      const item = config.styles[id];

      /* Check rendered is exist? */
      if (item === undefined || item.rendered === undefined) {
        return res
          .status(StatusCodes.NOT_FOUND)
          .send("Rendered does not exist");
      }

      if (req.query.cancel === "true") {
        /* Check export is not running? (export === true is not running) */
        if (item.rendered.export === true) {
          printLog(
            "warn",
            "No render is currently running. Skipping cancel render..."
          );

          return res.status(StatusCodes.NOT_FOUND).send("OK");
        } else {
          printLog("info", "Canceling render...");

          item.rendered.export = true;

          return res.status(StatusCodes.OK).send("OK");
        }
      } else {
        /* Check export is running? (export === false is not running) */
        if (item.rendered.export === false) {
          printLog("warn", "A render is already running. Skipping render...");

          return res.status(StatusCodes.CONFLICT).send("OK");
        } else {
          /* Render style */
          try {
            validateJSON(await getJSONSchema("render"), req.body);
          } catch (error) {
            return res
              .status(StatusCodes.BAD_REQUEST)
              .send(`Options is invalid: ${error}`);
          }

          const defaultTileScale = 1;
          const defaultTileSize = 256;
          const defaultConcurrency = os.cpus().length;
          const defaultStoreTransparent = false;
          const defaultCreateOverview = true;

          setTimeout(() => {
            item.rendered.export = false;

            if (req.body.storeType === "xyz") {
              renderXYZTiles(
                id,
                req.body.metadata,
                req.body.tileScale || defaultTileScale,
                req.body.tileSize || defaultTileSize,
                req.body.bbox,
                req.body.maxzoom,
                req.body.concurrency || defaultConcurrency,
                req.body.storeTransparent || defaultStoreTransparent,
                req.body.createOverview || defaultCreateOverview,
                req.body.refreshBefore?.time ||
                  req.body.refreshBefore?.day ||
                  req.body.refreshBefore?.md5
              )
                .catch((error) => {
                  printLog("error", `Failed to render style "${id}": ${error}`);
                })
                .finally(() => {
                  item.rendered.export = true;
                });
            } else if (req.body.storeType === "mbtiles") {
              renderMBTilesTiles(
                id,
                req.body.metadata,
                req.body.tileScale || defaultTileScale,
                req.body.tileSize || defaultTileSize,
                req.body.bbox,
                req.body.maxzoom,
                req.body.concurrency || defaultConcurrency,
                req.body.storeTransparent || defaultStoreTransparent,
                req.body.createOverview || defaultCreateOverview,
                req.body.refreshBefore?.time ||
                  req.body.refreshBefore?.day ||
                  req.body.refreshBefore?.md5
              )
                .catch((error) => {
                  printLog("error", `Failed to render style "${id}": ${error}`);
                })
                .finally(() => {
                  item.rendered.export = true;
                });
            } else if (req.body.storeType === "pg") {
              renderPostgreSQLTiles(
                id,
                req.body.metadata,
                req.body.tileScale || defaultTileScale,
                req.body.tileSize || defaultTileSize,
                req.body.bbox,
                req.body.maxzoom,
                req.body.concurrency || defaultConcurrency,
                req.body.storeTransparent || defaultStoreTransparent,
                req.body.createOverview || defaultCreateOverview,
                req.body.refreshBefore?.time ||
                  req.body.refreshBefore?.day ||
                  req.body.refreshBefore?.md5
              )
                .catch((error) => {
                  printLog("error", `Failed to render style "${id}": ${error}`);
                })
                .finally(() => {
                  item.rendered.export = true;
                });
            }
          }, 0);

          return res.status(StatusCodes.CREATED).send("OK");
        }
      }
    } catch (error) {
      printLog("error", `Failed to render style "${id}": ${error}`);

      if (error instanceof SyntaxError) {
        return res
          .status(StatusCodes.BAD_REQUEST)
          .send("option parameter is invalid");
      } else {
        return res
          .status(StatusCodes.INTERNAL_SERVER_ERROR)
          .send("Internal server error");
      }
    }
  };
}

/**
 * Get style list handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function getStylesListHandler() {
  return async (req, res, next) => {
    try {
      const requestHost = getRequestHost(req);

      const result = await Promise.all(
        Object.keys(config.styles).map(async (id) => {
          return {
            id: id,
            name: config.styles[id].name,
            url: `${requestHost}/styles/${id}/style.json`,
          };
        })
      );

      return res.status(StatusCodes.OK).send(result);
    } catch (error) {
      printLog("error", `Failed to get styles": ${error}`);

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

/**
 * Get rendered tile handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function getRenderedTileHandler() {
  return async (req, res, next) => {
    if (
      req.params.tileSize !== undefined &&
      ["256", "512"].includes(req.params.tileSize) === false
    ) {
      return res
        .status(StatusCodes.BAD_REQUEST)
        .send("Tile size is not support");
    }

    const id = req.params.id;
    const item = config.styles[id];

    /* Check rendered is exist? */
    if (item === undefined || item.rendered === undefined) {
      return res.status(StatusCodes.NOT_FOUND).send("Rendered does not exist");
    }

    /* Get and check rendered tile scale (Default: 1). Ex: @2x -> 2 */
    const tileScale = Number(req.params.tileScale?.slice(1, -1)) || 1;

    /* Get tile size (Default: 256px x 256px) */
    const z = Number(req.params.z);
    const x = Number(req.params.x);
    const y = Number(req.params.y);
    const tileSize = Number(req.params.tileSize) || 256;

    /* Render tile */
    try {
      const image = await renderImage(
        tileScale,
        tileSize,
        item.rendered.compressionLevel,
        item.rendered.styleJSON,
        z,
        x,
        y
      );

      res.header("content-type", "image/png");

      return res.status(StatusCodes.OK).send(image);
    } catch (error) {
      printLog(
        "error",
        `Failed to get rendered "${id}" - Tile "${z}/${x}/${y}: ${error}`
      );

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

/**
 * Get rendered tileJSON handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function getRenderedHandler() {
  return async (req, res, next) => {
    if (
      req.params.tileSize !== undefined &&
      ["256", "512"].includes(req.params.tileSize) === false
    ) {
      return res
        .status(StatusCodes.BAD_REQUEST)
        .send("Tile size is not support");
    }

    const id = req.params.id;

    try {
      const item = config.styles[id];

      /* Check rendered is exist? */
      if (item === undefined || item.rendered === undefined) {
        return res
          .status(StatusCodes.NOT_FOUND)
          .send("Rendered does not exist");
      }

      const requestHost = getRequestHost(req);

      res.header("content-type", "application/json");

      /* Get render info */
      return res.status(StatusCodes.OK).send({
        ...item.rendered.tileJSON,
        tilejson: "2.2.0",
        scheme: "xyz",
        id: id,
        tiles: [
          req.params.tileSize === undefined
            ? `${requestHost}/styles/${id}/{z}/{x}/{y}.png`
            : `${requestHost}/styles/${id}/${req.params.tileSize}/{z}/{x}/{y}.png`,
        ],
      });
    } catch (error) {
      printLog("error", `Failed to get rendered "${id}": ${error}`);

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

/**
 * Get rendered list handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function getRenderedsListHandler() {
  return async (req, res, next) => {
    try {
      const requestHost = getRequestHost(req);

      const result = [];

      Object.keys(config.styles).map((id) => {
        const item = config.styles[id].rendered;

        if (item !== undefined) {
          result.push({
            id: id,
            name: item.tileJSON.name,
            url: [
              `${requestHost}/styles/256/${id}.json`,
              `${requestHost}/styles/512/${id}.json`,
            ],
          });
        }
      });

      return res.status(StatusCodes.OK).send(result);
    } catch (error) {
      printLog("error", `Failed to get rendereds": ${error}`);

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

/**
 * Get styleJSON list handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function getStyleJSONsListHandler() {
  return async (req, res, next) => {
    try {
      const requestHost = getRequestHost(req);

      const result = await Promise.all(
        Object.keys(config.styles).map(async (id) => {
          const item = config.styles[id];

          /* Get styleJSON and cache if not exist if use cache */
          let styleJSON;

          try {
            styleJSON = await getStyle(item.path, true);
          } catch (error) {
            if (
              item.sourceURL !== undefined &&
              error.message === "Style does not exist"
            ) {
              printLog(
                "info",
                `Forwarding style "${id}" - To "${item.sourceURL}"...`
              );

              styleJSON = await getStyleJSONFromURL(
                item.sourceURL,
                60000, // 1 mins
                true
              );

              if (item.storeCache === true) {
                printLog(
                  "info",
                  `Caching style "${id}" - File "${item.path}"...`
                );

                cacheStyleFile(item.path, JSON.stringify(styleJSON)).catch(
                  (error) =>
                    printLog(
                      "error",
                      `Failed to cache style "${id}" - File "${item.path}": ${error}`
                    )
                );
              }
            } else {
              throw error;
            }
          }

          if (req.query.raw !== "true") {
            /* Fix sprite url */
            if (styleJSON.sprite !== undefined) {
              if (styleJSON.sprite.startsWith("sprites://") === true) {
                styleJSON.sprite = styleJSON.sprite.replace(
                  "sprites://",
                  `${requestHost}/sprites/`
                );
              }
            }

            /* Fix fonts url */
            if (styleJSON.glyphs !== undefined) {
              if (styleJSON.glyphs.startsWith("fonts://") === true) {
                styleJSON.glyphs = styleJSON.glyphs.replace(
                  "fonts://",
                  `${requestHost}/fonts/`
                );
              }
            }

            /* Fix source urls */
            await Promise.all(
              Object.keys(styleJSON.sources).map(async (id) => {
                const source = styleJSON.sources[id];

                // Fix tileJSON URL
                if (source.url !== undefined) {
                  if (isLocalTileURL(source.url) === true) {
                    const sourceID = source.url.split("/")[2];

                    source.url = `${requestHost}/datas/${sourceID}.json`;
                  }
                }

                // Fix tileJSON URLs
                if (source.urls !== undefined) {
                  const urls = new Set(
                    source.urls.map((url) => {
                      if (isLocalTileURL(url) === true) {
                        const sourceID = url.split("/")[2];

                        url = `${requestHost}/datas/${sourceID}.json`;
                      }

                      return url;
                    })
                  );

                  source.urls = Array.from(urls);
                }

                // Fix tile URL
                if (source.tiles !== undefined) {
                  const tiles = new Set(
                    source.tiles.map((tile) => {
                      if (isLocalTileURL(tile) === true) {
                        const sourceID = tile.split("/")[2];
                        const sourceData = config.datas[sourceID];

                        tile = `${requestHost}/datas/${sourceID}/{z}/{x}/{y}.${sourceData.tileJSON.format}`;
                      }

                      return tile;
                    })
                  );

                  source.tiles = Array.from(tiles);
                }
              })
            );
          }

          return styleJSON;
        })
      );

      return res.status(StatusCodes.OK).send(result);
    } catch (error) {
      printLog("error", `Failed to get styleJSONs": ${error}`);

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

/**
 * Get rendered tileJSON list handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function getRenderedTileJSONsListHandler() {
  return async (req, res, next) => {
    try {
      const requestHost = getRequestHost(req);

      const result = [];

      Object.keys(config.styles).map((id) => {
        const item = config.styles[id].rendered;

        if (item !== undefined) {
          result.push({
            ...item.tileJSON,
            id: id,
            tilejson: "2.2.0",
            scheme: "xyz",
            tiles: [`${requestHost}/styles/${id}/{z}/{x}/{y}.png`],
          });
        }
      });

      return res.status(StatusCodes.OK).send(result);
    } catch (error) {
      printLog("error", `Failed to get rendered tileJSONs": ${error}`);

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

export const serve_style = {
  /**
   * Register style handlers
   * @param {Express} app Express object
   * @returns {void}
   */
  init: (app) => {
    /**
     * @swagger
     * tags:
     *   - name: Style
     *     description: Style related endpoints
     * /styles/styles.json:
     *   get:
     *     tags:
     *       - Style
     *     summary: Get all styles
     *     responses:
     *       200:
     *         description: List of all styles
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
    app.get("/styles/styles.json", getStylesListHandler());

    /**
     * @swagger
     * tags:
     *   - name: Style
     *     description: Style related endpoints
     * /styles/stylejsons.json:
     *   get:
     *     tags:
     *       - Style
     *     summary: Get all styleJSONs
     *     parameters:
     *       - in: query
     *         name: raw
     *         schema:
     *           type: boolean
     *         required: false
     *         description: Use raw
     *     responses:
     *       200:
     *         description: List of all styleJSONs
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
    app.get("/styles/stylejsons.json", getStyleJSONsListHandler());

    if (config.enableBackendRender === true) {
      if (process.env.ENABLE_EXPORT !== "false") {
        /**
         * @swagger
         * tags:
         *   - name: Style
         *     description: Style related endpoints
         * /styles/{id}/export:
         *   get:
         *     tags:
         *       - Style
         *     summary: Cancel render style
         *     parameters:
         *       - in: query
         *         name: cancel
         *         schema:
         *           type: boolean
         *         required: false
         *         description: Cancel render
         *     responses:
         *       200:
         *         description: Style render is canceled
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
         *       - Style
         *     summary: Render style
         *     requestBody:
         *       required: true
         *       content:
         *         application/json:
         *             schema:
         *               type: object
         *               example: {}
         *       description: Style render options
         *     responses:
         *       201:
         *         description: Style render is started
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
        app.get("/styles/:id/export", renderStyleHandler());
        app.post("/styles/:id/export", renderStyleHandler());
      }

      /**
       * @swagger
       * tags:
       *   - name: Style
       *     description: Style related endpoints
       * /styles/{id}/style.json:
       *   get:
       *     tags:
       *       - Style
       *     summary: Get styleJSON
       *     parameters:
       *       - in: path
       *         name: id
       *         schema:
       *           type: string
       *           example: id
       *         required: true
       *         description: ID of the style
       *       - in: query
       *         name: raw
       *         schema:
       *           type: boolean
       *         required: false
       *         description: Use raw
       *     responses:
       *       200:
       *         description: StyleJSON
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
      app.get("/styles/:id/style.json", getStyleHandler());

      /**
       * @swagger
       * tags:
       *   - name: Style
       *     description: Style related endpoints
       * /styles/{id}/wmts.xml:
       *   get:
       *     tags:
       *       - Style
       *     summary: Get WMTS XML for style
       *     parameters:
       *       - in: path
       *         name: id
       *         schema:
       *           type: string
       *           example: id
       *         required: true
       *         description: ID of the style
       *     responses:
       *       200:
       *         description: WMTS XML for the style
       *         content:
       *           text/xml:
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
      app.get("/styles/:id/wmts.xml", serveWMTSHandler());

      /**
       * @swagger
       * tags:
       *   - name: Rendered
       *     description: Rendered related endpoints
       * /styles/rendereds.json:
       *   get:
       *     tags:
       *       - Rendered
       *     summary: Get all style rendereds
       *     parameters:
       *       - in: path
       *         name: tileSize
       *         schema:
       *           type: integer
       *           enum: [256, 512]
       *         required: false
       *         description: Tile size (256 or 512)
       *     responses:
       *       200:
       *         description: List of all style rendereds
       *         content:
       *           application/json:
       *             schema:
       *               type: array
       *               items:
       *                 type: object
       *                 properties:
       *                   id:
       *                     type: string
       *                     example: style1
       *                   name:
       *                     type: string
       *                     example: Style 1
       *                   url:
       *                     type: array
       *                     items:
       *                       type: string
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
      app.get("/styles/rendereds.json", getRenderedsListHandler());

      /**
       * @swagger
       * tags:
       *   - name: Rendered
       *     description: Rendered related endpoints
       * /styles/tilejsons.json:
       *   get:
       *     tags:
       *       - Rendered
       *     summary: Get all rendered tileJSONs
       *     responses:
       *       200:
       *         description: List of all rendered tileJSONs
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
      app.get("/styles/tilejsons.json", getRenderedTileJSONsListHandler());

      /**
       * @swagger
       * tags:
       *   - name: Rendered
       *     description: Rendered related endpoints
       * /styles/{tileSize}/{id}.json:
       *   get:
       *     tags:
       *       - Rendered
       *     summary: Get style rendered
       *     parameters:
       *       - in: path
       *         name: tileSize
       *         schema:
       *           type: integer
       *           enum: [256, 512]
       *           example: 256
       *         required: false
       *         description: Tile size (256 or 512)
       *       - in: path
       *         name: id
       *         schema:
       *           type: string
       *           example: id
       *         required: true
       *         description: ID of the style rendered
       *     responses:
       *       200:
       *         description: Style rendered
       *         content:
       *           application/json:
       *             schema:
       *               type: object
       *               properties:
       *                 tileJSON:
       *                   type: object
       *                 tiles:
       *                   type: array
       *                   items:
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
      app.get("/styles/{:tileSize/}:id.json", getRenderedHandler());

      /**
       * @swagger
       * tags:
       *   - name: Rendered
       *     description: Rendered related endpoints
       * /styles/{id}/{tileSize}/{z}/{x}/{y}{tileScale}.png:
       *   get:
       *     tags:
       *       - Rendered
       *     summary: Get style rendered tile
       *     parameters:
       *       - in: path
       *         name: id
       *         schema:
       *           type: string
       *           example: id
       *         required: true
       *         description: ID of the style
       *       - in: path
       *         name: tileSize
       *         schema:
       *           type: integer
       *           enum: [256, 512]
       *           example: 256
       *         required: false
       *         description: Tile size (256 or 512)
       *       - in: path
       *         name: z
       *         schema:
       *           type: integer
       *         required: true
       *         description: Zoom level
       *       - in: path
       *         name: x
       *         schema:
       *           type: integer
       *         required: true
       *         description: X coordinate
       *       - in: path
       *         name: y
       *         schema:
       *           type: integer
       *         required: true
       *         description: Y coordinate
       *       - in: path
       *         name: tileScale
       *         schema:
       *           type: string
       *         required: false
       *         description: Scale of the tile (e.g., @2x)
       *     responses:
       *       200:
       *         description: Style tile
       *         content:
       *           image/png:
       *             schema:
       *               type: string
       *               format: binary
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
      app.get(
        "/styles/:id{/:tileSize}/:z/:x/:y{:tileScale}.png",
        getRenderedTileHandler()
      );
    }

    if (process.env.SERVE_FRONT_PAGE !== "false") {
      /**
       * @swagger
       * tags:
       *   - name: Style
       *     description: Style related endpoints
       * /styles/{id}/:
       *   get:
       *     tags:
       *       - Style
       *     summary: Serve style page
       *     parameters:
       *       - in: path
       *         name: id
       *         schema:
       *           type: string
       *           example: id
       *         required: true
       *         description: ID of the style
       *     responses:
       *       200:
       *         description: Style page
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
      app.get("/styles/:id", serveStyleHandler());
    }
  },

  add: async () => {
    if (config.styles === undefined) {
      printLog("info", "No styles in config. Skipping...");
    } else {
      const ids = Object.keys(config.styles);

      printLog("info", `Loading ${ids.length} styles...`);

      const repos = {};

      await Promise.all(
        ids.map(async (id) => {
          const item = config.styles[id];

          let isCanServeRendered = false;

          const styleInfo = {};

          let styleJSON;

          /* Serve style */
          try {
            if (
              item.style.startsWith("https://") === true ||
              item.style.startsWith("http://") === true
            ) {
              styleInfo.path = `${process.env.DATA_DIR}/styles/${id}/style.json`;

              /* Download style.json file */
              if ((await isExistFile(styleInfo.path)) === false) {
                printLog(
                  "info",
                  `Downloading style file "${styleInfo.path}" - From "${item.style}"...`
                );

                await downloadStyleFile(
                  item.style,
                  styleInfo.path,
                  5,
                  300000 // 5 mins
                );
              }
            } else {
              if (item.cache !== undefined) {
                styleInfo.path = `${process.env.DATA_DIR}/caches/styles/${item.style}/style.json`;

                const cacheSource = seed.styles?.[item.style];

                if (cacheSource === undefined) {
                  throw new Error(`Cache style "${item.style}" is invalid`);
                }

                if (item.cache.forward === true) {
                  styleInfo.sourceURL = cacheSource.url;
                  styleInfo.storeCache = item.cache.store;
                }
              } else {
                styleInfo.path = `${process.env.DATA_DIR}/styles/${item.style}`;
              }
            }

            try {
              /* Read style.json file */
              styleJSON = await getStyle(styleInfo.path, true);

              /* Validate style */
              await validateStyle(styleJSON);

              /* Store style info */
              styleInfo.name = styleJSON.name || "Unknown";
              styleInfo.zoom = styleJSON.zoom || 0;
              styleInfo.center = styleJSON.center || [0, 0, 0];

              /* Mark to serve rendered */
              isCanServeRendered = true;
            } catch (error) {
              if (
                item.cache !== undefined &&
                error.message === "Style does not exist"
              ) {
                styleInfo.name =
                  seed.styles[item.style].metadata.name || "Unknown";
                styleInfo.zoom = seed.styles[item.style].metadata.zoom || 0;
                styleInfo.center = seed.styles[item.style].metadata.center || [
                  0, 0, 0,
                ];

                /* Mark to serve rendered */
                isCanServeRendered = false;
              } else {
                throw error;
              }
            }

            /* Add to repo */
            repos[id] = styleInfo;
          } catch (error) {
            printLog(
              "error",
              `Failed to load style "${id}": ${error}. Skipping...`
            );
          }

          /* Serve rendered */
          if (
            config.enableBackendRender === true &&
            item.rendered !== undefined &&
            isCanServeRendered === true
          ) {
            try {
              /* Rendered info */
              const rendered = {
                tileJSON: createRenderedMetadata({
                  name: styleInfo.name,
                  description: styleInfo.name,
                }),
                styleJSON: {},
                compressionLevel: item.rendered.compressionLevel || 6,
              };

              /* Fix center */
              if (styleJSON.center?.length >= 2 && styleJSON.zoom) {
                rendered.tileJSON.center = [
                  styleJSON.center[0],
                  styleJSON.center[1],
                  Math.floor(styleJSON.zoom),
                ];
              }

              /* Fix sources */
              await Promise.all(
                Object.keys(styleJSON.sources).map(async (id) => {
                  const source = styleJSON.sources[id];

                  if (source.tiles !== undefined) {
                    const tiles = new Set(
                      source.tiles.map((tile) => {
                        if (isLocalTileURL(tile) === true) {
                          const sourceID = tile.split("/")[2];
                          const sourceData = config.datas[sourceID];

                          tile = `${sourceData.sourceType}://${sourceID}/{z}/{x}/{y}.${sourceData.tileJSON.format}`;
                        }

                        return tile;
                      })
                    );

                    source.tiles = Array.from(tiles);
                  }

                  if (source.urls !== undefined) {
                    const otherUrls = [];

                    source.urls.forEach((url) => {
                      if (isLocalTileURL(url) === true) {
                        const sourceID = url.split("/")[2];
                        const sourceData = config.datas[sourceID];

                        const tile = `${sourceData.sourceType}://${sourceID}/{z}/{x}/{y}.${sourceData.tileJSON.format}`;

                        if (source.tiles !== undefined) {
                          if (source.tiles.includes(tile) === false) {
                            source.tiles.push(tile);
                          }
                        } else {
                          source.tiles = [tile];
                        }
                      } else {
                        if (otherUrls.includes(url) === false) {
                          otherUrls.push(url);
                        }
                      }
                    });

                    if (otherUrls.length === 0) {
                      delete source.urls;
                    } else {
                      source.urls = otherUrls;
                    }
                  }

                  if (source.url !== undefined) {
                    if (isLocalTileURL(source.url) === true) {
                      const sourceID = source.url.split("/")[2];
                      const sourceData = config.datas[sourceID];

                      const tile = `${sourceData.sourceType}://${sourceID}/{z}/{x}/{y}.${sourceData.tileJSON.format}`;

                      if (source.tiles !== undefined) {
                        if (source.tiles.includes(tile) === false) {
                          source.tiles.push(tile);
                        }
                      } else {
                        source.tiles = [tile];
                      }

                      delete source.url;
                    }
                  }

                  if (
                    source.url === undefined &&
                    source.urls === undefined &&
                    source.tiles !== undefined
                  ) {
                    if (source.tiles.length === 1) {
                      if (isLocalTileURL(source.tiles[0]) === true) {
                        const sourceID = source.tiles[0].split("/")[2];
                        const sourceData = config.datas[sourceID];

                        styleJSON.sources[id] = {
                          ...sourceData.tileJSON,
                          ...source,
                          tiles: [source.tiles[0]],
                        };
                      }
                    }
                  }

                  // Add atribution
                  if (
                    source.attribution &&
                    rendered.tileJSON.attribution.includes(
                      source.attribution
                    ) === false
                  ) {
                    rendered.tileJSON.attribution += ` | ${source.attribution}`;
                  }
                })
              );

              /* Add styleJSON */
              rendered.styleJSON = styleJSON;

              /* Add to repo */
              repos[id].rendered = rendered;
            } catch (error) {
              printLog(
                "error",
                `Failed to load rendered "${id}": ${error}. Skipping...`
              );
            }
          }
        })
      );

      config.styles = repos;
    }
  },
};
