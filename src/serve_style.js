"use strict";

import { getRenderedStyleJSON, validateStyle, getStyle } from "./style.js";
import { renderImageTileData } from "./render_style.js";
import { getAndCacheDataStyleJSON } from "./data.js";
import { StatusCodes } from "http-status-codes";
import { printLog } from "./logger.js";
import { config } from "./config.js";
import { seed } from "./seed.js";
import {
  detectContentTypeFromFormat,
  createTileMetadata,
  calculateMD5OfFile,
  compileTemplate,
  getRequestHost,
  isLocalURL,
  gzipAsync,
} from "./utils.js";

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
      const item = config.styles[id].tileJSON;

      if (item === undefined) {
        return res.status(StatusCodes.NOT_FOUND).send("WMTS does not exist");
      }

      const compiled = await compileTemplate("wmts", {
        id: id,
        name: item.name,
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
      /* Check style is used? */
      if (config.styles[id] === undefined) {
        return res.status(StatusCodes.NOT_FOUND).send("Style does not exist");
      }

      /* Get and cache StyleJSON */
      let styleJSON = await getAndCacheDataStyleJSON(id);

      if (req.query.raw !== "true") {
        styleJSON = JSON.parse(styleJSON);

        const requestHost = getRequestHost(req);

        /* Fix sprite url */
        if (styleJSON.sprite !== undefined) {
          if (styleJSON.sprite.startsWith("sprites://")) {
            styleJSON.sprite = styleJSON.sprite.replace(
              "sprites://",
              `${requestHost}/sprites/`
            );
          }
        }

        /* Fix font url */
        if (styleJSON.glyphs !== undefined) {
          if (styleJSON.glyphs.startsWith("fonts://")) {
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

            // Fix geoJSON URL
            if (source.data !== undefined) {
              if (isLocalURL(source.data)) {
                const parts = source.data.split("/");

                source.data = `${requestHost}/geojsons/${parts[2]}/${parts[3]}.geojson`;
              }
            }

            // Fix tileJSON URL
            if (source.url !== undefined) {
              if (isLocalURL(source.url)) {
                const sourceID = source.url.split("/")[2];

                source.url = `${requestHost}/datas/${sourceID}.json`;
              }
            }

            // Fix tileJSON URLs
            if (source.urls !== undefined) {
              const urls = new Set(
                source.urls.map((url) => {
                  if (isLocalURL(url)) {
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
                  if (isLocalURL(tile)) {
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

      const headers = {
        "content-type": "application/json",
      };

      if (req.query.compression === "true") {
        styleJSON = await gzipAsync(
          req.query.raw !== "true" ? JSON.stringify(styleJSON) : styleJSON
        );

        headers["content-encoding"] = "gzip";
      }

      res.set(headers);

      return res.status(StatusCodes.OK).send(styleJSON);
    } catch (error) {
      printLog("error", `Failed to get style "${id}": ${error}`);

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
    /* Check data tile format */
    if (
      ["jpeg", "jpg", "png", "webp", "gif"].includes(req.params.format) ===
      false
    ) {
      return res
        .status(StatusCodes.BAD_REQUEST)
        .send("Rendered tile format is not support");
    }

    if (
      req.query.tileSize !== undefined &&
      !["256", "512"].includes(req.query.tileSize)
    ) {
      return res
        .status(StatusCodes.BAD_REQUEST)
        .send("Tile size is not support");
    }

    const id = req.params.id;
    const item = config.styles[id];

    /* Check rendered is exist? */
    if (item === undefined || item.tileJSON === undefined) {
      return res.status(StatusCodes.NOT_FOUND).send("Rendered does not exist");
    }

    /* Get tile scale (Default: 1) */
    const tileScale = Number(req.query.tileScale) || 1;

    /* Get tile size (Default: 256) */
    const tileSize = Number(req.query.tileSize) || 256;

    const z = Number(req.params.z);
    const x = Number(req.params.x);
    const y = Number(req.params.y);

    /* Render tile */
    try {
      const renderedStyleJSON = await getRenderedStyleJSON(item.path);

      const image = await renderImageTileData(
        renderedStyleJSON,
        tileScale,
        tileSize,
        z,
        x,
        y,
        req.params.format
      );

      res.header(
        "content-type",
        detectContentTypeFromFormat(req.params.format)
      );

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
    const id = req.params.id;
    const queryStrings = [];

    if (req.query.tileSize !== undefined) {
      if (!["256", "512"].includes(req.query.tileSize)) {
        return res
          .status(StatusCodes.BAD_REQUEST)
          .send("Tile size is not support");
      } else {
        queryStrings.push(`tileSize=${req.query.tileSize}`);
      }
    }

    if (req.query.tileScale !== undefined) {
      queryStrings.push(`tileScale=${req.query.tileScale}`);
    }

    try {
      const item = config.styles[id];

      /* Check rendered is exist? */
      if (item === undefined || item.tileJSON === undefined) {
        return res
          .status(StatusCodes.NOT_FOUND)
          .send("Rendered does not exist");
      }

      const requestHost = getRequestHost(req);

      res.header("content-type", "application/json");

      /* Get render info */
      return res.status(StatusCodes.OK).send({
        ...item.tileJSON,
        tilejson: "2.2.0",
        scheme: "xyz",
        id: id,
        tiles: [
          `${requestHost}/styles/${id}/{z}/{x}/{y}.png${
            queryStrings.length ? `?${queryStrings.join("&")}` : ""
          }`,
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
 * Get Style MD5 handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function getStyleMD5Handler() {
  return async (req, res, next) => {
    const id = req.params.id;

    try {
      const item = config.styles[id];

      /* Check style is used? */
      if (item === undefined) {
        return res.status(StatusCodes.NOT_FOUND).send("Style does not exist");
      }

      /* Calculate MD5 and Add to header */
      res.set({
        etag: await calculateMD5OfFile(item.path),
      });

      return res.status(StatusCodes.OK).send();
    } catch (error) {
      printLog("error", `Failed to get md5 of style "${id}": ${error}`);

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
 * Get rendered list handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function getRenderedsListHandler() {
  return async (req, res, next) => {
    try {
      const requestHost = getRequestHost(req);

      const result = [];

      Object.keys(config.styles).map((id) => {
        const item = config.styles[id].tileJSON;

        if (item !== undefined) {
          result.push({
            id: id,
            name: item.name,
            url: [
              `${requestHost}/styles/256/${id}.json`,
              `${requestHost}/styles/512/${id}.json`,
            ],
          });
        }
      });

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
      printLog("error", `Failed to get rendereds": ${error}`);

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
     *     parameters:
     *       - in: query
     *         name: compression
     *         schema:
     *           type: boolean
     *         required: false
     *         description: Compressed response
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

    /* Serve backend render */
    if (process.env.BACKEND_RENDER === "true") {
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
       *       - in: query
       *         name: compression
       *         schema:
       *           type: boolean
       *         required: false
       *         description: Compressed response
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
       *       - in: query
       *         name: compression
       *         schema:
       *           type: boolean
       *         required: false
       *         description: Compressed response
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
       *                   name:
       *                     type: string
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
       * /styles/{id}.json:
       *   get:
       *     tags:
       *       - Rendered
       *     summary: Get style rendered
       *     parameters:
       *       - in: path
       *         name: id
       *         schema:
       *           type: string
       *           example: id
       *         required: true
       *         description: ID of the style rendered
       *       - in: query
       *         name: tileSize
       *         schema:
       *           type: integer
       *           enum: [256, 512]
       *           example: 256
       *         required: false
       *         description: Tile size
       *       - in: query
       *         name: tileScale
       *         schema:
       *           type: integer
       *           example: 2
       *         required: false
       *         description: Tile scale
       *     responses:
       *       200:
       *         description: Style rendered
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
      app.get("/styles/:id.json", getRenderedHandler());

      /**
       * @swagger
       * tags:
       *   - name: Style
       *     description: Style related endpoints
       * /styles/{id}/md5:
       *   get:
       *     tags:
       *       - Style
       *     summary: Get style md5
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
       *         description: Style md5
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
      app.get("/styles/:id/md5", getStyleMD5Handler());

      /**
       * @swagger
       * tags:
       *   - name: Rendered
       *     description: Rendered related endpoints
       * /styles/{id}/{z}/{x}/{y}.{format}:
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
       *         name: format
       *         required: true
       *         schema:
       *           type: string
       *           enum: [jpeg, jpg, png, webp, gif]
       *           example: png
       *         description: Tile format
       *       - in: query
       *         name: tileSize
       *         schema:
       *           type: integer
       *           enum: [256, 512]
       *           example: 256
       *         required: false
       *         description: Tile size
       *       - in: query
       *         name: tileScale
       *         schema:
       *           type: integer
       *           example: 2
       *         required: false
       *         description: Tile scale
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
      app.get("/styles/:id/:z/:x/:y.:format", getRenderedTileHandler());
    }

    if (process.env.SERVE_FRONT_PAGE !== "false") {
      /**
       * @swagger
       * tags:
       *   - name: Style
       *     description: Style related endpoints
       * /styles/{id}:
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

  /**
   * Add style
   * @returns {void}
   */
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
            if (item.cache !== undefined) {
              styleInfo.path = `${process.env.DATA_DIR}/caches/styles/${item.style}/style.json`;

              const cacheSource = seed.styles?.[item.style];

              if (cacheSource === undefined) {
                throw new Error(`Cache style "${item.style}" is invalid`);
              }

              if (item.cache.forward) {
                styleInfo.sourceURL = cacheSource.url;
                styleInfo.storeCache = item.cache.store;
              }
            } else {
              styleInfo.path = `${process.env.DATA_DIR}/styles/${item.style}`;
            }

            try {
              /* Read style.json file */
              styleJSON = JSON.parse(await getStyle(styleInfo.path));

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
                error.message === "JSON does not exist"
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
            process.env.BACKEND_RENDER === "true" &&
            isCanServeRendered
          ) {
            try {
              /* Rendered info */
              const tileJSON = createTileMetadata({
                name: styleInfo.name,
                description: styleInfo.name,
              });

              /* Fix center */
              if (
                styleJSON.center?.length >= 2 &&
                styleJSON.zoom !== undefined
              ) {
                tileJSON.center = [
                  styleJSON.center[0],
                  styleJSON.center[1],
                  Math.floor(styleJSON.zoom),
                ];
              }

              /* Add to repo */
              repos[id].tileJSON = tileJSON;
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
