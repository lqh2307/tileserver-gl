"use strict";

import { StatusCodes } from "http-status-codes";
import {
  updateConfigFile,
  readConfigFile,
  validateConfig,
  config,
} from "../configs/index.js";
import {
  compileHandleBarsTemplate,
  getXYZFromLonLatZ,
  getRequestHost,
  getVersion,
  printLog,
} from "../utils/index.js";

/**
 * Serve front page handler
 * @returns {(req: Request, res: Response, next: NextFunction) => Promise<any>}
 */
function serveFrontPageHandler() {
  return async (req, res) => {
    try {
      if (!config.isStarted) {
        return res.status(StatusCodes.SERVICE_UNAVAILABLE).send("Starting...");
      }

      const styles = {};
      const geojsons = {};
      const geojsonGroups = {};
      const datas = {};
      const sprites = {};
      const fonts = {};

      const requestHost = getRequestHost(req);

      await Promise.all([
        ...Object.keys(config.styles).map(async (id) => {
          const style = config.styles[id];

          if (style.tileJSON) {
            const { name, center } = style.tileJSON;

            const [x, y, z] = getXYZFromLonLatZ(
              center[0],
              center[1],
              center[2]
            );

            styles[id] = {
              name: name,
              viewer_hash: `#${center[2]}/${center[1]}/${center[0]}`,
              thumbnail: `${requestHost}/styles/${id}/${z}/${x}/${y}.png`,
              cache: style.storeCache,
              cancel_render: style.export,
            };
          } else {
            const { name, zoom, center } = style;

            styles[id] = {
              name: name,
              viewer_hash: `#${zoom}/${center[1]}/${center[0]}`,
              cache: style.storeCache,
            };
          }
        }),
        ...Object.keys(config.geojsons).map(async (id) => {
          geojsonGroups[id] = true;

          Object.keys(config.geojsons[id]).map(async (layer) => {
            const geojson = config.geojsons[id][layer];

            geojsons[`${id}/${layer}`] = {
              group: id,
              layer: layer,
              cache: geojson.storeCache,
            };
          });
        }),
        ...Object.keys(config.datas).map(async (id) => {
          const data = config.datas[id];
          const { name, center, format } = data.tileJSON;

          if (format !== "pbf") {
            const [x, y, z] = getXYZFromLonLatZ(
              center[0],
              center[1],
              center[2]
            );

            datas[id] = {
              name: name,
              format: format,
              viewer_hash: `#${center[2]}/${center[1]}/${center[0]}`,
              thumbnail: `${requestHost}/datas/${id}/${z}/${x}/${y}.${format}`,
              source_type: data.sourceType,
              cache: data.storeCache,
              cancel_export: data.export,
            };
          } else {
            datas[id] = {
              name: name,
              format: format,
              viewer_hash: `#${center[2]}/${center[1]}/${center[0]}`,
              source_type: data.sourceType,
              cache: data.storeCache,
              cancel_export: data.export,
            };
          }
        }),
        ...Object.keys(config.sprites).map(async (id) => {
          const sprite = config.sprites[id];

          sprites[id] = {
            cache: sprite.storeCache,
          };
        }),
        ...Object.keys(config.fonts).map(async (id) => {
          const font = config.fonts[id];

          fonts[id] = {
            cache: font.storeCache,
          };
        }),
      ]);

      const compiled = await compileHandleBarsTemplate("index", {
        styles: styles,
        geojsons: geojsons,
        geojson_groups: geojsonGroups,
        datas: datas,
        fonts: fonts,
        sprites: sprites,
        style_count: Object.keys(styles).length,
        geojson_count: Object.keys(geojsons).length,
        geojson_group_count: Object.keys(geojsonGroups).length,
        data_count: Object.keys(datas).length,
        font_count: Object.keys(fonts).length,
        sprite_count: Object.keys(sprites).length,
        base_url: requestHost,
      });

      return res.status(StatusCodes.OK).send(compiled);
    } catch (error) {
      printLog("error", `Failed to serve front page: ${error}`);

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

/**
 * Get config content handler
 * @returns {(req: Request, res: Response, next: NextFunction) => Promise<any>}
 */
function serveConfigHandler() {
  return async (req, res) => {
    try {
      res.header("content-type", "application/json");

      return res
        .status(StatusCodes.OK)
        .send(readConfigFile(req.query.type, false));
    } catch (error) {
      printLog("error", `Failed to get config: ${error}`);

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

/**
 * Update config content handler
 * @returns {(req: Request, res: Response, next: NextFunction) => Promise<any>}
 */
function serveConfigUpdateHandler() {
  return async (req, res) => {
    try {
      await validateConfig(req.query.type, req.body);
    } catch (error) {
      return res
        .status(StatusCodes.BAD_REQUEST)
        .send(`Invalid ${req.query.type}: ${error}`);
    }

    try {
      const config = readConfigFile(req.query.type, true);

      if (req.query.type === "seed") {
        if (!req.body.styles) {
          printLog("info", "No styles to update in seed. Skipping...");
        } else {
          const ids = Object.keys(req.body.styles);

          printLog("info", `Updating ${ids.length} styles in seed...`);

          ids.map((id) => {
            config.styles[id] = req.body.styles[id];
          });
        }

        if (!req.body.geojsons) {
          printLog("info", "No GeoJSONs to update in seed. Skipping...");
        } else {
          const ids = Object.keys(req.body.geojsons);

          printLog("info", `Updating ${ids.length} GeoJSONs in seed...`);

          ids.map((id) => {
            config.geojsons[id] = req.body.geojsons[id];
          });
        }

        if (!req.body.datas) {
          printLog("info", "No datas to update in seed. Skipping...");
        } else {
          const ids = Object.keys(req.body.datas);

          printLog("info", `Updating ${ids.length} datas in seed...`);

          ids.map((id) => {
            config.datas[id] = req.body.datas[id];
          });
        }

        if (!req.body.sprites) {
          printLog("info", "No sprites to update in seed. Skipping...");
        } else {
          const ids = Object.keys(req.body.sprites);

          printLog("info", `Updating ${ids.length} sprites in seed...`);

          ids.map((id) => {
            config.sprites[id] = req.body.sprites[id];
          });
        }

        if (!req.body.fonts) {
          printLog("info", "No fonts to update in seed. Skipping...");
        } else {
          const ids = Object.keys(req.body.fonts);

          printLog("info", `Updating ${ids.length} fonts in seed...`);

          ids.map((id) => {
            config.fonts[id] = req.body.fonts[id];
          });
        }

        await updateConfigFile("cleanup", config, 60000);
      } else if (req.query.type === "cleanup") {
        if (!req.body.styles) {
          printLog("info", "No styles to update in cleanup. Skipping...");
        } else {
          const ids = Object.keys(req.body.styles);

          printLog("info", `Updating ${ids.length} styles in cleanup...`);

          ids.map((id) => {
            config.styles[id] = req.body.styles[id];
          });
        }

        if (!req.body.geojsons) {
          printLog("info", "No GeoJSONs to update in cleanup. Skipping...");
        } else {
          const ids = Object.keys(req.body.geojsons);

          printLog("info", `Updating ${ids.length} GeoJSONs in cleanup...`);

          ids.map((id) => {
            config.geojsons[id] = req.body.geojsons[id];
          });
        }

        if (!req.body.datas) {
          printLog("info", "No datas to update in cleanup. Skipping...");
        } else {
          const ids = Object.keys(req.body.datas);

          printLog("info", `Updating ${ids.length} datas in cleanup...`);

          ids.map((id) => {
            config.datas[id] = req.body.datas[id];
          });
        }

        if (!req.body.sprites) {
          printLog("info", "No sprites to update in cleanup. Skipping...");
        } else {
          const ids = Object.keys(req.body.sprites);

          printLog("info", `Updating ${ids.length} sprites in cleanup...`);

          ids.map((id) => {
            config.sprites[id] = req.body.sprites[id];
          });
        }

        if (!req.body.fonts) {
          printLog("info", "No fonts to update in cleanup. Skipping...");
        } else {
          const ids = Object.keys(req.body.fonts);

          printLog("info", `Updating ${ids.length} fonts in cleanup...`);

          ids.map((id) => {
            config.fonts[id] = req.body.fonts[id];
          });
        }

        await updateConfigFile("cleanup", config, 60000);
      } else {
        if (!req.body.styles) {
          printLog("info", "No styles to update in config. Skipping...");
        } else {
          const ids = Object.keys(req.body.styles);

          printLog("info", `Updating ${ids.length} styles in config...`);

          ids.map((id) => {
            config.styles[id] = req.body.styles[id];
          });
        }

        if (!req.body.geojsons) {
          printLog(
            "info",
            "No GeoJSON groups to update in config. Skipping..."
          );
        } else {
          const ids = Object.keys(req.body.geojsons);

          printLog(
            "info",
            `Updating ${ids.length} GeoJSON groups in config...`
          );

          ids.map((id) => {
            config.geojsons[id] = req.body.geojsons[id];
          });
        }

        if (!req.body.datas) {
          printLog("info", "No datas to update in config. Skipping...");
        } else {
          const ids = Object.keys(req.body.datas);

          printLog("info", `Updating ${ids.length} datas in config...`);

          ids.map((id) => {
            config.datas[id] = req.body.datas[id];
          });
        }

        if (!req.body.sprites) {
          printLog("info", "No sprites to update in config. Skipping...");
        } else {
          const ids = Object.keys(req.body.sprites);

          printLog("info", `Updating ${ids.length} sprites in config...`);

          ids.map((id) => {
            config.sprites[id] = req.body.sprites[id];
          });
        }

        if (!req.body.fonts) {
          printLog("info", "No fonts to update in config. Skipping...");
        } else {
          const ids = Object.keys(req.body.fonts);

          printLog("info", `Updating ${ids.length} fonts in config...`);

          ids.map((id) => {
            config.fonts[id] = req.body.fonts[id];
          });
        }

        await updateConfigFile(config, 60000);
      }

      if (req.query.restart === "true") {
        setTimeout(
          () =>
            process.send({
              action: "restartServer",
            }),
          0
        );
      }

      return res.status(StatusCodes.OK).send("OK");
    } catch (error) {
      printLog("error", `Failed to update config: ${error}`);

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

/**
 * Delete config content handler
 * @returns {(req: Request, res: Response, next: NextFunction) => Promise<any>}
 */
function serveConfigDeleteHandler() {
  return async (req, res) => {
    try {
      await validateConfig(req.query.type, req.body);
    } catch (error) {
      return res
        .status(StatusCodes.BAD_REQUEST)
        .send(`Invalid ${req.query.type}: ${error}`);
    }

    try {
      const config = readConfigFile(req.query.type, true);

      if (req.query.type === "seed") {
        if (!req.body.styles) {
          printLog("info", "No styles to remove in seed. Skipping...");
        } else {
          printLog(
            "info",
            `Removing ${req.body.styles.length} styles in seed...`
          );

          req.body.styles.map((id) => {
            delete config.styles[id];
          });
        }

        if (!req.body.geojsons) {
          printLog("info", "No GeoJSONs to remove in seed. Skipping...");
        } else {
          printLog(
            "info",
            `Removing ${req.body.geojsons.length} GeoJSONs in seed...`
          );

          req.body.geojsons.map((id) => {
            delete config.geojsons[id];
          });
        }

        if (!req.body.datas) {
          printLog("info", "No datas to remove in seed. Skipping...");
        } else {
          printLog(
            "info",
            `Removing ${req.body.datas.length} datas in seed...`
          );

          req.body.datas.map((id) => {
            delete config.datas[id];
          });
        }

        if (!req.body.sprites) {
          printLog("info", "No sprites to remove in seed. Skipping...");
        } else {
          printLog(
            "info",
            `Removing ${req.body.sprites.length} sprites in seed...`
          );

          req.body.sprites.map((id) => {
            delete config.sprites[id];
          });
        }

        if (!req.body.fonts) {
          printLog("info", "No fonts to remove in seed. Skipping...");
        } else {
          printLog(
            "info",
            `Removing ${req.body.fonts.length} fonts in seed...`
          );

          req.body.fonts.map((id) => {
            delete config.fonts[id];
          });
        }

        await updateConfigFile("cleanup", config, 60000);
      } else if (req.query.type === "cleanup") {
        if (!req.body.styles) {
          printLog("info", "No styles to remove in cleanup. Skipping...");
        } else {
          printLog(
            "info",
            `Removing ${req.body.styles.length} styles in cleanup...`
          );

          req.body.styles.map((id) => {
            delete config.styles[id];
          });
        }

        if (!req.body.geojsons) {
          printLog("info", "No GeoJSONs to remove in cleanup. Skipping...");
        } else {
          printLog(
            "info",
            `Removing ${req.body.geojsons.length} GeoJSONs in cleanup...`
          );

          req.body.geojsons.map((id) => {
            delete config.geojsons[id];
          });
        }

        if (!req.body.datas) {
          printLog("info", "No datas to remove in cleanup. Skipping...");
        } else {
          printLog(
            "info",
            `Removing ${req.body.datas.length} datas in cleanup...`
          );

          req.body.datas.map((id) => {
            delete config.datas[id];
          });
        }

        if (!req.body.sprites) {
          printLog("info", "No sprites to remove in cleanup. Skipping...");
        } else {
          printLog(
            "info",
            `Removing ${req.body.sprites.length} sprites in cleanup...`
          );

          req.body.sprites.map((id) => {
            delete config.sprites[id];
          });
        }

        if (!req.body.fonts) {
          printLog("info", "No fonts to remove in cleanup. Skipping...");
        } else {
          printLog(
            "info",
            `Removing ${req.body.fonts.length} fonts in cleanup...`
          );

          req.body.fonts.map((id) => {
            delete config.fonts[id];
          });
        }

        await updateConfigFile("cleanup", config, 60000);
      } else {
        if (!req.body.styles) {
          printLog("info", "No styles to remove in cleanup. Skipping...");
        } else {
          printLog(
            "info",
            `Removing ${req.body.styles.length} styles in cleanup...`
          );

          req.body.styles.map((id) => {
            delete config.styles[id];
          });
        }

        if (!req.body.geojsons) {
          printLog("info", "No GeoJSONs to remove in cleanup. Skipping...");
        } else {
          printLog(
            "info",
            `Removing ${req.body.geojsons.length} GeoJSONs in config...`
          );

          req.body.geojsons.map((id) => {
            delete config.geojsons[id];
          });
        }

        if (!req.body.datas) {
          printLog("info", "No datas to remove in config. Skipping...");
        } else {
          printLog(
            "info",
            `Removing ${req.body.datas.length} datas in config...`
          );

          req.body.datas.map((id) => {
            delete config.datas[id];
          });
        }

        if (!req.body.sprites) {
          printLog("info", "No sprites to remove in config. Skipping...");
        } else {
          printLog(
            "info",
            `Removing ${req.body.sprites.length} sprites in config...`
          );

          req.body.sprites.map((id) => {
            delete config.sprites[id];
          });
        }

        if (!req.body.fonts) {
          printLog("info", "No fonts to remove in config. Skipping...");
        } else {
          printLog(
            "info",
            `Removing ${req.body.fonts.length} fonts in config...`
          );

          req.body.fonts.map((id) => {
            delete config.fonts[id];
          });
        }

        await updateConfigFile(config, 60000);
      }

      if (req.query.restart === "true") {
        setTimeout(
          () =>
            process.send({
              action: "restartServer",
            }),
          0
        );
      }

      return res.status(StatusCodes.OK).send("OK");
    } catch (error) {
      printLog("error", `Failed to delete config: ${error}`);

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

/**
 * Get version of server handler
 * @returns {(req: Request, res: Response, next: NextFunction) => Promise<any>}
 */
function serveVersionHandler() {
  return async (_, res) => {
    try {
      const version = await getVersion();

      return res.status(StatusCodes.OK).send(version);
    } catch (error) {
      printLog("error", `Failed to check version server: ${error}`);

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

/**
 * Get ready of server handler
 * @returns {(req: Request, res: Response, next: NextFunction) => Promise<any>}
 */
function serveReadyHandler() {
  return async (_, res) => {
    try {
      return res.status(StatusCodes.OK).send("OK");
    } catch (error) {
      printLog("error", `Failed to check ready server: ${error}`);

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

/**
 * Get health of server handler
 * @returns {(req: Request, res: Response, next: NextFunction) => Promise<any>}
 */
function serveHealthHandler() {
  return async (_, res) => {
    try {
      if (!config.isStarted) {
        return res.status(StatusCodes.SERVICE_UNAVAILABLE).send("Starting...");
      }

      return res.status(StatusCodes.OK).send("OK");
    } catch (error) {
      printLog("error", `Failed to check health server: ${error}`);

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

/**
 * Restart/kill server handler
 * @returns {(req: Request, res: Response, next: NextFunction) => Promise<any>}
 */
function serveRestartKillHandler() {
  return async (req, res) => {
    try {
      if (req.query.type === "kill") {
        setTimeout(
          () =>
            process.send({
              action: "killServer",
            }),
          0
        );
      } else {
        setTimeout(
          () =>
            process.send({
              action: "restartServer",
            }),
          0
        );
      }

      return res.status(StatusCodes.OK).send("OK");
    } catch (error) {
      printLog("error", `Failed to restart/kill server: ${error}`);

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

export const serve_common = {
  /**
   * Register common handlers
   * @param {Express} app Express object
   * @returns {void}
   */
  init: (app) => {
    /**
     * @swagger
     * tags:
     *   - name: Common
     *     description: Common related endpoints
     * /health:
     *   get:
     *     tags:
     *       - Common
     *     summary: Check health of the server
     *     responses:
     *       200:
     *         description: Server is healthy
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
    app.get("/health", serveHealthHandler());

    /**
     * @swagger
     * tags:
     *   - name: Common
     *     description: Common related endpoints
     * /ready:
     *   get:
     *     tags:
     *       - Common
     *     summary: Check ready of the server
     *     responses:
     *       200:
     *         description: Server is ready
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
    app.get("/ready", serveReadyHandler());

    /**
     * @swagger
     * tags:
     *   - name: Common
     *     description: Common related endpoints
     * /version:
     *   get:
     *     tags:
     *       - Common
     *     summary: Check version of the server
     *     responses:
     *       200:
     *         description: Version of server
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
    app.get("/version", serveVersionHandler());

    /**
     * @swagger
     * tags:
     *   - name: Common
     *     description: Common related endpoints
     * /config:
     *   get:
     *     tags:
     *       - Common
     *     summary: Get config
     *     parameters:
     *       - in: query
     *         name: type
     *         schema:
     *           type: string
     *           enum: [config, seed, cleanup]
     *           example: config
     *         required: false
     *         description: Config type
     *     responses:
     *       200:
     *         description: Config
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
     *   put:
     *     tags:
     *       - Common
     *     summary: Update config
     *     parameters:
     *       - in: query
     *         name: type
     *         schema:
     *           type: string
     *           enum: [config, seed, cleanup]
     *           example: config
     *         required: false
     *         description: Config type
     *       - in: query
     *         name: restart
     *         schema:
     *           type: boolean
     *         required: false
     *         description: Restart server after change
     *     requestBody:
     *       required: true
     *       content:
     *         application/json:
     *             schema:
     *               type: object
     *               example: {}
     *       description: Update config object
     *     responses:
     *       200:
     *         description: Config is updated
     *         content:
     *           application/json:
     *             schema:
     *               type: object
     *       400:
     *         description: Bad request
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
     *   delete:
     *     tags:
     *       - Common
     *     summary: Update config
     *     parameters:
     *       - in: query
     *         name: type
     *         schema:
     *           type: string
     *           enum: [config, seed, cleanup]
     *           example: config
     *         required: false
     *         description: Config type
     *       - in: query
     *         name: restart
     *         schema:
     *           type: boolean
     *         required: false
     *         description: Restart server after change
     *     requestBody:
     *       required: true
     *       content:
     *         application/json:
     *             schema:
     *               type: object
     *               example: {}
     *       description: Delete config object
     *     responses:
     *       200:
     *         description: Config is updated
     *         content:
     *           application/json:
     *             schema:
     *               type: object
     *       400:
     *         description: Bad request
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
    app.get("/config", serveConfigHandler());
    app.put("/config", serveConfigUpdateHandler());
    app.delete("/config", serveConfigDeleteHandler());

    /**
     * @swagger
     * tags:
     *   - name: Common
     *     description: Common related endpoints
     * /restart:
     *   get:
     *     tags:
     *       - Common
     *     summary: Restart/kill the server
     *     parameters:
     *       - in: query
     *         name: type
     *         schema:
     *           type: string
     *           enum: [restart, kill]
     *           example: restart
     *         required: false
     *         description: Action type
     *     responses:
     *       200:
     *         description: Server will restart/kill
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
    app.get("/restart", serveRestartKillHandler());

    /* Serve front page */
    if (process.env.SERVE_FRONT_PAGE !== "false") {
      /**
       * @swagger
       * tags:
       *   - name: Common
       *     description: Common related endpoints
       * /:
       *   get:
       *     tags:
       *       - Common
       *     summary: Serve front page
       *     responses:
       *       200:
       *         description: Front page
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
      app.get("/", serveFrontPageHandler());
    }
  },
};
