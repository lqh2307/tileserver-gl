"use strict";

import { getJSONSchema, validateJSON } from "./utils.js";
import { StatusCodes } from "http-status-codes";
import { exportAll } from "./export_all.js";
import { printLog } from "./logger.js";
import { config } from "./config.js";
import os from "os";
import {
  exportPostgreSQLTiles,
  exportMBTilesTiles,
  exportXYZTiles,
} from "./export_data.js";
import {
  renderPostgreSQLTiles,
  renderMBTilesTiles,
  renderXYZTiles,
} from "./render_style.js";

/**
 * Export all handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function exportAllHandler() {
  return async (req, res, next) => {
    try {
      try {
        validateJSON(await getJSONSchema("export_all"), req.body);
      } catch (error) {
        throw new SyntaxError(error);
      }

      if (req.body.styles !== undefined) {
        for (const styleID of req.body.styles) {
          if (config.styles[styleID] === undefined) {
            return res
              .status(StatusCodes.NOT_FOUND)
              .send(`Style id "${styleID}" does not exist`);
          }
        }
      }

      if (req.body.datas !== undefined) {
        for (const dataID of req.body.datas) {
          if (config.datas[dataID] === undefined) {
            return res
              .status(StatusCodes.NOT_FOUND)
              .send(`Data id "${dataID}" does not exist`);
          }
        }
      }

      if (req.body.geojsons !== undefined) {
        for (const group of req.body.geojsons) {
          if (config.geojsons[group] === undefined) {
            return res
              .status(StatusCodes.NOT_FOUND)
              .send(`GeoJSON group id "${group}" does not exist`);
          }
        }
      }

      if (req.body.sprites !== undefined) {
        for (const spriteID of req.body.sprites) {
          if (config.sprites[spriteID] === undefined) {
            return res
              .status(StatusCodes.NOT_FOUND)
              .send(`Sprite id "${spriteID}" does not exist`);
          }
        }
      }

      exportAll(
        `${process.env.DATA_DIR}/exports/alls/${req.body.id}`,
        req.body,
        req.body.concurrency || os.cpus().length,
        req.body.storeTransparent ?? true,
        req.body.parentServerHost || "http://localhost:8080",
        req.body.exportData ?? true,
        req.body.refreshBefore?.time ||
          req.body.refreshBefore?.day ||
          req.body.refreshBefore?.md5
      );

      return res.status(StatusCodes.CREATED).send("OK");
    } catch (error) {
      printLog("error", `Failed to export all: ${error}`);

      if (error instanceof SyntaxError) {
        return res
          .status(StatusCodes.BAD_REQUEST)
          .send(`Options parameter is invalid: ${error}`);
      } else {
        return res
          .status(StatusCodes.INTERNAL_SERVER_ERROR)
          .send("Internal server error");
      }
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
            throw new SyntaxError(error);
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
          .send(`Options parameter is invalid: ${error}`);
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
      if (item === undefined || item.tileJSON === undefined) {
        return res
          .status(StatusCodes.NOT_FOUND)
          .send("Rendered does not exist");
      }

      if (req.query.cancel === "true") {
        /* Check export is not running? (export === true is not running) */
        if (item.export === true) {
          printLog(
            "warn",
            "No render is currently running. Skipping cancel render..."
          );

          return res.status(StatusCodes.NOT_FOUND).send("OK");
        } else {
          printLog("info", "Canceling render...");

          item.export = true;

          return res.status(StatusCodes.OK).send("OK");
        }
      } else {
        /* Check export is running? (export === false is not running) */
        if (item.export === false) {
          printLog("warn", "A render is already running. Skipping render...");

          return res.status(StatusCodes.CONFLICT).send("OK");
        } else {
          /* Render style */
          try {
            validateJSON(await getJSONSchema("style_render"), req.body);
          } catch (error) {
            throw new SyntaxError(error);
          }

          item.export = false;

          const refreshBefore =
            req.body.refreshBefore?.time ||
            req.body.refreshBefore?.day ||
            req.body.refreshBefore?.md5;

          switch (req.body.storeType) {
            case "xyz": {
              renderXYZTiles(
                id,
                `${process.env.DATA_DIR}/exports/style_renders/xyzs/${req.body.id}`,
                `${process.env.DATA_DIR}/exports/style_renders/xyzs/${req.body.id}/${req.body.id}.sqlite`,
                req.body.metadata,
                req.body.maxRendererPoolSize,
                req.body.concurrency || os.cpus().length,
                req.body.storeTransparent ?? true,
                req.body.createOverview ?? false,
                req.body.fastRender ?? false,
                refreshBefore
              )
                .catch((error) => {
                  printLog("error", `Failed to render style "${id}": ${error}`);
                })
                .finally(() => {
                  item.export = true;
                });

              break;
            }

            case "mbtiles": {
              renderMBTilesTiles(
                id,
                `${process.env.DATA_DIR}/exports/style_renders/mbtiles/${req.body.id}/${req.body.id}.mbtiles`,
                req.body.metadata,
                req.body.maxRendererPoolSize,
                req.body.concurrency || os.cpus().length,
                req.body.storeTransparent ?? true,
                req.body.createOverview ?? false,
                req.body.fastRender ?? false,
                refreshBefore
              )
                .catch((error) => {
                  printLog("error", `Failed to render style "${id}": ${error}`);
                })
                .finally(() => {
                  item.export = true;
                });

              break;
            }

            case "pg": {
              renderPostgreSQLTiles(
                id,
                `${process.env.POSTGRESQL_BASE_URI}/${req.body.id}`,
                req.body.metadata,
                req.body.maxRendererPoolSize,
                req.body.concurrency || os.cpus().length,
                req.body.storeTransparent ?? true,
                req.body.createOverview ?? false,
                req.body.fastRender ?? false,
                refreshBefore
              )
                .catch((error) => {
                  printLog("error", `Failed to render style "${id}": ${error}`);
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
      printLog("error", `Failed to render style "${id}": ${error}`);

      if (error instanceof SyntaxError) {
        return res
          .status(StatusCodes.BAD_REQUEST)
          .send(`Options parameter is invalid: ${error}`);
      } else {
        return res
          .status(StatusCodes.INTERNAL_SERVER_ERROR)
          .send("Internal server error");
      }
    }
  };
}

export const serve_export = {
  /**
   * Register export handlers
   * @param {Express} app Express object
   * @returns {void}
   */
  init: (app) => {
    /**
     * @swagger
     * tags:
     *   - name: Export
     *     description: Export related endpoints
     * /exports:
     *   post:
     *     tags:
     *       - Export
     *     summary: Export all
     *     requestBody:
     *       required: true
     *       content:
     *         application/json:
     *             schema:
     *               type: object
     *               example: {}
     *       description: Export all options
     *     responses:
     *       201:
     *         description: Export all is started
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
    app.post("/exports", exportAllHandler());

    /**
     * @swagger
     * tags:
     *   - name: Export
     *     description: Export related endpoints
     * /exports/data/{id}:
     *   get:
     *     tags:
     *       - Export
     *     summary: Cancel export data
     *     parameters:
     *       - in: path
     *         name: id
     *         required: true
     *         schema:
     *           type: string
     *           example: id
     *         description: Data ID
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
     *       - Export
     *     summary: Export data
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
     *       description: Export export options
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
    app.get("/exports/data/:id", exportDataHandler());
    app.post("/exports/data/:id", exportDataHandler());

    if (
      config.enableBackendRender === true &&
      process.env.ENABLE_EXPORT !== "false"
    ) {
      /**
       * @swagger
       * tags:
       *   - name: Export
       *     description: Export related endpoints
       * /exports/style-render/{id}:
       *   get:
       *     tags:
       *       - Export
       *     summary: Cancel render style
       *     parameters:
       *       - in: path
       *         name: id
       *         required: true
       *         schema:
       *           type: string
       *           example: id
       *         description: Style ID
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
       *       - Export
       *     summary: Render style
       *     parameters:
       *       - in: path
       *         name: id
       *         required: true
       *         schema:
       *           type: string
       *           example: id
       *         description: Style ID
       *     requestBody:
       *       required: true
       *       content:
       *         application/json:
       *             schema:
       *               type: object
       *               example: {}
       *       description: Export render options
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
      app.get("/exports/style-render/:id", renderStyleHandler());
      app.post("/exports/style-render/:id", renderStyleHandler());
    }
  },
};
