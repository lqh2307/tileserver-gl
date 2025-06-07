"use strict";

import { renderStyleJSONToImage, renderSVGToImage } from "./render_style.js";
import { StatusCodes } from "http-status-codes";
import { printLog } from "./logger.js";
import { createReadStream } from "fs";
import { stat } from "fs/promises";
import { v4 } from "uuid";
import os from "os";
import {
  detectContentTypeFromFormat,
  getJSONSchema,
  validateJSON,
  gzipAsync,
} from "./utils.js";

/**
 * Render style JSON handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function renderStyleJSONHandler() {
  return async (req, res, next) => {
    try {
      /* Validate options */
      try {
        validateJSON(await getJSONSchema("render_stylejson"), req.body);
      } catch (error) {
        throw new SyntaxError(error);
      }

      const tmpFolder = v4();
      const id = req.body.id ? req.body.id : tmpFolder;
      const fileName = `${id}.${req.body.format}`;
      const dirPath = `${process.env.DATA_DIR}/exports/style_renders/${req.body.format}s/${tmpFolder}`;
      const filePath = `${dirPath}/${fileName}`;

      /* Render style */
      const metadata = await renderStyleJSONToImage(
        req.body.styleJSON,
        req.body.bbox,
        req.body.zoom,
        req.body.format,
        id,
        dirPath,
        req.body.maxRendererPoolSize,
        req.body.concurrency || os.cpus().length,
        req.body.storeTransparent ?? true,
        req.body.tileScale || 1,
        req.body.tileSize || 256,
        req.body.frame,
        req.body.grid,
        req.body.overlays
      );

      const stats = await stat(filePath);

      res.set({
        "content-length": stats.size,
        "content-disposition": `attachment; filename="${fileName}"`,
        "content-type": detectContentTypeFromFormat(req.body.format),
        "content-metadata": JSON.stringify(metadata),
      });

      const readStream = createReadStream(filePath);

      readStream.pipe(res);

      readStream.on("error", (error) => {
        throw error;
      });
    } catch (error) {
      printLog("error", `Failed to render style JSON: ${error}`);

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
 * Render SVG handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function renderSVGHandler() {
  return async (req, res, next) => {
    try {
      /* Validate options */
      try {
        validateJSON(await getJSONSchema("render_svg"), req.body);
      } catch (error) {
        throw new SyntaxError(error);
      }

      const result = await renderSVGToImage(
        req.body.format,
        req.body.overlays,
        req.body.concurrency || os.cpus().length
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
      printLog("error", `Failed to render SVG: ${error}`);

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

export const serve_render = {
  /**
   * Register render handlers
   * @param {Express} app Express object
   * @returns {void}
   */
  init: (app) => {
    /**
     * @swagger
     * tags:
     *   - name: Render
     *     description: Render related endpoints
     * /renders/stylejson:
     *   post:
     *     tags:
     *       - Render
     *     summary: Render style JSON
     *     requestBody:
     *       required: true
     *       content:
     *         application/json:
     *             schema:
     *               type: object
     *               example: {}
     *       description: Render style JSON options
     *     responses:
     *       201:
     *         description: Style JSON rendered
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
    app.post("/renders/stylejson", renderStyleJSONHandler());

    /**
     * @swagger
     * tags:
     *   - name: Render
     *     description: Render related endpoints
     * /renders/svg:
     *   post:
     *     tags:
     *       - Render
     *     summary: Render SVG
     *     parameters:
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
     *       description: Render SVG options
     *     responses:
     *       201:
     *         description: SVG rendered
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
    app.post("/renders/svg", renderSVGHandler());
  },
};
