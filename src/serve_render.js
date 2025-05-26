"use strict";

import { renderStyleJSONToImage } from "./render_style.js";
import { StatusCodes } from "http-status-codes";
import { printLog } from "./logger.js";
import { createReadStream } from "fs";
import { stat } from "fs/promises";
import os from "os";
import {
  detectContentTypeFromFormat,
  getJSONSchema,
  validateJSON,
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
        validateJSON(await getJSONSchema("render"), req.body);
      } catch (error) {
        throw new SyntaxError(error);
      }

      const fileName =
        req.body.frame !== undefined
          ? `${req.body.id}.${req.body.format}`
          : `${req.body.id}.zip`;
      const dirPath = `${process.env.DATA_DIR}/exports/style_renders/${req.body.format}s/${req.body.id}`;
      const filePath = `${dirPath}/${fileName}`;

      /* Render style */
      await renderStyleJSONToImage(
        req.body.styleJSON,
        req.body.bbox,
        req.body.zoom,
        req.body.format,
        dirPath,
        req.body.maxRendererPoolSize,
        req.body.concurrency || os.cpus().length,
        req.body.storeTransparent ?? true,
        req.body.tileScale || 1,
        req.body.tileSize || 256,
        req.body.frame,
        req.body.overlays
      );

      const stats = await stat(filePath);

      res.set({
        "content-length": stats.size,
        "content-disposition": `attachment; filename="${fileName}"`,
        "content-type":
          req.body.frame !== undefined
            ? detectContentTypeFromFormat(req.body.format)
            : "application/zip",
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
     * /renders:
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
     *       description: Render options
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
    app.post("/renders", renderStyleJSONHandler());
  },
};
