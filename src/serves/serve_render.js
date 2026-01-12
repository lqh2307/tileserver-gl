"use strict";

import { getRenderedStyleJSON } from "../resources/style.js";
import { renderStyleJSON } from "../render_style.js";
import { StatusCodes } from "http-status-codes";
import { config } from "../configs/index.js";
import { readFile, rm } from "fs/promises";
import { createReadStream } from "fs";
import { Readable } from "stream";
import { nanoid } from "nanoid";
import path from "path";
import {
  detectContentTypeFromFormat,
  renderImageToHighQualityPDF,
  createImageOutput,
  renderImageToPDF,
  addFrameToImage,
  base64ToBuffer,
  bufferToBase64,
  getJSONSchema,
  validateJSON,
  getFileSize,
  printLog,
} from "../utils/index.js";

/**
 * Render styleJSON handler
 * @returns {(req: Request, res: Response, next: NextFunction) => Promise<any>}
 */
function renderStyleJSONHandler() {
  return async (req, res) => {
    try {
      /* Validate options */
      try {
        validateJSON(await getJSONSchema("render_stylejson"), req.body);
      } catch (error) {
        throw new SyntaxError(error);
      }

      if (req.body.styleId) {
        const item = config.styles[req.body.styleId];

        if (!item) {
          return res.status(StatusCodes.NOT_FOUND).send("Style does not exist");
        }

        req.body.styleJSON = await getRenderedStyleJSON(item.path);
      }

      /* Render style JSON */
      const format = req.body.format ?? "png";

      const filePath = await renderStyleJSON({
        ...req.body,
        format: format,
      });

      let readStream;

      if (req.body.base64) {
        const image = bufferToBase64(await readFile(filePath), format);

        res.set({
          "content-length": image.length,
          "content-type": "text/plain",
        });

        readStream = Readable.from(image);
      } else {
        res.set({
          "content-length": await getFileSize(filePath),
          "content-disposition": `attachment; filename="${path.basename(filePath)}"`,
          "content-type": detectContentTypeFromFormat(format),
        });

        readStream = createReadStream(filePath);
      }

      readStream.pipe(res);

      readStream
        .on("error", (error) => {
          throw error;
        })
        .on("close", () => {
          rm(path.dirname(filePath), {
            force: true,
            recursive: true,
          });
        });
    } catch (error) {
      printLog("error", `Failed to render styleJSON: ${error}`);

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
 * Add frame handler
 * @returns {(req: Request, res: Response, next: NextFunction) => Promise<any>}
 */
function addFrameHandler() {
  return async (req, res) => {
    try {
      /* Validate options */
      try {
        validateJSON(await getJSONSchema("add_frame"), req.body);
      } catch (error) {
        throw new SyntaxError(error);
      }

      /* Add frame */
      const format = req.body.output.format ?? "png";

      let image = await addFrameToImage(
        {
          ...req.body.input,
          image: base64ToBuffer(req.body.input.image),
        },
        req.body.overlays,
        req.body.frame,
        req.body.grid,
        {
          ...req.body.output,
          format: format,
        },
      );

      if (req.body.output.base64) {
        image = bufferToBase64(image, format);

        res.set({
          "content-length": image.length,
          "content-type": "text/plain",
        });
      } else {
        res.set({
          "content-length": image.length,
          "content-disposition": `attachment; filename="${`${nanoid()}.${format}`}"`,
          "content-type": detectContentTypeFromFormat(format),
        });
      }

      const readStream = Readable.from(image);

      readStream.pipe(res);

      readStream.on("error", (error) => {
        throw error;
      });
    } catch (error) {
      printLog("error", `Failed to add frame: ${error}`);

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
 * @returns {(req: Request, res: Response, next: NextFunction) => Promise<any>}
 */
function renderSVGHandler() {
  return async (req, res) => {
    try {
      /* Validate options */
      try {
        validateJSON(await getJSONSchema("render_svg"), req.body);
      } catch (error) {
        throw new SyntaxError(error);
      }

      /* Render SVG */
      const format = req.body.format ?? "png";

      let image = await createImageOutput({
        ...req.body,
        format: format,
        data: base64ToBuffer(req.body.image),
      });

      if (req.body.base64) {
        image = bufferToBase64(image, format);

        res.set({
          "content-length": image.length,
          "content-type": "text/plain",
        });
      } else {
        res.set({
          "content-length": image.length,
          "content-disposition": `attachment; filename="${`${nanoid()}.${format}`}"`,
          "content-type": detectContentTypeFromFormat(format),
        });
      }

      const readStream = Readable.from(image);

      readStream.pipe(res);

      readStream.on("error", (error) => {
        throw error;
      });
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

/**
 * Render PDF handler
 * @returns {(req: Request, res: Response, next: NextFunction) => Promise<any>}
 */
function renderPDFHandler() {
  return async (req, res) => {
    try {
      /* Validate options */
      try {
        validateJSON(await getJSONSchema("render_pdf"), req.body);
      } catch (error) {
        throw new SyntaxError(error);
      }

      /* Render PDF */
      const format = req.body.output.format ?? "png";

      let image = await renderImageToPDF(
        {
          ...req.body.input,
          images: req.body.input.images.map(base64ToBuffer),
        },
        req.body.preview,
        {
          ...req.body.output,
          format: format,
        },
      );

      if (req.body.output.base64) {
        image = bufferToBase64(image, format);

        res.set({
          "content-length": image.length,
          "content-type": "text/plain",
        });
      } else {
        res.set({
          "content-length": image.length,
          "content-disposition": `attachment; filename="${`${nanoid()}.${format}`}"`,
          "content-type": detectContentTypeFromFormat(format),
        });
      }

      const readStream = Readable.from(image);

      readStream.pipe(res);

      readStream.on("error", (error) => {
        throw error;
      });
    } catch (error) {
      printLog("error", `Failed to render PDF: ${error}`);

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
 * Render PDF high quality handler
 * @returns {(req: Request, res: Response, next: NextFunction) => Promise<any>}
 */
function renderHighQualityPDFHandler() {
  return async (req, res) => {
    try {
      /* Validate options */
      try {
        validateJSON(await getJSONSchema("render_high_quality_pdf"), req.body);
      } catch (error) {
        throw new SyntaxError(error);
      }

      /* Render PDF */
      const format = req.body.output.format ?? "png";

      let image = await renderImageToHighQualityPDF(
        {
          ...req.body.input,
          images: req.body.input.images.map((item) => ({
            image: base64ToBuffer(item.image),
            res: item.resolution,
          })),
        },
        req.body.preview,
        {
          ...req.body.output,
          format: format,
        },
      );

      if (req.body.output.base64) {
        image = bufferToBase64(image, format);

        res.set({
          "content-length": image.length,
          "content-type": "text/plain",
        });
      } else {
        res.set({
          "content-length": image.length,
          "content-disposition": `attachment; filename="${`${nanoid()}.${format}`}"`,
          "content-type": detectContentTypeFromFormat(format),
        });
      }

      const readStream = Readable.from(image);

      readStream.pipe(res);

      readStream.on("error", (error) => {
        throw error;
      });
    } catch (error) {
      printLog("error", `Failed to render high quality PDF: ${error}`);

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
     * /renders/add-frame:
     *   post:
     *     tags:
     *       - Render
     *     summary: Add frame to image
     *     requestBody:
     *       required: true
     *       content:
     *         application/json:
     *             schema:
     *               type: object
     *               example: {}
     *       description: Add frame options
     *     responses:
     *       201:
     *         description: Frame added
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
    app.post("/renders/add-frame", addFrameHandler());

    /**
     * @swagger
     * tags:
     *   - name: Render
     *     description: Render related endpoints
     * /renders/stylejson:
     *   post:
     *     tags:
     *       - Render
     *     summary: Render styleJSON
     *     requestBody:
     *       required: true
     *       content:
     *         application/json:
     *             schema:
     *               type: object
     *               example: {}
     *       description: Render styleJSON options
     *     responses:
     *       201:
     *         description: StyleJSONs rendered
     *         content:
     *           text/plain:
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
     *     requestBody:
     *       required: true
     *       content:
     *         application/json:
     *             schema:
     *                 type: object
     *                 properties:
     *                   content:
     *                     type: string
     *                   width:
     *                     type: number
     *                   height:
     *                     type: number
     *                   format:
     *                    type: string
     *                    enum: [jpeg, jpg, png, webp, gif]
     *                   base64:
     *                    type: boolean
     *               example: {}
     *       description: Render SVG options
     *     responses:
     *       201:
     *         description: SVG rendered
     *         content:
     *           application/json:
     *             schema:
     *               type: array
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

    /**
     * @swagger
     * tags:
     *   - name: Render
     *     description: Render related endpoints
     * /renders/high-quality-pdf:
     *   post:
     *     tags:
     *       - Render
     *     summary: Render high quality PDF
     *     requestBody:
     *       required: true
     *       content:
     *         application/json:
     *             schema:
     *               type: object
     *               example: {}
     *       description: Render high quality PDF options
     *     responses:
     *       201:
     *         description: High quality PDF rendered
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
    app.post("/renders/high-quality-pdf", renderHighQualityPDFHandler());

    /**
     * @swagger
     * tags:
     *   - name: Render
     *     description: Render related endpoints
     * /renders/pdf:
     *   post:
     *     tags:
     *       - Render
     *     summary: Render PDF
     *     requestBody:
     *       required: true
     *       content:
     *         application/json:
     *             schema:
     *               type: object
     *               example: {}
     *       description: Render PDF options
     *     responses:
     *       201:
     *         description: PDF rendered
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
    app.post("/renders/pdf", renderPDFHandler());
  },
};
