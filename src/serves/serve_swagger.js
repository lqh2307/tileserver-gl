"use strict";

import { getRequestHost } from "../utils/index.js";
import { createRequire } from "node:module";
import swaggerUi from "swagger-ui-express";
import swaggerJsdoc from "swagger-jsdoc";

const require = createRequire(import.meta.url);
const { version } = require("../../package.json");

const tagDescriptions = {
  Common:
    "Health, readiness, version, configuration, restart, and front-page endpoints.",
  Data: "TileJSON metadata, tile binaries, checksums, and bounded extra-info queries for MBTiles, XYZ, PostgreSQL, and PMTiles sources.",
  Export:
    "Asynchronous export and style-render jobs. A 201 response means the job was accepted, not that it has finished.",
  Font: "Font catalog, glyph PBF ranges, checksums, and downloadable font files.",
  GeoJSON:
    "GeoJSON catalogs, metadata, source data, checksums, and browser viewers.",
  Prometheus: "Prometheus-compatible service metrics.",
  Render:
    "Image, SVG, StyleJSON, and PDF rendering operations. Backend rendering requires MapLibre GL Native.",
  Rendered:
    "Metadata and raster tiles generated from configured MapLibre styles.",
  Sprite: "Sprite catalogs, JSON/PNG sprite sheets, and checksums.",
  Style:
    "MapLibre StyleJSON documents, WMTS capabilities, checksums, and style viewers.",
  Summary:
    "Runtime summaries for configured services, seed jobs, and cleanup jobs.",
  Task: "Start or cancel background seed and cleanup tasks.",
};

const operationDescriptions = {
  "GET /health":
    "Liveness probe. Returns OK while the HTTP worker is running, including while resources are still loading.",
  "GET /ready":
    "Readiness probe. Returns OK only after configured styles, data, fonts, sprites, and GeoJSON resources have loaded.",
  "PUT /config":
    "Validates and merges the supplied config, seed, or cleanup object. Use the restart query parameter to control restart behavior after the update.",
  "DELETE /config":
    "Validates a delete selection and removes matching entries from config, seed, or cleanup configuration.",
  "GET /restart":
    "Requests a process restart or termination. This is an administrative operation and can interrupt active requests.",
  "GET /tasks/start":
    "Starts one background worker with the selected seed and cleanup groups. Only one task worker can run at a time.",
  "GET /tasks/cancel":
    "Terminates the active seed or cleanup worker. It does not delete data already written by completed work.",
  "POST /datas/{id}/extra-info":
    "Returns hash or creation-time information for exact XYZ tile ranges. A request is limited to 100000 tiles and can be gzip-compressed.",
  "GET /datas/{id}/extra-info":
    "Recalculates and persists extra information for cached tiles belonging to the selected data source.",
  "POST /exports":
    "Starts an asynchronous full export containing the selected configuration and resources.",
  "POST /exports/data/{id}":
    "Starts an asynchronous tile export into MBTiles, XYZ, or PostgreSQL storage.",
  "POST /exports/style-render/{id}":
    "Starts asynchronous rendering of every tile covered by the supplied style metadata.",
  "POST /renders/stylejson":
    "Renders a static image from an inline StyleJSON object or a configured style ID.",
};

const schemas = {
  AddFrame: require("../../public/schemas/add_frame.json"),
  Cleanup: require("../../public/schemas/cleanup.json"),
  Config: require("../../public/schemas/config.json"),
  DataExport: require("../../public/schemas/data_export.json"),
  DeleteConfig: require("../../public/schemas/delete.json"),
  ExportAll: require("../../public/schemas/export_all.json"),
  RenderHighQualityPDF: require("../../public/schemas/render_high_quality_pdf.json"),
  RenderPDF: require("../../public/schemas/render_pdf.json"),
  RenderStyleJSON: require("../../public/schemas/render_stylejson.json"),
  RenderSVG: require("../../public/schemas/render_svg.json"),
  Seed: require("../../public/schemas/seed.json"),
  StyleRender: require("../../public/schemas/style_render.json"),
  TileBounds: require("../../public/schemas/tile_bounds.json"),
};

const tags = [];
for (const [name, description] of Object.entries(tagDescriptions)) {
  const tag = {};
  tag.name = name;
  tag.description = description;

  tags.push(tag);
}

const swaggerDocument = swaggerJsdoc({
  definition: {
    openapi: "3.0.0",
    info: {
      title: "Tile Server API",
      version,
      description: `HTTP API for serving and caching map tiles, MapLibre styles,
GeoJSON, sprites, fonts, exports, rendering, and background seed/cleanup tasks.

Important behavior:

- Binary tile and render endpoints return the media type requested by the URL or body.
- A \`201\` export/render response acknowledges an asynchronous job; monitor logs or summaries for completion.
- A \`204\` tile response means no tile content is available.
- List and extra-info endpoints support gzip where the \`compression\` query parameter is documented.
- When deployed behind a reverse proxy, forward \`X-Forwarded-Proto\`, \`X-Forwarded-Host\`, and \`X-Forwarded-Prefix\` so Try it out uses the public URL. The \`proxy\` query parameter has highest priority.`,
    },
    tags,
    components: {
      schemas,
    },
  },
  apis: ["src/**/*.js"],
});

for (const [path, pathItem] of Object.entries(swaggerDocument.paths || {})) {
  for (const [method, operation] of Object.entries(pathItem)) {
    if (!["delete", "get", "patch", "post", "put"].includes(method)) {
      continue;
    }

    const operationKey = `${method.toUpperCase()} ${path}`;
    const operationWords = path
      .replace(/[{}]/g, "")
      .split(/[^a-zA-Z0-9]+/)
      .filter(Boolean);

    let operationID = method;
    for (const word of operationWords) {
      operationID += word[0].toUpperCase() + word.slice(1);
    }

    operation.operationId = operationWords.length
      ? operationID
      : `${method}Root`;
    operation.description =
      operationDescriptions[operationKey] ||
      `${operation.summary}. ${tagDescriptions[operation.tags?.[0]] || ""}`;
  }
}

/**
 * Serve Swagger UI with the public request URL
 * @param {Request} req Request object
 * @param {Response} res Response object
 * @param {NextFunction} next Next function
 * @returns {any} Swagger response
 */
function serveSwagger(req, res, next) {
  return swaggerUi.setup(
    {
      ...swaggerDocument,
      servers: [
        {
          url: getRequestHost(req),
          description: "Tile Server",
        },
      ],
    },
    {
      customSiteTitle: "Tile Server API",
      swaggerOptions: {
        displayRequestDuration: true,
        filter: true,
        persistAuthorization: true,
      },
    },
  )(req, res, next);
}

export const serve_swagger = {
  /**
   * Register swagger handlers
   * @param {Express} app Express object
   * @returns {void}
   */
  init: (app) => {
    /* Serve swagger */
    if (process.env.SERVE_SWAGGER !== "false") {
      app.use("/swagger", swaggerUi.serve, serveSwagger);
    }
  },
};
