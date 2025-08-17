"use strict";

import { readFile } from "node:fs/promises";
import handlebars from "handlebars";

/**
 * Compile handlebars template
 * @param {"index"|"viewer"|"vector_data"|"raster_data"|"geojson_group"|"geojson"|"wmts"} template Template
 * @param {object} data Data to fill to template
 * @returns {Promise<string>}
 */
export async function compileHandleBarsTemplate(template, data) {
  return handlebars.compile(
    await readFile(`public/templates/${template}.tmpl`, "utf8")
  )(data);
}
