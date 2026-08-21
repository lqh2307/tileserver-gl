"use strict";

import { readFile } from "node:fs/promises";
import handlebars from "handlebars";

function toSafeJavaScriptLiteral(value) {
  return JSON.stringify(value ?? "")
    .replace(/</g, "\\u003C")
    .replace(/>/g, "\\u003E")
    .replace(/&/g, "\\u0026")
    .replace(/\u2028/g, "\\u2028")
    .replace(/\u2029/g, "\\u2029");
}

handlebars.registerHelper("js", (value) => {
  return new handlebars.SafeString(toSafeJavaScriptLiteral(value));
});

handlebars.registerHelper("urlSegment", (value) => {
  return new handlebars.SafeString(encodeURIComponent(String(value ?? "")));
});

/**
 * Compile handlebars template
 * @param {"index"|"viewer"|"vector_data"|"raster_data"|"geojson_data"|"wmts"} template Template
 * @param {{ [key: string]: any }} data Data to fill to template
 * @returns {Promise<string>}
 */
export async function compileHandleBarsTemplate(template, data) {
  return handlebars.compile(
    await readFile(`public/templates/${template}.tmpl`, "utf8"),
  )(data);
}
