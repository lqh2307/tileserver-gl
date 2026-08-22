"use strict";

/** Split a comma-delimited parameter into trimmed non-empty values. */
export function splitParameter(value) {
  if (!value) {
    return [];
  }

  return String(value)
    .split(",")
    .map((item) => {
      return item.trim();
    })
    .filter(Boolean);
}

/** Normalize a string for case- and diacritic-insensitive matching. */
export function normalizeString(value, form) {
  return value
    ?.toLowerCase()
    .normalize(form)
    .replace(/[\u0300-\u036f]/g, "");
}

/** Capitalize the first character of each space-delimited word. */
export function capitalizeWords(value) {
  return value
    .split(" ")
    .map((word) => {
      return word.length > 0 ? word[0].toUpperCase() + word.slice(1) : "";
    })
    .join(" ");
}
