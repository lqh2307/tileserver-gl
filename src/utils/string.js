"use strict";

const ESCAPED_STRING_VALUES = {
  "&": "&amp;",
  '"': "&quot;",
  "'": "&apos;",
  "<": "&lt;",
  ">": "&gt;",
};

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

/** Escape XML special characters. */
export function escapeString(value) {
  return value.replace(/[&"'<>]/g, (character) => {
    return ESCAPED_STRING_VALUES[character];
  });
}
