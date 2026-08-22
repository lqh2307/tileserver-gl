"use strict";

import { max, min } from "./number.js";

const COMMENT_END_REGEX = /-->/;
const CDATA_END_REGEX = /\]>/;
const OPEN_TAG_REGEX = /<\w/;
const CLOSE_TAG_REGEX = /<\//;
const SELF_CLOSING_TAG_REGEX = /\/>/;

/**
 * Format XML with a lightweight regex-based formatter.
 * @param {string} value XML string
 * @param {string} indent Indentation for each nesting level
 * @returns {string}
 */
export function formatXML(value, indent) {
  if (!value) {
    return "";
  }

  const processedXML = value.replace(/>\s+</g, "><");
  const indentation = indent ?? "\t";
  const shift = new Array(20);
  shift[0] = "\n";

  for (let index = 1; index < shift.length; index++) {
    shift[index] = shift[index - 1] + indentation;
  }

  const indentAt = (depth) => {
    return shift[min(depth, shift.length - 1)];
  };
  const getTagName = (tagString) => {
    const match = /^<(\/?[\w:\-.,]+)/.exec(tagString);
    return match ? match[1].replace("/", "") : "";
  };
  const parts = processedXML
    .replace(/</g, "~::~<")
    .replace(/\s*xmlns\:/g, "~::~xmlns:")
    .replace(/\s*xmlns\=/g, "~::~xmlns=")
    .split("~::~");

  let inComment = false;
  let depth = 0;
  const resultParts = [];

  for (let index = 0; index < parts.length; index++) {
    const part = parts[index];
    if (!part) {
      continue;
    }

    if (part.search(/<!/) > -1) {
      resultParts.push(indentAt(depth) + part);
      inComment = !(
        COMMENT_END_REGEX.test(part) ||
        CDATA_END_REGEX.test(part) ||
        part.search(/!DOCTYPE/) > -1
      );
    } else if (COMMENT_END_REGEX.test(part) || CDATA_END_REGEX.test(part)) {
      resultParts.push(part);
      inComment = false;
    } else if (
      /^<\w/.exec(parts[index - 1]) &&
      /^<\/\w/.exec(part) &&
      getTagName(parts[index - 1]) === getTagName(part)
    ) {
      resultParts.push(part);
      if (!inComment) {
        depth = max(depth - 1, 0);
      }
    } else if (
      OPEN_TAG_REGEX.test(part) &&
      !CLOSE_TAG_REGEX.test(part) &&
      !SELF_CLOSING_TAG_REGEX.test(part)
    ) {
      if (inComment) {
        resultParts.push(part);
      } else {
        resultParts.push(indentAt(depth) + part);
        depth += 1;
      }
    } else if (OPEN_TAG_REGEX.test(part) && CLOSE_TAG_REGEX.test(part)) {
      resultParts.push(inComment ? part : indentAt(depth) + part);
    } else if (CLOSE_TAG_REGEX.test(part)) {
      depth = max(depth - 1, 0);
      resultParts.push(inComment ? part : indentAt(depth) + part);
    } else if (SELF_CLOSING_TAG_REGEX.test(part)) {
      resultParts.push(inComment ? part : indentAt(depth) + part);
    } else if (part.search(/<\?/) > -1) {
      resultParts.push(indentAt(depth) + part);
    } else if (part.search(/xmlns\:/) > -1 || part.search(/xmlns\=/) > -1) {
      resultParts.push(indentAt(depth) + part);
    } else {
      resultParts.push(part);
    }
  }

  return resultParts.join("").trim();
}

/** Remove inter-tag whitespace and optionally XML comments. */
export function minifyXML(value, removeComments) {
  if (!value) {
    return "";
  }

  let result = value.trim();

  if (removeComments) {
    result = result.replace(/<!--[\s\S]*?-->/g, "");
  }

  return result.replace(/>\s*</g, "><");
}
