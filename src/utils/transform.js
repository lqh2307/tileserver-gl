"use strict";

/** Check if a transform has non-zero translation. */
export function isHasTransition(option) {
  return Boolean(option.x || option.y);
}

/** Reset translation to zero and optionally mutate the supplied object. */
export function createResetTransition(option) {
  const attributes = {
    x: 0,
    y: 0,
  };

  if (option) {
    Object.assign(option, attributes);
  }

  return attributes;
}

/** Check if a transform has non-default scale. */
export function isHasScale(option) {
  return (
    (option.scaleX !== undefined && option.scaleX !== 1) ||
    (option.scaleY !== undefined && option.scaleY !== 1)
  );
}

/** Reset scale to one and optionally mutate the supplied object. */
export function createResetScale(option) {
  const attributes = {
    scaleX: 1,
    scaleY: 1,
  };

  if (option) {
    Object.assign(option, attributes);
  }

  return attributes;
}

/** Check if a transform has non-zero skew. */
export function isHasSkew(option) {
  return Boolean(option.skewX || option.skewY);
}

/** Reset skew to zero and optionally mutate the supplied object. */
export function createResetSkew(option) {
  const attributes = {
    skewX: 0,
    skewY: 0,
  };

  if (option) {
    Object.assign(option, attributes);
  }

  return attributes;
}

/** Check if any transform field differs from its default. */
export function isHasTransform(option) {
  return Boolean(
    option.x ||
    option.y ||
    (option.scaleX !== undefined && option.scaleX !== 1) ||
    (option.scaleY !== undefined && option.scaleY !== 1) ||
    option.skewX ||
    option.skewY ||
    option.rotation,
  );
}

/** Reset all transform fields and optionally mutate the supplied object. */
export function createResetTransform(option) {
  const attributes = {
    rotation: 0,
    scaleX: 1,
    scaleY: 1,
    skewX: 0,
    skewY: 0,
    x: 0,
    y: 0,
  };

  if (option) {
    Object.assign(option, attributes);
  }

  return attributes;
}

/** Check if a transform has non-zero rotation. */
export function isHasRotation(option) {
  return Boolean(option.rotation);
}

/** Reset rotation to zero and optionally mutate the supplied object. */
export function createResetRotation(option) {
  const attributes = {
    rotation: 0,
  };

  if (option) {
    Object.assign(option, attributes);
  }

  return attributes;
}
