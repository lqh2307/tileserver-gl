"use strict";

/**
 * Deep clone a JSON-serializable value via JSON stringify/parse.
 * Note: drops functions, `undefined`, `Date`, `Map`, `Set`, `BigInt`, and
 * loses prototypes. Only use for plain data objects/arrays.
 * @param {any} obj Input value to clone
 * @returns {any} Deeply cloned value, or `undefined`
 */
export function deepClone(obj) {
  if (obj !== undefined) {
    return JSON.parse(JSON.stringify(obj));
  }
}

/**
 * Create a new object with properties updated from `updates`.
 * Does not mutate the original object.
 * @param {any} obj Source object
 * @param {any} updates Partial object to merge in
 * @param {boolean} isDeepClone If true, deep clone `obj` before merging
 * @returns {any} New object containing merged properties
 */
export function updateObjects(obj, updates, isDeepClone) {
  const newObj = isDeepClone
    ? deepClone(obj)
    : {
        ...obj,
      };

  Object.assign(newObj, updates);

  return newObj;
}

/**
 * Assign specified keys from `source` to `target` if they exist in `source`.
 * @param {any} target The object to assign properties to (mutated in place)
 * @param {any} source The object to copy properties from
 * @param {string[]} omitKeys An array of keys to omit from `source` when assigning to `target`
 * @returns {any} The updated `target` object
 */
export function assignObject(target, source, omitKeys) {
  if (!target || !source) {
    return target;
  }

  const keysSet = new Set(omitKeys);

  for (const key in source) {
    if (!keysSet.has(key)) {
      target[key] = source[key];
    }
  }

  return target;
}

/**
 * Create a new array with values at specified indices replaced.
 * Indices outside the array bounds are ignored.
 * @param {any} arr Source array
 * @param {number[]} indexs Indices to update
 * @param {any} values Values to assign (values[i] -> arr[indexs[i]])
 * @param {boolean} isDeepClone If true, deep clone `arr` before updating
 * @returns {any} New array with updated values (does not modify original)
 */
export function updateArrays(arr, indexs, values, isDeepClone) {
  const newArr = isDeepClone ? deepClone(arr) : [...arr];

  indexs.forEach((index) => {
    return (newArr[index] = values[index]);
  });

  return newArr;
}

/**
 * Return elements from `arr1` that are not present in `arr2`.
 * @param {string[]} arr1 Primary array
 * @param {string[]} arr2 Exclusion array
 * @param {boolean} emptyAsUndefined If true, return `undefined` when no differences
 * @returns {string[]} Difference array or `undefined` when empty and `emptyAsUndefined` is true
 */
export function differenceArray(arr1, arr2, emptyAsUndefined) {
  const arr2Set = new Set(arr2);
  const result = [];

  arr1.forEach((item) => {
    if (!arr2Set.has(item)) {
      result.push(item);
    }
  });

  if (result.length) {
    return result;
  }

  return emptyAsUndefined ? undefined : result;
}

/**
 * Remove items that appear in `arr2` from a nested array structure.
 * Input may be a string, an array of strings, or nested arrays of strings.
 * If removals empty a nested array, it is collapsed/removed:
 * - Empty array -> `undefined`
 * - Single item array -> that item
 * @param {any} arr1 Nested array structure
 * @param {any} arr2 Values to remove
 * @returns {any} Updated structure after removals (may be string, array, or `undefined`)
 */
export function removeNestedArrayItems(arr1, arr2) {
  const removeSet = new Set(arr2);

  function helper(node) {
    // leaf
    if (!Array.isArray(node)) {
      return removeSet.has(node) ? undefined : node;
    }

    let result;

    for (let i = 0; i < node.length; i++) {
      const oldItem = node[i];
      const newItem = helper(oldItem);

      // chưa có thay đổi
      if (!result) {
        if (newItem !== oldItem) {
          result = node.slice(0, i);

          if (newItem !== undefined) {
            result.push(newItem);
          }
        }
      } else {
        if (newItem !== undefined) {
          result.push(newItem);
        }
      }
    }

    // không đổi → return original
    if (result === undefined) {
      return node;
    }

    // collapse
    const rlen = result.length;
    if (rlen === 0) {
      return;
    } else if (rlen === 1) {
      return result[0];
    }

    return result;
  }

  return helper(arr1);
}

/**
 * Find the index of `target` in a nested array `arr`, and whether it is nested.
 * @param {any} arr A nested array to search through
 * @param {any} target The value to find
 * @returns {{index: number, isNested: boolean}} An object containing:
 *   - `index`: the index of the first occurrence of `target` in `arr` (or -1 if not found)
 *   - `isNested`: true if `target` is found within a nested array, false if found at top level
 */
export function findNestedArrayItem(arr, target) {
  function hasTarget(arr, target) {
    if (!Array.isArray(arr)) {
      return false;
    }

    for (const item of arr) {
      if (item === target || (Array.isArray(item) && hasTarget(item, target))) {
        return true;
      }
    }

    return false;
  }

  for (let i = 0; i < arr.length; i++) {
    if (arr[i] === target) {
      return {
        index: i,
        isNested: false,
      };
    }

    if (hasTarget(arr[i], target)) {
      return {
        index: i,
        isNested: true,
      };
    }
  }

  return {
    index: -1,
    isNested: false,
  };
}

/**
 * Compare two string arrays.
 * @param {string[]} arr1 First array
 * @param {string[]} arr2 Second array
 * @param {boolean} order If true, compare by order; if false, compare as sets
 * @returns {boolean} True if arrays are equal under the chosen comparison
 */
export function compareArray(arr1, arr2, order) {
  if (arr1.length !== arr2.length) {
    return false;
  }

  if (order) {
    for (let i = 0; i < arr1.length; i++) {
      if (arr1[i] !== arr2[i]) {
        return false;
      }
    }

    return true;
  } else {
    const setA = new Set(arr1);
    if (setA.size !== arr2.length) {
      return false;
    }

    return arr2.every((item) => {
      return setA.has(item);
    });
  }
}

/**
 * Clear all properties from an object, leaving it empty.
 * @param {any} obj The object to clear (mutated in place)
 * @returns {void}
 */
export function clearObject(obj) {
  for (const key in obj) {
    delete obj[key];
  }
}

/**
 * Check if any of the specified fields exist in the object.
 * @param {any} obj The object to check
 * @param {string[]} fields An array of field names to check for
 * @returns {boolean} True if at least one field exists in the object, false otherwise
 */
export function hasAnyFields(obj, fields) {
  return fields.some((field) => {
    return field in obj;
  });
}

/**
 * Check if all of the specified fields exist in the object.
 * @param {any} obj The object to check
 * @param {string[]} fields An array of field names to check for
 * @returns {boolean} True if all fields exist in the object, false otherwise
 */
export function hasAllFields(obj, fields) {
  return fields.every((field) => {
    return field in obj;
  });
}

/**
 * Check if the values of the specified fields are equal between two objects.
 * @param {any} obj1 The first object to compare
 * @param {any} obj2 The second object to compare
 * @param {string[]} fields An array of field names to compare
 * @returns {boolean} True if all specified fields have equal values in both objects, false otherwise
 */
export function isEqualFields(obj1, obj2, fields) {
  return fields.every((field) => {
    return obj1[field] === obj2[field];
  });
}

/** Remove specified fields from an object in place. */
export function removeFields(obj, fields) {
  for (const key of fields) {
    delete obj[key];
  }

  return obj;
}

/** Create an object containing only the requested fields. */
export function pickFields(obj, fields) {
  const result = {};

  for (const key of fields) {
    if (key in obj) {
      result[key] = obj[key];
    }
  }

  return result;
}

/** Check whether any selected field differs between two objects. */
export function isDifferentFields(obj1, obj2, fields) {
  return fields.some((field) => {
    return obj1[field] !== obj2[field];
  });
}

/** Parse JSON without throwing and return an optional fallback on failure. */
export function parseStringJSON(value, fallback) {
  try {
    return {
      result: JSON.parse(value),
    };
  } catch (error) {
    return {
      result: fallback,
      error: error instanceof Error ? error : new Error(String(error)),
    };
  }
}

/**
 * Parse XML without throwing when DOMParser is available in the runtime.
 * In Node, a caller may provide a DOMParser polyfill through `globalThis`.
 */
export function parseStringXML(value, fallback) {
  try {
    if (typeof globalThis.DOMParser !== "function") {
      throw new Error("DOMParser is not available in this runtime.");
    }

    const result = new globalThis.DOMParser().parseFromString(
      value,
      "application/xml",
    );
    const parserError = result.querySelector("parsererror");

    if (parserError) {
      throw new Error(parserError.textContent?.trim() || "XML is invalid.");
    }

    return {
      result,
    };
  } catch (error) {
    return {
      result: fallback,
      error: error instanceof Error ? error : new Error(String(error)),
    };
  }
}

/** Check if a value is a plain object. */
export function isRecord(value) {
  return Object.prototype.toString.call(value) === "[object Object]";
}

/** Check if a value is an array. */
export function isArray(value) {
  return Array.isArray(value);
}

export const hasOwnKey = (value, key) => {
  return Object.prototype.hasOwnProperty.call(value, key);
};

const createJSONContainer = (nextSegment) => {
  return typeof nextSegment === "number" ? [] : {};
};

/** Encode a JSON path into a collision-free key suitable for maps. */
export const getJSONPathKey = (path) => {
  return JSON.stringify(path);
};

/** Return the value at a JSON path, or `undefined` for an invalid path. */
export const getJSONValueAtPath = (value, path) => {
  let currentValue = value;

  for (const segment of path) {
    if (
      Array.isArray(currentValue) &&
      typeof segment === "number" &&
      segment >= 0 &&
      segment < currentValue.length
    ) {
      currentValue = currentValue[segment];
    } else if (
      isRecord(currentValue) &&
      typeof segment === "string" &&
      hasOwnKey(currentValue, segment)
    ) {
      currentValue = currentValue[segment];
    } else {
      return;
    }
  }

  return currentValue;
};

/** Set a nested value, cloning only containers on the changed branch. */
export const setNestedValue = (value, path, nextValue, mutable = false) => {
  if (path.length === 0) {
    return nextValue;
  }

  const [segment, ...remainingPath] = path;

  if (Array.isArray(value) && typeof segment === "number") {
    if (segment < 0 || !Number.isInteger(segment)) {
      return value;
    }

    const currentChild = value[segment];
    const updatedChild = remainingPath.length
      ? setNestedValue(
          currentChild ?? createJSONContainer(remainingPath[0]),
          remainingPath,
          nextValue,
          mutable,
        )
      : nextValue;

    if (currentChild === updatedChild && segment in value) {
      return value;
    }

    if (mutable) {
      value[segment] = updatedChild;
      return value;
    }

    const nextArray = [...value];
    nextArray[segment] = updatedChild;
    return nextArray;
  }

  if (!isRecord(value) || typeof segment !== "string") {
    return value;
  }

  const currentChild = value[segment];
  const updatedChild = remainingPath.length
    ? setNestedValue(
        currentChild ?? createJSONContainer(remainingPath[0]),
        remainingPath,
        nextValue,
        mutable,
      )
    : nextValue;

  if (currentChild === updatedChild && hasOwnKey(value, segment)) {
    return value;
  }

  if (mutable) {
    value[segment] = updatedChild;
    return value;
  }

  return {
    ...value,
    [segment]: updatedChild,
  };
};

/** Delete a nested value, cloning only containers on the changed branch. */
export const deleteNestedValue = (value, path, mutable = false) => {
  if (path.length === 0) {
    return value;
  }

  const [segment, ...remainingPath] = path;

  if (Array.isArray(value) && typeof segment === "number") {
    if (segment < 0 || segment >= value.length || !Number.isInteger(segment)) {
      return value;
    }

    if (remainingPath.length === 0) {
      if (mutable) {
        value.splice(segment, 1);
        return value;
      }

      return value.filter((_, index) => {
        return index !== segment;
      });
    }

    const currentChild = value[segment];
    const updatedChild = deleteNestedValue(
      currentChild,
      remainingPath,
      mutable,
    );

    if (currentChild === updatedChild) {
      return value;
    }

    if (mutable) {
      value[segment] = updatedChild;
      return value;
    }

    const nextArray = [...value];
    nextArray[segment] = updatedChild;
    return nextArray;
  }

  if (
    !isRecord(value) ||
    typeof segment !== "string" ||
    !hasOwnKey(value, segment)
  ) {
    return value;
  }

  if (remainingPath.length === 0) {
    if (mutable) {
      delete value[segment];
      return value;
    }

    const nextRecord = {
      ...value,
    };
    delete nextRecord[segment];
    return nextRecord;
  }

  const currentChild = value[segment];
  const updatedChild = deleteNestedValue(currentChild, remainingPath, mutable);

  if (currentChild === updatedChild) {
    return value;
  }

  if (mutable) {
    value[segment] = updatedChild;
    return value;
  }

  return {
    ...value,
    [segment]: updatedChild,
  };
};

/** Rename an object key at `parentPath`, preserving key order and values. */
export const renameJSONKeyAtPath = (
  value,
  parentPath,
  oldKey,
  newKey,
  mutable = false,
) => {
  const normalizedKey = newKey.trim();
  const parent = getJSONValueAtPath(value, parentPath);

  if (
    !normalizedKey ||
    oldKey === normalizedKey ||
    !isRecord(parent) ||
    !hasOwnKey(parent, oldKey) ||
    hasOwnKey(parent, normalizedKey)
  ) {
    return value;
  }

  const renamedParent = Object.keys(parent).reduce((result, key) => {
    result[key === oldKey ? normalizedKey : key] = parent[key];
    return result;
  }, {});

  if (mutable) {
    Object.keys(parent).forEach((key) => {
      return delete parent[key];
    });
    Object.assign(parent, renamedParent);
    return value;
  }

  return setNestedValue(value, parentPath, renamedParent);
};

/** Add a default child to an array or object at `path`. */
export const addJSONChildAtPath = (value, path, mutable = false) => {
  const parent = getJSONValueAtPath(value, path);

  if (Array.isArray(parent)) {
    if (mutable) {
      parent.push(null);
      return value;
    }

    return setNestedValue(value, path, [...parent, null]);
  }

  if (!isRecord(parent)) {
    return value;
  }

  let index = 1;
  let key = "newKey";

  while (hasOwnKey(parent, key)) {
    key = `newKey${index}`;
    index += 1;
  }

  if (mutable) {
    parent[key] = "";
    return value;
  }

  return setNestedValue(value, path, {
    ...parent,
    [key]: "",
  });
};

/** Sort object keys recursively while preserving array item order. */
export const sortJSONKeys = (value) => {
  if (Array.isArray(value)) {
    return value.map(sortJSONKeys);
  }

  if (isRecord(value)) {
    return Object.keys(value)
      .sort((firstKey, secondKey) => {
        return firstKey.localeCompare(secondKey);
      })
      .reduce((sorted, key) => {
        sorted[key] = sortJSONKeys(value[key]);
        return sorted;
      }, {});
  }

  return value;
};

/** Return map entries that expand every object and array in a JSON value. */
export const getExpandedJSONPathKeys = (value, path = [], expanded = {}) => {
  if (!Array.isArray(value) && !isRecord(value)) {
    return expanded;
  }

  expanded[getJSONPathKey(path)] = true;

  if (Array.isArray(value)) {
    value.forEach((child, index) => {
      return getExpandedJSONPathKeys(child, [...path, index], expanded);
    });
  } else {
    Object.entries(value).forEach(([key, child]) => {
      return getExpandedJSONPathKeys(child, [...path, key], expanded);
    });
  }

  return expanded;
};

/** Ensure a user-provided filename uses the `.json` extension. */
export const normalizeJSONFileName = (fileName) => {
  return fileName.trim().toLowerCase().endsWith(".json")
    ? fileName.trim()
    : `${fileName.trim() || "data"}.json`;
};

/** Apply undefined defaults to a target object in place. */
export function applyDefaults(target, defaults, keys) {
  if (!defaults) {
    return target;
  }

  for (const key of keys ?? Object.keys(defaults)) {
    const value = defaults[key];

    if (value !== undefined && target[key] === undefined) {
      target[key] = value;
    }
  }

  return target;
}

/** Return an existing set or materialize another iterable as a set. */
export function toSet(iterable) {
  return iterable instanceof Set ? iterable : new Set(iterable);
}
