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
  const newObj = isDeepClone ? deepClone(obj) : { ...obj };

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

  indexs.forEach((index) => (newArr[index] = values[index]));

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

    return arr2.every((item) => setA.has(item));
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
  return fields.some((field) => field in obj);
}

/**
 * Check if all of the specified fields exist in the object.
 * @param {any} obj The object to check
 * @param {string[]} fields An array of field names to check for
 * @returns {boolean} True if all fields exist in the object, false otherwise
 */
export function hasAllFields(obj, fields) {
  return fields.every((field) => field in obj);
}

/**
 * Check if the values of the specified fields are equal between two objects.
 * @param {any} obj1 The first object to compare
 * @param {any} obj2 The second object to compare
 * @param {string[]} fields An array of field names to compare
 * @returns {boolean} True if all specified fields have equal values in both objects, false otherwise
 */
export function isEqualFields(obj1, obj2, fields) {
  return fields.every((field) => obj1[field] === obj2[field]);
}
