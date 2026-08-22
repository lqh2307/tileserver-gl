(function (window, document) {
  "use strict";

  function JSONEditor(element, options) {
    this.element = element;
    this.options = options || {};
    this.value = {};
    this.render();
  }

  JSONEditor.prototype.render = function () {
    var self = this;
    this.element.className = "json-editor";
    this.element.innerHTML =
      '<div class="json-editor__toolbar"><button type="button" data-mode="source" class="active">Source</button><button type="button" data-mode="tree">Tree</button></div><textarea class="json-editor__source" spellcheck="false"></textarea><div class="json-editor__tree" hidden></div><div class="json-editor__error" hidden></div>';
    this.source = this.element.querySelector(".json-editor__source");
    this.tree = this.element.querySelector(".json-editor__tree");
    this.error = this.element.querySelector(".json-editor__error");
    this.element.querySelectorAll("[data-mode]").forEach(function (button) {
      button.addEventListener("click", function () {
        self.setMode(button.dataset.mode);
      });
    });
    this.source.addEventListener("input", function () {
      self.parse();
    });
    this.setValue(this.options.value || {});
  };

  JSONEditor.prototype.setMode = function (mode) {
    var self = this;
    if (mode === "tree") {
      if (!this.parse()) return;
      this.tree.innerHTML = "";
      this.renderNode(this.value, this.tree, "");
    }
    this.source.hidden = mode !== "source";
    this.tree.hidden = mode !== "tree";
    this.element.querySelectorAll("[data-mode]").forEach(function (button) {
      button.classList.toggle("active", button.dataset.mode === mode);
    });
  };

  JSONEditor.prototype.parse = function () {
    try {
      this.value = JSON.parse(this.source.value || "{}");
      this.error.hidden = true;
      if (this.options.onChange) this.options.onChange(this.value);
      return true;
    } catch (error) {
      this.error.textContent = "Invalid JSON: " + error.message;
      this.error.hidden = false;
      return false;
    }
  };

  JSONEditor.prototype.setValue = function (value) {
    this.value = value == null ? {} : value;
    this.source.value = JSON.stringify(this.value, null, 2);
    this.error.hidden = true;
  };

  JSONEditor.prototype.getValue = function () {
    return this.parse() ? this.value : null;
  };

  JSONEditor.prototype.getText = function () {
    if (!this.parse()) throw new Error(this.error.textContent);
    return JSON.stringify(this.value);
  };

  JSONEditor.prototype.renderNode = function (value, parent, key) {
    var row = document.createElement("div");
    row.className = "json-editor__node";
    var label = key
      ? '<span class="json-editor__key">' + key + ": </span>"
      : "";
    var isContainer = value !== null && typeof value === "object";
    if (isContainer) {
      var toggle = document.createElement("button");
      toggle.type = "button";
      toggle.className = "json-editor__toggle";
      toggle.textContent = "▼";
      toggle.setAttribute("aria-label", "Collapse " + (key || "root"));
      row.appendChild(toggle);
      row.insertAdjacentHTML(
        "beforeend",
        label + (Array.isArray(value) ? "[" : "{"),
      );
      var children = document.createElement("div");
      children.className = "json-editor__children";
      Object.keys(value).forEach(function (child) {
        this.renderNode(value[child], children, child);
      }, this);
      row.appendChild(children);
      row.insertAdjacentHTML("beforeend", Array.isArray(value) ? "]" : "}");
      toggle.addEventListener("click", function () {
        var collapsed = children.classList.toggle("collapsed");
        toggle.textContent = collapsed ? "▶" : "▼";
        toggle.setAttribute(
          "aria-label",
          (collapsed ? "Expand " : "Collapse ") + (key || "root"),
        );
      });
    } else if (value === null)
      row.innerHTML = label + '<span class="json-editor__null">null</span>';
    else if (typeof value === "string")
      row.innerHTML =
        label +
        '<span class="json-editor__string">&quot;' +
        value
          .replace(/&/g, "&amp;")
          .replace(/</g, "&lt;")
          .replace(/>/g, "&gt;") +
        "&quot;</span>";
    else if (typeof value === "number")
      row.innerHTML =
        label + '<span class="json-editor__number">' + value + "</span>";
    else if (typeof value === "boolean")
      row.innerHTML =
        label + '<span class="json-editor__boolean">' + value + "</span>";
    parent.appendChild(row);
  };

  window.JSONEditor = JSONEditor;
})(window, document);
