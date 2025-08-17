import globals from "globals";

export default [
  {
    files: ["src/**/*.js"],
    languageOptions: {
      ecmaVersion: 2021,
      sourceType: "module",
      globals: {
        ...globals.node,
        myCustomGlobal: "readonly",
      },
    },
    rules: {
      "no-undef": "error",
      semi: ["error", "always"],
    },
  },
];
