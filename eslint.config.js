export default [
  {
    files: ["src/*.js"],
    languageOptions: {
      ecmaVersion: 2021,
      sourceType: "module",
    },
    rules: {
      "no-undef": "error",
      semi: ["error", "always"],
    },
  },
];
