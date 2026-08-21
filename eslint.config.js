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
      "no-undef": "off",
      "semi": ["error", "always"],
      "react-hooks/exhaustive-deps": "off",
      "arrow-body-style": ["error", "always"],
      "object-curly-newline": [
        "error",
        {
          ObjectExpression: {
            multiline: true,
            minProperties: 1,
          },
        },
      ],
      curly: ["error", "all"],
    },
  },
];
