module.exports = {
  plugins: [
    "@stylistic",
    '@typescript-eslint'
  ],
  parser: '@typescript-eslint/parser',
  "env": {
    "commonjs": true,
    "es6": true
  },
  "parserOptions": {
    "ecmaVersion": 2017
  },
  "extends": ["eslint:recommended", "plugin:@typescript-eslint/eslint-recommended", 'plugin:@typescript-eslint/recommended'],
  "rules": {
    "indent": [
      "error",
      2,
      { "SwitchCase": 1 }
    ],
    "linebreak-style": [
      "error",
      "unix"
    ],
    "quotes": [
      "off",
      "single"
    ],
    "semi": [
      "error",
      "always"
    ],
    "no-constant-condition": [
      "error",
      { "checkLoops": false }
    ],
    "strict": ["error", "global"],
    "array-callback-return": "error",
    "curly": "error",
    "no-prototype-builtins": "off",
    "@typescript-eslint/no-namespace": "off",
    "no-unused-vars": "off",
    "@typescript-eslint/no-duplicate-enum-values": "off",
    "@typescript-eslint/no-unused-vars": ["error", {
      "argsIgnorePattern": "^_",
      "caughtErrorsIgnorePattern": "^_"
    }],
    "@typescript-eslint/no-unsafe-function-type": "off",
    "@typescript-eslint/ban-ts-comment": "off",
    "global-require": "error",
    "eqeqeq": ["error", "allow-null"],

    // make sure for-in loops have an if statement
    "guard-for-in": "error",
    "no-alert": "error",
    "no-caller": "error",
    "no-case-declarations": "error",
    "no-else-return": "error",
    "no-empty-pattern": "error",
    "no-eval": "error",
    "no-extend-native": "error",
    "no-extra-bind": "error",
    "no-extra-label": "error",
    "no-floating-decimal": "error",
    "no-global-assign": ["error", { exceptions: [] }],
    "no-implicit-coercion": ["off", {
      boolean: false,
      number: true,
      string: true,
      allow: [],
    }],
    "no-implied-eval": "error",
    "no-labels": ["error", { allowLoop: false, allowSwitch: false }],
    "no-lone-blocks": "error",
    "no-loop-func": "error",
    //
    // disallow use of multiple spaces
    "no-multi-spaces": "error",
    "no-new": "error",
    "no-new-func": "error",
    "no-new-wrappers": "error",
    "no-octal-escape": "error",
    "no-proto": "error",
    "no-redeclare": "off",
    "no-restricted-properties": ["error", {
      object: "arguments",
      property: "callee",
      message: "arguments.callee is deprecated",
    }, {
      property: "__defineGetter__",
      message: "Please use Object.defineProperty instead.",
    }, {
      property: "__defineSetter__",
      message: "Please use Object.defineProperty instead.",
    }],
    "no-self-assign": "error",
    "no-self-compare": "error",
    "no-sequences": "error",
    "no-throw-literal": "error",
    "no-unmodified-loop-condition": "off",
    "no-unused-expressions": ["error", {
      allowShortCircuit: false,
      allowTernary: false,
    }],
    "@typescript-eslint/no-this-alias": "off",
    "@typescript-eslint/no-explicit-any": "off",
    "no-useless-call": "off",
    "no-useless-concat": "error",
    "no-useless-escape": "error",
    "no-useless-return": "error",
    "no-void": "error",
    "no-with": "error",
    "no-buffer-constructor": "error",
    radix: "error",
    "no-var": "error",
    "prefer-const": "error",
    "arrow-body-style": ["error", "as-needed"],
    "arrow-spacing": "error",
    "no-confusing-arrow": ["error", { "allowParens": true }],
    "yoda": "error",
    "constructor-super": "error",
    "require-await": "error",
    "require-atomic-updates": "off",
    "prefer-rest-params": "off",
    "sort-imports": 
    [
      "error", 
      { 
        "ignoreCase": true, 
        "ignoreDeclarationSort": true 
      }
    ], 
    "@typescript-eslint/no-require-imports": "off",
    "prefer-spread": "off",
    "@typescript-eslint/no-unsafe-function-types": "off",
  },
  "globals": {
    "Buffer": false,
    "Promise": true,
    "Symbol": false,
    "Uint16Array": false,
    "Int32Array": false,
    "Int8Array": false,
    "BigInt": false,
    "process": false,
    "setInterval": false,
    "setTimeout": false,
    "setImmediate": false,
    "clearInterval": false,
    "clearTimeout": false,
    "describe": false,
    "xdescribe": false,
    "it": false,
    "xit": false,
    "context": false,
    "after": false,
    "afterEach": false,
    "before": false,
    "beforeEach": false,
    "__filename": false
  }
};
