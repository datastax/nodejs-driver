module.exports = {
  "env": {
    "commonjs": true,
    "es6": true
  },
  "extends": "eslint:recommended",
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
    "no-unused-vars": ["error", { "args": "none" }],
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
    "no-redeclare": "error",
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
    "no-useless-call": "off",
    "no-useless-concat": "error",
    "no-useless-escape": "error",
    "no-useless-return": "error",
    "no-void": "error",
    "no-with": "error",
    "no-buffer-constructor": "error",
    radix: "error"
  },
  "globals": {
    "Buffer": false,
    "Promise": true,
    "Symbol": false,
    "Uint16Array": false,
    "process": false,
    "setTimeout": false,
    "setImmediate": false,
    "clearTimeout": false,
    "describe": false,
    "it": false,
    "context": false,
    "after": false,
    "afterEach": false,
    "before": false,
    "beforeEach": false,
    "__filename": false
  }
};
