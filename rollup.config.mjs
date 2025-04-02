// Contents of the file /rollup.config.js
import typescript from '@rollup/plugin-typescript';
import dts from "rollup-plugin-dts";
const config = [{
    input: 'out/index.d.ts',
    output: {
      file: 'out/cassandra-rollup.d.ts',
      format: 'es'
    },
    external: ['events', 'stream', 'util', 'tls', 'net'],
    plugins: [dts()]
  }
];
export default config;