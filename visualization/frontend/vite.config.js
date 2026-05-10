import { defineConfig } from 'vite';
import path from 'path';

export default defineConfig(({ command }) => {
  const scalaVersion = '3.7.3';
  return {
    resolve: {
      alias: {
        "scalajs": command === "serve"
          ? `./target/scala-${scalaVersion}/viz-frontend-fastopt/main.js`
          : `./target/scala-${scalaVersion}/viz-frontend-opt/main.js`,
        "resources": path.resolve(__dirname, "./src/main/resources"),
      }
    },
    server: {
      port: 9876,
      historyApiFallback: true
    },
  };
});
