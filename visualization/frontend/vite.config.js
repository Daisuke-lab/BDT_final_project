import { defineConfig, loadEnv } from 'vite';
import path from 'path';

export default defineConfig(({ command, mode }) => {
  const env = loadEnv(mode, process.cwd(), '');
  const backendUrl = env.VITE_BACKEND_BASE_URL || 'https://visualization-backend.lifespacedigital.com';
  const scalaVersion = '3.7.3';
  console.log(backendUrl)
  return {
    define: {
      // ScalaJS compiles `sys.env.getOrElse("BACKEND_BASE_URL", ...)` to
      // `process.env["BACKEND_BASE_URL"]` — Vite replaces it at build time.
      'process.env.BACKEND_BASE_URL': JSON.stringify(backendUrl),
    },
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
