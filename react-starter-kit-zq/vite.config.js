import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import jsconfigPaths from "vite-jsconfig-paths";
import eslint from "vite-plugin-eslint";
import svgr from "vite-plugin-svgr";
import tailwindcss from "@tailwindcss/vite";

const isVitest = Boolean(process.env.VITEST);

const apiProxy = {
  "/api/pos01": {
    target: process.env.VITE_POS01_TARGET || "http://localhost:8081",
    changeOrigin: true,
    rewrite: (path) => path.replace(/^\/api\/pos01/, "/api"),
  },
  "/api/pos02": {
    target: process.env.VITE_POS02_TARGET || "http://localhost:8082",
    changeOrigin: true,
    rewrite: (path) => path.replace(/^\/api\/pos02/, "/api"),
  },
  "/api/online": {
    target: process.env.VITE_ONLINE_TARGET || "http://localhost:8083",
    changeOrigin: true,
    rewrite: (path) => path.replace(/^\/api\/online/, "/api"),
  },
  "/api/inventory": {
    target: process.env.VITE_INVENTORY_TARGET || "http://localhost:8084",
    changeOrigin: true,
    rewrite: (path) => path.replace(/^\/api\/inventory/, "/api"),
  },
  "/api/kitchen": {
    target: process.env.VITE_KITCHEN_TARGET || "http://localhost:8085",
    changeOrigin: true,
  },
  "/api/reporting": {
    target: process.env.VITE_REPORTING_TARGET || "http://localhost:8080",
    changeOrigin: true,
  },
};

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [
    react(),
    jsconfigPaths(),
    svgr(),
    !isVitest && eslint(),
    !isVitest && tailwindcss(),
  ].filter(Boolean),
  server: {
    proxy: apiProxy,
  },
  preview: {
    proxy: apiProxy,
  },
  test: {
    environment: "jsdom",
    globals: true,
    setupFiles: "./src/test/setupTests.js",
    css: false,
    testTimeout: 30000,
    fileParallelism: false,
    isolate: false,
  },
});
