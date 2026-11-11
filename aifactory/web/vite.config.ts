import { fileURLToPath } from "node:url";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

const page = (name: string) => fileURLToPath(new URL(name, import.meta.url));

export default defineConfig({
  plugins: [react()],
  build: {
    outDir: "../src/aifactory/web", emptyOutDir: true, assetsDir: "assets",
    // The app, plus a static Mind fixture with invented data for visual review on any deployment.
    rollupOptions: { input: { main: page("index.html"), mind: page("mind-fixture.html") } },
  },
  server: {
    port: 5173,
    proxy: { "/api": "http://127.0.0.1:8000", "/auth": "http://127.0.0.1:8000", "/gateway": "http://127.0.0.1:8000" },
  },
});
