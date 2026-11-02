import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";

// Production: a standalone static site (Render Static Site / any CDN) that calls the API at
// VITE_API_BASE. Development: Vite proxies API calls to VITE_API_TARGET, so there is no CORS
// (the local API by default, or e.g. https://txn-api-j67u.onrender.com).
const target = process.env.VITE_API_TARGET ?? "http://localhost:8000";
const proxy = { target, changeOrigin: true };

export default defineConfig({
  base: "/",
  plugins: [react()],
  server: {
    port: 5173,
    proxy: { "/v1": proxy, "/health": proxy, "/metrics": proxy, "/docs": proxy, "/openapi.json": proxy },
  },
  test: { environment: "node" },
});
