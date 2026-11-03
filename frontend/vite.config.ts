// defineConfig from vitest/config types both the Vite config and the `test` block below.
import { defineConfig } from "vitest/config";
// Enables JSX transform and Fast Refresh for React.
import react from "@vitejs/plugin-react";

// Production: a standalone static site (Render Static Site / any CDN) that calls the API at
// VITE_API_BASE. Development: Vite proxies API calls to VITE_API_TARGET, so there is no CORS
// (the local API by default, or a deployed API URL).
// Where the dev proxy forwards API calls; defaults to the local API.
const target = process.env.VITE_API_TARGET ?? "http://localhost:8000";
// Shared proxy options; changeOrigin rewrites the Host header so the target accepts the request.
const proxy = { target, changeOrigin: true };

// Export the Vite + Vitest configuration.
export default defineConfig({
  // Assets are served from the site root.
  base: "/",
  // Register the React plugin.
  plugins: [react()],
  // Dev server settings.
  server: {
    // Fixed dev port.
    port: 5173,
    // Proxy every API route the dashboard calls so the browser stays same-origin in development.
    proxy: { "/v1": proxy, "/health": proxy, "/metrics": proxy, "/docs": proxy, "/openapi.json": proxy },
  },
  // Unit tests only exercise pure logic, so a Node environment (no DOM) is enough.
  test: { environment: "node" },
});
