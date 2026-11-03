// StrictMode surfaces unsafe effects/side effects during development.
import { StrictMode } from "react";
// createRoot is React's concurrent-mode entry point for mounting the app.
import { createRoot } from "react-dom/client";
// The root component of the dashboard.
import { App } from "./App";
// Global stylesheet; imported for its side effect (Vite injects the CSS).
import "./styles.css";

// Mount the app into the #root element from index.html (non-null assertion: the element always exists).
createRoot(document.getElementById("root")!).render(
  // StrictMode wrapper double-invokes effects in dev to catch cleanup bugs.
  <StrictMode>
    {/* The whole dashboard lives under App. */}
    <App />
  </StrictMode>,
);
