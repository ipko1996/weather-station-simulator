/// <reference types="vitest/config" />
import { defineConfig } from 'vite'
import { devtools } from '@tanstack/devtools-vite'

import { tanstackRouter } from '@tanstack/router-plugin/vite'

import viteReact from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

const config = defineConfig({
  resolve: { tsconfigPaths: true },
  plugins: [
    devtools(),
    tailwindcss(),
    tanstackRouter({ target: 'react', autoCodeSplitting: true }),
    viteReact(),
  ],
  server: {
    // The dev server impersonates the backend's origin: the browser only ever
    // talks to localhost:3000, so CORS never enters the picture — the same
    // same-origin story nginx provides in the compose deployment. /api goes to
    // the sensor-gateway, /ws to the notification-gateway; rewriteWsOrigin
    // makes the proxied Origin header match what the gateway expects, so its
    // origin check can stay strict even in dev.
    proxy: {
      '/api': { target: 'http://localhost:8080', changeOrigin: true },
      '/ws': { target: 'ws://localhost:8082', ws: true, rewriteWsOrigin: true },
    },
  },
  test: {
    environment: 'jsdom',
    setupFiles: ['./src/test/setup.ts'],
    globals: false,
  },
})

export default config
