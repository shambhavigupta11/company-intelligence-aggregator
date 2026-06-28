import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  // Base path for built assets. Defaults to '/' for local dev/preview; the
  // Pages deploy sets VITE_BASE='/mosaic/' so asset URLs resolve under the repo.
  base: process.env.VITE_BASE || '/',
  server: {
    port: 5173,
    proxy: {
      '/api': 'http://localhost:5050',
    },
  },
})
