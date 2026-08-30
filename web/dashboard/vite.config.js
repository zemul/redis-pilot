import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import tailwindcss from '@tailwindcss/vite';
import path from 'path';

const apiTarget = process.env.VITE_API_TARGET || 'http://10.70.100.67:8080';

export default defineConfig({
  base: '/dashboard/',
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  build: {
    outDir: '../../internal/server/dashboard_dist',
    emptyOutDir: true
  },
  server: {
    proxy: {
      '/api': apiTarget,
      '/auth': apiTarget,
      '/instance': apiTarget,
      '/node': apiTarget,
      '/backup': apiTarget,
      '/audit': apiTarget,
      '/server': apiTarget,
      '/sentinel': apiTarget,
      '/inventory': apiTarget,
      '/tasks': apiTarget
    }
  }
});
