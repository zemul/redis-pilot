import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  base: '/dashboard/',
  plugins: [react()],
  build: {
    outDir: '../../internal/server/dashboard_dist',
    emptyOutDir: true
  },
  server: {
    proxy: {
      '/instance': 'http://127.0.0.1:8080',
      '/node': 'http://127.0.0.1:8080',
      '/audit': 'http://127.0.0.1:8080',
      '/inventory': 'http://127.0.0.1:8080'
    }
  }
});
