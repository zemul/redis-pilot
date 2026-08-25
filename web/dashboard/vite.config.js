import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

const apiTarget = process.env.VITE_API_TARGET || 'http://10.70.100.67:8080';

export default defineConfig({
  base: '/dashboard/',
  plugins: [react()],
  build: {
    outDir: '../../internal/server/dashboard_dist',
    emptyOutDir: true
  },
  server: {
    proxy: {
      '/auth': apiTarget,
      '/instance': apiTarget,
      '/node': apiTarget,
      '/backup': apiTarget,
      '/audit': apiTarget,
      '/server': apiTarget,
      '/inventory': apiTarget
    }
  }
});
