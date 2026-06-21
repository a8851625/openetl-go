import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  build: {
    outDir: '../resource/public',
    emptyOutDir: true,
    sourcemap: false,
  },
  server: {
    proxy: {
      '/api/v2': 'http://127.0.0.1:8001',
      '/metrics': 'http://127.0.0.1:8001',
    },
  },
});
