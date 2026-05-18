import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

const VENDOR_PACKAGES = ['react', 'react-dom', 'react-router-dom'];
const ANTD_PACKAGES = ['antd', '@ant-design'];

function getChunkName(id) {
  if (VENDOR_PACKAGES.some((pkg) => id.includes(`node_modules/${pkg}`))) return 'vendor';
  if (ANTD_PACKAGES.some((pkg) => id.includes(`node_modules/${pkg}`))) return 'antd';
  if (id.includes('node_modules/diff2html')) return 'diff';
  return undefined;
}

export default defineConfig({
  plugins: [react()],
  server: {
    port: 3000,
    proxy: {
      '/api': {
        target: 'http://localhost:3001',
        changeOrigin: true,
      },
    },
  },
  build: {
    outDir: 'dist',
    sourcemap: false,
    rollupOptions: {
      output: {
        manualChunks: getChunkName,
      },
    },
  },
});
