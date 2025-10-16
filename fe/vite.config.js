import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vite';
import { optimizeCss } from 'carbon-preprocess-svelte';

export default defineConfig({
	plugins: [
		sveltekit(),
		// Optimize CSS from `carbon-components-svelte` when building for production.
		optimizeCss()	
	],
	server: {
		// Proxy API requests to your local FastAPI server
		proxy: {
		  '/api': {
			target: 'http://localhost:8000', 
			changeOrigin: true, // Allow the proxy to add the host header to the target URL
		  },
		},
	  },

	// For even faster cold starts, exclude packages that `optimizeImports` will resolve.
	optimizeDeps: {
		exclude: ['carbon-components-svelte', 'carbon-icons-svelte']
	}
});
