# Lakevision Frontend (SvelteKit)

This is the SvelteKit frontend of Lakevision. It visualizes Iceberg table metadata and connects to the backend via REST API.

## Running locally

### Prerequisites

- Node.js 18+

### Setup

```bash
npm install
```

### Start the frontend
```bash
npm run dev -- --port 8081
```

Visit [http://localhost:8081](http://localhost:8081) to view the app.

Environment configuration (e.g., API base URL) is handled at build time using `my.env` from the project root.


### Building

To create a production version of your app:

```bash
npm run build
```

You can preview the production build with:

```bash
npm run preview
```

> To deploy your app, you may need to install an [adapter](https://kit.svelte.dev/docs/adapters) for your target environment.
