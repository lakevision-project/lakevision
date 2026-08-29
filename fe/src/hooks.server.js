// src/hooks.server.js
import { dev } from '$app/environment';

export async function handle({ event, resolve }) {
	// Previously logged the entire `event` object on every request, which buried
	// real output in hundreds of lines and included request cookies. One line, and
	// only in dev.
	if (dev) {
		console.log(`${event.request.method} ${event.url.pathname}`);
	}

	return resolve(event);
	// No CORS headers here: this server is same-origin with the API behind nginx.
	// It previously set Access-Control-Allow-Origin: '*' on every response, which
	// let any site read authenticated page responses.
}

export async function handleError({ error, event }) {
	// Log the real cause -- it is otherwise lost, since the client is shown a
	// generic message deliberately (server errors can carry internal detail).
	console.error(`Unhandled error on ${event?.request?.method} ${event?.url?.pathname}:`, error);

	return {
		message: 'Something went wrong, please try again later.',
		code: error?.code ?? 'UNKNOWN_ERROR'
	};
}
