import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { extname, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(fileURLToPath(new URL('.', import.meta.url)));
const apiBase = 'https://api.londonstrategicedge.com';
const apiKey = process.env.LSE_API_KEY || '71f880e1d2ef471664f3b6c04c6dc1e618f94e51f68c87522bc6dcbc0ca173a5';
const vaultApiKey = process.env.LSE_API_KEY || '';
const symbolCategories = new Set(['stock', 'forex', 'futures', 'commodity', 'index', 'crypto', 'etf']);
const vaultTimeframes = {
	'1S': '1s',
	'5S': '5s',
	'15S': '15s',
	'30S': '30s',
	'1': '1m',
	'3': '3m',
	'5': '5m',
	'15': '15m',
	'30': '30m',
	'60': '1h',
	'240': '4h',
	'1D': '1d',
	'1W': '1w',
	'1M': '1mo',
};
const datasetByCategory = {
	stock: 'stocks',
	forex: 'fx',
	futures: 'futures',
	commodity: 'commodity',
	index: 'index',
	crypto: 'crypto',
	etf: 'etf',
};
const legacyResolutionTables = {
	'5': 'x_candles_5m',
	'15': 'x_candles_15m',
	'60': 'x_candles_1h',
	'240': 'x_candles_4h',
	'1D': 'x_candles_1d',
};
const aggregateIntervals = {
	'5': 5 * 60 * 1000,
	'15': 15 * 60 * 1000,
	'60': 60 * 60 * 1000,
	'240': 4 * 60 * 60 * 1000,
	'1D': 24 * 60 * 60 * 1000,
};
const mimeTypes = {
	'.css': 'text/css; charset=utf-8',
	'.html': 'text/html; charset=utf-8',
	'.js': 'text/javascript; charset=utf-8',
	'.json': 'application/json; charset=utf-8',
	'.svg': 'image/svg+xml',
	'.wasm': 'application/wasm',
};

function sendJson(response, status, body) {
	response.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8' });
	response.end(JSON.stringify(body));
}

async function requestApi(endpoint, parameters, key = apiKey) {
	const url = new URL(`${apiBase}/${endpoint}`);
	for (const [key, value] of Object.entries(parameters)) {
		if (value !== undefined) url.searchParams.set(key, value);
	}
	const upstream = await fetch(url, { headers: { 'x-api-key': key } });
	return {
		status: upstream.status,
		contentType: upstream.headers.get('content-type') || 'application/json; charset=utf-8',
		body: await upstream.text(),
	};
}

async function forwardApi(response, endpoint, parameters) {
	const result = await requestApi(endpoint, parameters);
	response.writeHead(result.status, { 'Content-Type': result.contentType });
	response.end(result.body);
}

async function requestRows(endpoint, parameters) {
	const result = await requestApi(endpoint, parameters);
	if (result.status < 200 || result.status >= 300) {
		throw new Error(`Provider request failed: ${result.status}`);
	}
	return JSON.parse(result.body);
}

function aggregateMinuteRows(rows, resolution) {
	const interval = aggregateIntervals[resolution];
	if (!interval) return [];
	const bars = new Map();
	for (const row of rows) {
		const time = Date.parse(row.timestamp);
		if (!Number.isFinite(time)) continue;
		const bucket = Math.floor(time / interval) * interval;
		const open = Number(row.open);
		const high = Number(row.high);
		const low = Number(row.low);
		const close = Number(row.close);
		const volume = Number(row.volume || 0);
		const bar = bars.get(bucket);
		if (bar) {
			bar.high = Math.max(bar.high, high);
			bar.low = Math.min(bar.low, low);
			bar.close = close;
			bar.volume += volume;
		} else {
			bars.set(bucket, {
				timestamp: new Date(bucket).toISOString(),
				open,
				high,
				low,
				close,
				volume,
			});
		}
	}
	return [...bars.values()].sort((left, right) => Date.parse(left.timestamp) - Date.parse(right.timestamp));
}

function handleStream(response, url) {
	if (!vaultApiKey) {
		sendJson(response, 503, { error: 'Set LSE_API_KEY to enable live streaming' });
		return;
	}
	const symbol = url.searchParams.get('symbol') || '';
	if (!/^[a-z0-9._/-]+$/i.test(symbol)) {
		sendJson(response, 400, { error: 'Invalid symbol' });
		return;
	}

	response.writeHead(200, {
		'Content-Type': 'text/event-stream; charset=utf-8',
		'Cache-Control': 'no-cache, no-transform',
		'Connection': 'keep-alive',
		'X-Accel-Buffering': 'no',
	});
	response.write(': connected\n\n');

	let stopped = false;
	let authenticated = false;
	let subscribed = false;
	let keepalive;
	let socket;
	const stop = () => {
		if (stopped) return;
		stopped = true;
		clearInterval(keepalive);
		if (socket && socket.readyState < WebSocket.CLOSING) socket.close();
	};
	response.on('close', stop);

	try {
		socket = new WebSocket('wss://data-ws.londonstrategicedge.com');
	} catch {
		response.end();
		return;
	}
	socket.addEventListener('message', (event) => {
		let message;
		try {
			message = JSON.parse(String(event.data));
		} catch {
			return;
		}
		if (message.type === 'welcome' && !authenticated) {
			socket.send(JSON.stringify({ action: 'auth', api_key: vaultApiKey }));
			return;
		}
		if (message.type === 'authenticated' && !subscribed) {
			authenticated = true;
			subscribed = true;
			socket.send(JSON.stringify({ action: 'subscribe', symbol }));
			keepalive = setInterval(() => {
				if (socket.readyState === WebSocket.OPEN) socket.send(JSON.stringify({ action: 'ping' }));
			}, 30000);
			return;
		}
		if (message.type === 'tick' && message.symbol === symbol && !response.destroyed) {
			response.write(`data: ${JSON.stringify(message)}\n\n`);
			return;
		}
		if (message.type === 'error' && !response.destroyed) {
			response.write(`event: provider_error\ndata: ${JSON.stringify({ message: message.message || message.code || 'Live feed rejected the request' })}\n\n`);
			response.end();
		}
	});
	socket.addEventListener('close', () => {
		clearInterval(keepalive);
		if (!response.destroyed) response.end();
	});
	socket.addEventListener('error', () => {
		if (!response.destroyed) response.end();
	});
}

function withDisplaySymbol(item) {
	if (item.symbol === 'NASDAQ100.F') {
		return {
			...item,
			symbol: 'NQ',
			provider_symbol: item.symbol,
			display_name: 'E-mini Nasdaq-100 Futures',
		};
	}
	return { ...item, provider_symbol: item.symbol };
}

async function handleApi(request, response, url) {
	if (request.method !== 'GET') {
		sendJson(response, 405, { error: 'Method not allowed' });
		return;
	}

	if (url.pathname === '/api/status') {
		sendJson(response, 200, {
			vaultEnabled: Boolean(vaultApiKey),
			realtimeEnabled: Boolean(vaultApiKey),
		});
		return;
	}
	if (url.pathname === '/api/stream') {
		handleStream(response, url);
		return;
	}
	if (url.pathname === '/api/latest-tick') {
		const symbol = url.searchParams.get('symbol') || '';
		if (!/^[a-z0-9._/-]+$/i.test(symbol)) {
			sendJson(response, 400, { error: 'Invalid symbol' });
			return;
		}
		await forwardApi(response, 'tickdata', {
			select: 'symbol,ts,price,volume',
			symbol: `eq.${symbol}`,
			order: 'ts.desc',
			limit: '1',
		});
		return;
	}

	if (url.pathname === '/api/symbols') {
		const query = (url.searchParams.get('q') || '').replace(/[^a-z0-9 ._/-]/gi, '').slice(0, 60).trim();
		const category = url.searchParams.get('category') || '';
		if (category && !symbolCategories.has(category)) {
			sendJson(response, 400, { error: 'Unsupported symbol category' });
			return;
		}
		if (!query && !category) {
			sendJson(response, 200, []);
			return;
		}
		const parameters = {
			select: 'symbol,display_name,category,candle_table',
			order: 'symbol.asc',
			limit: '30',
		};
		if (category) parameters.category = `eq.${category}`;
		if (query && ['NQ', 'NQ.F', 'NASDAQ100.F'].includes(query.toUpperCase())) {
			parameters.symbol = 'eq.NASDAQ100.F';
		} else if (query) {
			parameters.or = `(symbol.ilike.*${query}*,display_name.ilike.*${query}*)`;
		}
		const result = await requestApi('x_symbols', parameters);
		if (result.status < 200 || result.status >= 300) {
			response.writeHead(result.status, { 'Content-Type': result.contentType });
			response.end(result.body);
			return;
		}
		const symbols = JSON.parse(result.body).map(withDisplaySymbol);
		sendJson(response, 200, symbols);
		return;
	}

	if (url.pathname === '/api/history') {
		const symbol = url.searchParams.get('symbol') || '';
		const symbolTable = url.searchParams.get('table') || '';
		const resolution = url.searchParams.get('resolution') || '';
		const category = url.searchParams.get('category') || 'stock';
		if (!/^[a-z0-9._/-]+$/i.test(symbol) || !/^(?:d_)?candles_[a-z0-9_]+$/i.test(symbolTable)) {
			sendJson(response, 400, { error: 'Invalid symbol or candle table' });
			return;
		}
		if (!symbolCategories.has(category)) {
			sendJson(response, 400, { error: 'Invalid symbol category' });
			return;
		}

		const latest = url.searchParams.get('latest') === '1';
		if (vaultApiKey) {
			const timeframe = vaultTimeframes[resolution];
			if (!timeframe) {
				sendJson(response, 400, { error: 'Unsupported resolution' });
				return;
			}
			const parameters = {
				symbol,
				timeframe,
				order: latest ? 'desc' : 'asc',
				limit: latest ? '2' : '5000',
				dataset: datasetByCategory[category],
			};
			if (!latest) {
				const from = Number(url.searchParams.get('from'));
				const to = Number(url.searchParams.get('to'));
				if (Number.isFinite(from) && Number.isFinite(to) && from > 0 && to >= from) {
					parameters.start = new Date(from * 1000).toISOString();
					parameters.end = new Date(to * 1000).toISOString();
				}
			}
			const result = await requestApi('vault/candles', parameters, vaultApiKey);
			if (result.status < 200 || result.status >= 300) {
				response.writeHead(result.status, { 'Content-Type': result.contentType });
				response.end(result.body);
				return;
			}
			const rows = JSON.parse(result.body).map((row) => ({
				...row,
				timestamp: row.ts || row.timestamp,
			}));
			sendJson(response, 200, rows);
			return;
		}

		const table = resolution === '1' ? symbolTable : legacyResolutionTables[resolution];
		if (!table) {
			sendJson(response, 400, { error: 'Unsupported resolution' });
			return;
		}

		const parameters = {
			select: 'symbol,timestamp,open,high,low,close,volume',
			symbol: `eq.${symbol}`,
			order: `timestamp.${latest ? 'desc' : 'asc'}`,
			limit: latest ? '2' : '5000',
		};
		if (!latest) {
			const from = Number(url.searchParams.get('from'));
			const to = Number(url.searchParams.get('to'));
			if (Number.isFinite(from) && Number.isFinite(to) && from > 0 && to >= from) {
				parameters.and = `(timestamp.gte.${new Date(from * 1000).toISOString()},timestamp.lte.${new Date(to * 1000).toISOString()})`;
			}
		}
		const result = await requestApi(table, parameters);
		if (result.status < 200 || result.status >= 300) {
			response.writeHead(result.status, { 'Content-Type': result.contentType });
			response.end(result.body);
			return;
		}
		let rows = JSON.parse(result.body);
		if (resolution !== '1' && rows.length === 0) {
			const minuteParameters = {
				select: 'symbol,timestamp,open,high,low,close,volume',
				symbol: `eq.${symbol}`,
				order: `timestamp.${latest ? 'desc' : 'asc'}`,
				limit: latest ? '2000' : '5000',
			};
			if (!latest) {
				const from = Number(url.searchParams.get('from'));
				const to = Number(url.searchParams.get('to'));
				if (Number.isFinite(from) && Number.isFinite(to) && from > 0 && to >= from) {
					minuteParameters.and = `(timestamp.gte.${new Date(from * 1000).toISOString()},timestamp.lte.${new Date(to * 1000).toISOString()})`;
				}
			}
			rows = aggregateMinuteRows(await requestRows(symbolTable, minuteParameters), resolution);
			if (latest) rows = rows.slice(-2);
		}
		sendJson(response, result.status, rows);
		return;
	}

	sendJson(response, 404, { error: 'Not found' });
}

const server = createServer(async (request, response) => {
	try {
		const url = new URL(request.url, 'http://localhost');
		if (url.pathname.startsWith('/api/')) {
			await handleApi(request, response, url);
			return;
		}

		const pathname = decodeURIComponent(url.pathname === '/' ? '/index.html' : url.pathname);
		const filePath = resolve(root, `.${pathname}`);
		if (filePath !== root && !filePath.startsWith(`${root}${sep}`)) {
			response.writeHead(403);
			response.end('Forbidden');
			return;
		}

		const content = await readFile(filePath);
		response.writeHead(200, {
			'Content-Type': mimeTypes[extname(filePath)] || 'application/octet-stream',
		});
		response.end(content);
	} catch (error) {
		if (error instanceof URIError) {
			sendJson(response, 400, { error: 'Invalid URL' });
		} else if (error.code === 'ENOENT' || error.code === 'EISDIR') {
			response.writeHead(404);
			response.end('Not found');
		} else {
			console.error(error);
			sendJson(response, 502, { error: 'Provider request failed' });
		}
	}
});

const port = Number(process.env.PORT || 8000);
server.listen(port, '127.0.0.1', () => {
	console.log(`Chart available at http://localhost:${port}`);
});