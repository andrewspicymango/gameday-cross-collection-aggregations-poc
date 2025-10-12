// fetch-aggregations.js
// Usage:
//   node fetch-aggregations.js fifa 289175
// Env:
//   AGG_BASE (default: http://localhost:8080/1-0/aggregate)

////////////////////////////////////////////////////////////////////////////////
const axios = require('axios');
const BASE = process.env.AGG_BASE || 'http://localhost:8080/1-0/aggregate';
const KEY_SEPARATOR = ' @ ';

////////////////////////////////////////////////////////////////////////////////
/** Build a URL using BASE with pattern /{resource}/{scope}/{id}, encoding scope and id.
 * @param {string} resource - Resource name.
 * @param {string} scope - Scope segment.
 * @param {string|number} id - Identifier.
 * @returns { string } Encoded URL.
 */
function buildUrl(resource, scope, id) {
	// Scope then id, matching your /competitions/{scope}/{id} pattern
	return `${BASE}/${resource}/${encodeURIComponent(scope)}/${encodeURIComponent(id)}`;
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Parse a key string of the form "id @ scope" into an object.
 * @param {string} key - Key string like "289177 @ fifa".
 * @returns {{id: string, scope: string}} Parsed id and scope.
 * @throws {Error} If the key does not contain exactly one separator or if id/scope are missing.
 */
function parseKey(key) {
	// "289177 @ fifa" -> { id: "289177", scope: "fifa" }
	const parts = String(key).split(KEY_SEPARATOR);
	if (parts.length !== 2) throw new Error(`Invalid key format: "${key}"`);
	const [id, scope] = parts.map((s) => s.trim());
	if (!id || !scope) throw new Error(`Missing id/scope in key: "${key}"`);
	return { id, scope };
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Fetch a competition by scope and id, build the URL and return the API response.
 * @param {string} scope - Scope segment used to build the endpoint.
 * @param {string|number} id - Competition identifier.
 * @returns {Promise<object>} Resolves with the competition response object (data.response).
 * @throws {Error} If the response shape is missing or otherwise unexpected.
 */
async function getCompetition(scope, id) {
	const url = buildUrl('competitions', scope, id);
	const { data } = await axios.post(url);

	if (!data || !data.response) {
		throw new Error(`Unexpected competition response shape from ${url}`);
	}
	return data.response;
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Fetch a stage by its external key of the form "id @ scope", build the URL and return the API response.
 * @param {string} stageKey - Stage external key like "289177 @ fifa".
 * @returns {Promise<{ok: boolean, url: string, key: string, data?: object, error?: any, status?: number}>}
 *          Resolves with an object indicating success or failure, including the URL and key used.
 */
async function getStageByKey(stageKey) {
	const { id, scope } = parseKey(stageKey);
	const url = buildUrl('stages', scope, id);
	try {
		const { data } = await axios.post(url);
		return { ok: true, url, key: stageKey, data };
	} catch (err) {
		return {
			ok: false,
			url,
			key: stageKey,
			error: err.response?.data || err.message || String(err),
			status: err.response?.status,
		};
	}
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Fetch an event by its external key of the form "id @ scope", build the URL and return the API response.
 * @param {string} eventKey - Event external key like "event123 @ fifa".
 * @returns {Promise<{ok: boolean, url: string, key: string, data?: object, error?: any, status?: number}>}
 *          Resolves with an object indicating success or failure, including the URL and key used.
 */
async function getEventByKey(eventKey) {
	const { id, scope } = parseKey(eventKey);
	const url = buildUrl('events', scope, id);
	try {
		const { data } = await axios.post(url);
		return { ok: true, url, key: eventKey, data };
	} catch (err) {
		return {
			ok: false,
			url,
			key: eventKey,
			error: err.response?.data || err.message || String(err),
			status: err.response?.status,
		};
	}
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Fetch events for a single stage and return results array.
 * @param {object} stageResult - Stage result object from getStageByKey.
 * @returns {Promise<Array>} Array of event fetch results.
 */
async function fetchEventsForStage(stageResult) {
	if (!stageResult.ok || !stageResult.data?.response) {
		return [];
	}

	const stageData = stageResult.data.response;
	const eventKeys = Array.isArray(stageData.eventKeys) ? stageData.eventKeys : [];

	if (eventKeys.length === 0) {
		return [];
	}

	console.log(`  → Fetching ${eventKeys.length} events for stage "${stageResult.key}"`);

	const eventResults = [];
	for (let i = 0; i < eventKeys.length; i++) {
		const eventKey = eventKeys[i];
		const result = await getEventByKey(eventKey);
		eventResults.push(result);

		// Log progress for large numbers of events
		if ((i + 1) % 10 === 0 || i === eventKeys.length - 1) {
			console.log(`    • Events: ${i + 1}/${eventKeys.length} processed`);
		}
	}

	return eventResults;
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Async CLI entrypoint that reads competition scope/id from process.argv, fetches the competition and its stages,
 * then recursively fetches events for each stage. Logs comprehensive summaries.
 * @async
 * @returns {Promise<void>}
 */
async function run() {
	const [scope, id] = process.argv.slice(2);
	if (!scope || !id) {
		console.error('Usage: node fetch-aggregations.js <competitionScope> <competitionId>');
		console.error('Example: node fetch-aggregations.js fifa 289175');
		process.exit(1);
	}

	console.log(`→ Fetch competition agg: ${scope}/${id}`);
	const comp = await getCompetition(scope, id);
	const stageKeys = Array.isArray(comp.stageKeys) ? comp.stageKeys : [];
	console.log(`✓ Competition "${comp.externalKey}" → ${stageKeys.length} stageKeys`);

	//////////////////////////////////////////////////////////////////////////////
	// Fetch stages 1 at a time
	console.log(`\n→ Fetching ${stageKeys.length} stages...`);
	const stageResults = [];
	for (let i = 0; i < stageKeys.length; i++) {
		const key = stageKeys[i];
		const result = await getStageByKey(key);
		console.log(`✓ Stage "${key}" (${i + 1}/${stageKeys.length})`);
		stageResults.push(result);
	}

	//////////////////////////////////////////////////////////////////////////////
	// Fetch events for each successful stage
	console.log(`\n→ Fetching events for stages...`);
	const allEventResults = [];
	const successfulStages = stageResults.filter((r) => r.ok);

	for (let i = 0; i < successfulStages.length; i++) {
		const stageResult = successfulStages[i];
		const eventResults = await fetchEventsForStage(stageResult);
		allEventResults.push(...eventResults);
		console.log(`✓ Stage "${stageResult.key}": ${eventResults.length} events processed`);
	}

	//////////////////////////////////////////////////////////////////////////////
	// Comprehensive Summary
	const stageOk = stageResults.filter((r) => r.ok);
	const stageFail = stageResults.filter((r) => !r.ok);
	const eventOk = allEventResults.filter((r) => r.ok);
	const eventFail = allEventResults.filter((r) => !r.ok);

	console.log(`\n=== AGGREGATION FETCH SUMMARY ===`);
	console.log(`Competition: ${comp.externalKey}`);
	console.log(`Stages - Total: ${stageResults.length}  OK: ${stageOk.length}  Failed: ${stageFail.length}`);
	console.log(`Events - Total: ${allEventResults.length}  OK: ${eventOk.length}  Failed: ${eventFail.length}`);

	if (stageOk.length) {
		console.log('\n--- Successful Stages ---');
		stageOk.forEach((r) => {
			const stageData = r.data?.response;
			const eventCount = Array.isArray(stageData?.eventKeys) ? stageData.eventKeys.length : 0;
			const msg = r.data?.message || 'OK';
			console.log(`  • ${r.key} (${eventCount} events) :: ${msg}`);
		});
	}

	if (stageFail.length) {
		console.log('\n--- Failed Stages ---');
		stageFail.forEach((r) => {
			console.log(`  • ${r.key}  ←  ${r.url}  :: status=${r.status ?? 'n/a'}`);
			console.log(`    ${typeof r.error === 'string' ? r.error : JSON.stringify(r.error)}`);
		});
	}

	if (eventOk.length) {
		console.log(`\n--- Successful Events (${eventOk.length}) ---`);
		// Group events by stage for better readability
		const eventsByStage = {};
		eventOk.forEach((r) => {
			// Find which stage this event belongs to by looking at stage results
			const belongsToStage = successfulStages.find((stage) => {
				const eventKeys = stage.data?.response?.eventKeys || [];
				return eventKeys.includes(r.key);
			});
			const stageKey = belongsToStage?.key || 'unknown';
			if (!eventsByStage[stageKey]) eventsByStage[stageKey] = [];
			eventsByStage[stageKey].push(r);
		});

		Object.entries(eventsByStage).forEach(([stageKey, events]) => {
			console.log(`  Stage: ${stageKey} (${events.length} events)`);
			events.slice(0, 5).forEach((r) => {
				// Show first 5 events per stage
				const msg = r.data?.message || 'OK';
				console.log(`    • ${r.key} :: ${msg}`);
			});
			if (events.length > 5) {
				console.log(`    ... and ${events.length - 5} more events`);
			}
		});
	}

	if (eventFail.length) {
		console.log(`\n--- Failed Events (showing first 10) ---`);
		eventFail.slice(0, 10).forEach((r) => {
			console.log(`  • ${r.key}  ←  ${r.url}  :: status=${r.status ?? 'n/a'}`);
			console.log(`    ${typeof r.error === 'string' ? r.error : JSON.stringify(r.error)}`);
		});
		if (eventFail.length > 10) {
			console.log(`  ... and ${eventFail.length - 10} more failed events`);
		}
	}

	// Optional: Save results to files
	// const stageDocuments = stageOk.map((r) => r.data?.response).filter(Boolean);
	// const eventDocuments = eventOk.map((r) => r.data?.response).filter(Boolean);
	// require('fs').writeFileSync('stages.json', JSON.stringify(stageDocuments, null, 2));
	// require('fs').writeFileSync('events.json', JSON.stringify(eventDocuments, null, 2));
}

////////////////////////////////////////////////////////////////////////////////
run().catch((e) => {
	console.error('FATAL:', e?.message || e);
	process.exit(1);
});
