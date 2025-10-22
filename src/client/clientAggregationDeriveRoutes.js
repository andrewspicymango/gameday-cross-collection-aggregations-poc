const { ClientAggregationError, ServerAggregationError } = require('./clientAggregationError.js');
const COMP_SCOPED = new Set(['competition', 'stage', 'event', 'team', 'staff', 'ranking', 'keyMoment', 'keymoment']);

////////////////////////////////////////////////////////////////////////////////
/**
 * Checks if a given type is competition-scoped.
 * @param {string} type - The type to check
 * @returns {boolean} True if the type is competition-scoped, false otherwise
 */
function isCS(type) {
	return COMP_SCOPED.has(type);
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Lists all outgoing edges from a given type in the edges map.
 * @param {Object} EDGES_MAP - Map of type relationships
 * @param {string} fromType - The source type to find edges from
 * @returns {Array<{field: string, to: string}>} Array of edge objects with field and destination type
 */
function listOutEdges(EDGES_MAP, fromType) {
	const outs = EDGES_MAP[fromType] || {};
	return Object.entries(outs).map(([field, to]) => ({ field, to }));
}

//////////////////////////////////////////////////////////////////////////////////
/**
 * Determines if a transition between two types is allowed under the root's scope regime.
 * Rule 1 (rootIsCS=true): Disallow non-competition-scoped -> competition-scoped transitions
 * Rule 2 (rootIsCS=false): Disallow competition-scoped -> competition-scoped transitions
 * @param {string} prevType - The source type of the transition
 * @param {string} nextType - The destination type of the transition
 * @param {boolean} rootIsCS - Whether the root type is competition-scoped
 * @returns {boolean} True if the transition is allowed, false otherwise
 */
function isAllowedTransition(prevType, nextType, rootIsCS) {
	const fromCS = isCS(prevType);
	const toCS = isCS(nextType);
	//////////////////////////////////////////////////////////////////////////////
	// Rule 1: disallow non competition scoped -> competition scoped
	if (rootIsCS) {
		if (!fromCS && toCS) return false;
		return true;
	}
	//////////////////////////////////////////////////////////////////////////////
	// Rule 2: disallow competition scoped -> competition scoped
	else {
		if (fromCS && toCS) return false;
		return true;
	}
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Calculates a score for a given path based on scope toggles, hop count, and lexical ordering.
 * Lower scores are better. Scoring criteria:
 * A) Number of competition-scoped/non-competition-scoped toggles (fewer is better)
 * B) Hop count (fewer is better)
 * C) Lexical ordering of path string for stable tiebreaking
 * @param {string} rootType - The root type to start scoring from
 * @param {Array<{from: string, field: string, to: string}>} path - The path to score
 * @returns {{toggles: number, hops: number, key: string}} Score object
 */
function scorePath(rootType, path) {
	let toggles = 0;
	let prevScope = isCS(rootType);
	for (const hop of path) {
		const curScope = isCS(hop.to);
		if (curScope !== prevScope) toggles++;
		prevScope = curScope;
	}
	const hops = path.length;
	const key = path.map((h) => `${h.from}.${h.field}->${h.to}`).join('|');
	return { toggles, hops, key };
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Compares two path scores to determine which is better.
 * @param {{toggles: number, hops: number, key: string}} a - First score to compare
 * @param {{toggles: number, hops: number, key: string}} b - Second score to compare
 * @returns {number} Negative if a is better, positive if b is better, 0 if equal
 */
function betterScore(a, b) {
	if (a.toggles !== b.toggles) return a.toggles - b.toggles;
	if (a.hops !== b.hops) return a.hops - b.hops;
	return a.key.localeCompare(b.key);
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Finds all valid paths from root type to target type using depth-first search with constraints.
 * Respects scope transition rules and prevents cycles.
 * @param {Object} EDGES_MAP - Map of type relationships
 * @param {string} rootType - The starting type
 * @param {string} targetType - The target type to reach
 * @param {Object} [options] - Configuration options
 * @param {number} [options.maxDepth=6] - Maximum path depth to explore
 * @returns {Array<Array<{from: string, field: string, to: string}>>} Array of valid paths
 */
function findConstrainedPaths(EDGES_MAP, rootType, targetType, { maxDepth = 6 } = {}) {
	const rootIsCS = isCS(rootType);
	const paths = [];
	const stack = [{ type: rootType, path: [], visited: new Set([rootType]) }];
	while (stack.length) {
		const { type, path, visited } = stack.pop();
		if (type === targetType) {
			paths.push(path);
			continue;
		}
		if (path.length >= maxDepth) continue;
		for (const { field, to } of listOutEdges(EDGES_MAP, type)) {
			if (visited.has(to)) continue; // no cycles
			if (!isAllowedTransition(type, to, rootIsCS)) continue;
			const nextPath = path.concat({ from: type, field, to });
			const nextVisited = new Set(visited);
			nextVisited.add(to);
			stack.push({ type: to, path: nextPath, visited: nextVisited });
		}
	}
	return paths;
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Selects the best path from root to target type based on scoring criteria.
 * @param {Object} EDGES_MAP - Map of type relationships
 * @param {string} rootType - The starting type
 * @param {string} targetType - The target type to reach
 * @param {Object} [opts] - Configuration options passed to findConstrainedPaths
 * @returns {Array<{from: string, field: string, to: string}>|null} Best path or null if none found
 */
function pickBestPath(EDGES_MAP, rootType, targetType, opts) {
	const cands = findConstrainedPaths(EDGES_MAP, rootType, targetType, opts);
	if (cands.length === 0) return null;
	let best = { path: cands[0], score: scorePath(rootType, cands[0]) };
	for (let i = 1; i < cands.length; i++) {
		const s = scorePath(rootType, cands[i]);
		if (betterScore(s, best.score) < 0) best = { path: cands[i], score: s };
	}
	return best.path;
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Auto-generates routes from a root type to multiple target types.
 * Finds optimal paths respecting scope constraints and generates route configurations.
 * @param {Object} params - Configuration object
 * @param {Object} params.EDGES_MAP - Map of type relationships
 * @param {string} params.rootType - The root type to start from
 * @param {Array<string>} params.targets - Array of target types to route to
 * @param {number} [params.maxDepth=6] - Maximum path depth to explore
 * @returns {Array<{key: string, to: string, via: Array<string>}>} Array of route configurations
 * @throws {ClientAggregationError} When no valid path exists to a target type
 */
function deriveRoutesFromTargets({ EDGES_MAP, rootType, targets, maxDepth = 6 }) {
	const routes = [];
	for (const to of targets) {
		////////////////////////////////////////////////////////////////////////////
		// Root can be materialised without a path; downstream uses $_rootIds for empty via
		if (to === rootType) {
			// No route needed for root; the builder uses $_rootIds and a dedicated root facet.
			continue;
		}
		////////////////////////////////////////////////////////////////////////////
		const path = pickBestPath(EDGES_MAP, rootType, to, { maxDepth });
		if (!path) throw new ClientAggregationError(`No valid scoped path from '${rootType}' to '${to}' under scope rules.`, 'UNREACHABLE_AUTO_ROUTE', { rootType, to });
		routes.push({ key: `${to}_${path.length}hops`, to, via: path.map((h) => `${h.from}.${h.field}->${h.to}`) });
	}
	return routes;
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { deriveRoutesFromTargets };
