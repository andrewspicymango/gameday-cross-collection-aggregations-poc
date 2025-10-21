////////////////////////////////////////////////////////////////////////////////
const _ = require('lodash');
const EDGES = require('./clientAggregationPipelineBuilderEdges');
const TYPE_ALIAS = require('./clientAggregationPipelineRouteTypeAlias');

////////////////////////////////////////////////////////////////////////////////
// Treat the user-provided edges as an unordered “allowed edge set”, then expand all
// contiguous paths its possible to reach from the rootType. Any edge whose from is
// never reached is naturally ignored.
//
// The code:
// - accepts an unordered list of edge IDs (e.g. from a single edgeSet query string),
// - validates each edge against the EDGES map,
// - builds all linear, cycle-free, contiguous multi-hop routes starting at rootType,
// - keeps only paths that end on one of the includeTypes (so we don’t explode the result space),
// - auto-generates keys (no explicit keys required),
// - returns the routes array the `buildMaterialisedListsPipelineTotalMax` already expects.

////////////////////////////////////////////////////////////////////////////////
function clientAggregationPipelineRouteBuilder({
	rootType,
	includeTypes, // ['team', 'venue', ...] ← only produce routes that end on these types
	edgeIds, // unordered array of "<from>.<to>", or "<from_alias>.<to_alias>"
	edges = null, // your graph map
	maxDepth = 12, // optional guardrail
	maxRoutes = 2000, // optional guardrail
}) {
	//////////////////////////////////////////////////////////////////////////////
	// Helpers
	function pathToRoute(path) {
		const to = path[path.length - 1].to;
		const via = path.map((h) => `${h.from}.${h.field}->${h.to}`);
		return { key: autoKey(to, via), to, via };
	}

	//////////////////////////////////////////////////////////////////////////////
	// Validation
	if (!rootType) throw new Error('rootType is required');
	if (!Array.isArray(includeTypes) || includeTypes.length === 0) throw new Error('includeTypes must be a non-empty array');
	if (!Array.isArray(edgeIds) || edgeIds.length === 0) throw new Error('edgeIds must be a non-empty array');

	/////////////////////////////////////////////////////////////////////////////
	// Directed, field-labelled graph
	const EDGES_FOR_PIPELINE = edges || EDGES;

	//////////////////////////////////////////////////////////////////////////////
	// 1) Normalise and validate input edges against EDGES (silently drop invalids)
	const processedEdgeIds = buildEdgeIds(edgeIds); // e.g. ['competition.stages->stage', ...]
	const allowed = buildAllowedEdgeMap(processedEdgeIds, EDGES_FOR_PIPELINE); // { from: { field: to } }

	//////////////////////////////////////////////////////////////////////////////
	// If the root itself has no outgoing allowed edges, there may still be direct materialisation requests,
	// but we can only form routes with at least one hop; otherwise return empty routes.
	if (!allowed[rootType]) return [];

	//////////////////////////////////////////////////////////////////////////////
	// 2) DFS from root across *allowed* edges only (not full EDGES)
	const routes = [];
	const stack = [{ node: rootType, path: [] }];
	let produced = 0;
	while (stack.length) {
		const { node, path } = stack.pop();
		////////////////////////////////////////////////////////////////////////////
		// If this node has no onward edges in the *allowed* set, consider closing the path here.
		const outs = allowed[node] || {};
		const fields = Object.keys(outs);
		const isLeaf = fields.length === 0;
		if (isLeaf && path.length > 0) {
			const endType = path[path.length - 1].to;
			if (includeTypes.includes(endType)) {
				routes.push(pathToRoute(path));
				produced++;
				if (produced >= maxRoutes) break;
			}
			continue;
		}
		////////////////////////////////////////////////////////////////////////////
		// Otherwise, branch to each allowed outgoing edge
		for (const field of fields) {
			const to = outs[field];
			const hop = { from: node, field, to };
			//////////////////////////////////////////////////////////////////////////
			// Cycle prevention: do not revisit a node
			if (path.some((h) => h.from === to || h.to === to) || node === to) {
				continue;
			}
			const newPath = path.concat(hop);
			if (newPath.length > maxDepth) continue; // guardrail

			//////////////////////////////////////////////////////////////////////////
			// If this hop lands on an includeType, we can emit this path as a route now,
			// *and* still continue exploring deeper if further edges exist (multi-depth options).
			if (includeTypes.includes(to)) {
				routes.push(pathToRoute(newPath));
				produced++;
				if (produced >= maxRoutes) break;
			}
			//////////////////////////////////////////////////////////////////////////
			// Continue DFS if there are further allowed outs from 'to'
			if (allowed[to] && produced < maxRoutes) {
				stack.push({ node: to, path: newPath });
			}
		}
	}

	//////////////////////////////////////////////////////////////////////////////
	// Deduplicate identical routes (same via and same 'to')
	const seen = new Set();
	const deduped = [];
	for (const r of routes) {
		const sig = `${r.to}|${r.via.join('~')}`;
		if (!seen.has(sig)) {
			seen.add(sig);
			deduped.push(r);
		}
	}
	//////////////////////////////////////////////////////////////////////////////
	return deduped;
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Checks if a value matches a type or its alias.
 *
 * This function validates that both parameters are strings, ensures the type
 * exists in the TYPE_ALIAS mapping, and then checks if the value matches
 * either the type directly or its corresponding alias.
 *
 * @param {string} type - The type name to check against
 * @param {string} val - The value to validate against the type or alias
 * @returns {boolean} True if val matches type or its alias, false otherwise
 *
 * @example
 * // Returns true if val equals type or TYPE_ALIAS[type]
 * typeOrAlias('string', 'str') // true if TYPE_ALIAS.string === 'str'
 * typeOrAlias('number', 'number') // true (exact match)
 */
function typeOrAlias(type, val) {
	if (!_.isString(type)) return false;
	if (!_.isString(val)) return false;
	if (!_.has(TYPE_ALIAS, type)) return false;
	if (type.toLowerCase() === val.toLowerCase()) return true;
	if (TYPE_ALIAS[type].toLowerCase() === val.toLowerCase()) return true;
	return false;
}

////////////////////////////////////////////////////////////////////////////////
// Edge IDs in the form from.to
// Return list needs to be an array of strings in the form "<from>.<field>-><to>"
function buildEdgeIds(rawEdgeIds) {
	const edgeIds = [];
	for (const raw of rawEdgeIds) {
		if (!_.isString(raw)) continue;
		const pair = raw.trim().split('.');
		if (pair.length !== 2) continue;
		const from = pair[0].trim().toLowerCase();
		const to = pair[1].trim().toLowerCase();
		if (!from || !to) continue;
		////////////////////////////////////////////////////////////////////////////
		if (typeOrAlias('competition', from)) {
			if (typeOrAlias('stage', to)) edgeIds.push(`competition.stages->stage`);
			else if (typeOrAlias('sgo', to)) edgeIds.push(`competition.sgos->sgo`);
		}
		////////////////////////////////////////////////////////////////////////////
		else if (typeOrAlias('stage', from)) {
			if (typeOrAlias('event', to)) edgeIds.push(`stage.events->event`);
			else if (typeOrAlias('competition', to)) edgeIds.push(`stage.competitions->competition`);
			else if (typeOrAlias('ranking', to)) edgeIds.push(`stage.rankings->ranking`);
		}
		////////////////////////////////////////////////////////////////////////////
		else if (typeOrAlias('event', from)) {
			if (typeOrAlias('team', to)) edgeIds.push(`event.teams->team`);
			else if (typeOrAlias('venue', to)) edgeIds.push(`event.venues->venue`);
			else if (typeOrAlias('sportsPerson', to)) edgeIds.push(`event.sportsPersons->sportsPerson`);
			else if (typeOrAlias('stage', to)) edgeIds.push(`event.stages->stage`);
			else if (typeOrAlias('ranking', to)) edgeIds.push(`event.rankings->ranking`);
			else if (typeOrAlias('keyMoment', to)) edgeIds.push(`event.keyMoments->keyMoment`);
		}
		////////////////////////////////////////////////////////////////////////////
		else if (typeOrAlias('team', from)) {
			if (typeOrAlias('club', to)) edgeIds.push(`team.clubs->club`);
			else if (typeOrAlias('event', to)) edgeIds.push(`team.events->event`);
			else if (typeOrAlias('nation', to)) edgeIds.push(`team.nations->nation`);
			else if (typeOrAlias('sportsPerson', to)) edgeIds.push(`team.sportsPersons->sportsPerson`);
			else if (typeOrAlias('staff', to)) edgeIds.push(`team.staff->staff`);
			else if (typeOrAlias('ranking', to)) edgeIds.push(`team.rankings->ranking`);
			else if (typeOrAlias('sgo', to)) edgeIds.push(`team.sgos->sgo`);
			else if (typeOrAlias('keyMoment', to)) edgeIds.push(`team.keyMoments->keyMoment`);
			else if (typeOrAlias('venue', to)) edgeIds.push(`team.venues->venue`);
		}
		////////////////////////////////////////////////////////////////////////////
		else if (typeOrAlias('club', from)) {
			if (typeOrAlias('team', to)) edgeIds.push(`club.teams->team`);
			else if (typeOrAlias('sgo', to)) edgeIds.push(`club.sgos->sgo`);
			else if (typeOrAlias('venue', to)) edgeIds.push(`club.venues->venue`);
			else if (typeOrAlias('sportsPerson', to)) edgeIds.push(`club.sportsPersons->sportsPerson`);
			else if (typeOrAlias('staff', to)) edgeIds.push(`club.staff->staff`);
		}
		///////////////////////////////////////////////////////////////////////////
		else if (typeOrAlias('nation', from)) {
			if (typeOrAlias('team', to)) edgeIds.push(`nation.teams->team`);
			else if (typeOrAlias('sgo', to)) edgeIds.push(`nation.sgos->sgo`);
			else if (typeOrAlias('venue', to)) edgeIds.push(`nation.venues->venue`);
			else if (typeOrAlias('staff', to)) edgeIds.push(`nation.staff->staff`);
		}
		////////////////////////////////////////////////////////////////////////////
		else if (typeOrAlias('venue', from)) {
			if (typeOrAlias('event', to)) edgeIds.push(`venue.events->event`);
			else if (typeOrAlias('team', to)) edgeIds.push(`venue.teams->team`);
			else if (typeOrAlias('sgo', to)) edgeIds.push(`venue.sgos->sgo`);
			else if (typeOrAlias('club', to)) edgeIds.push(`venue.clubs->club`);
			else if (typeOrAlias('nation', to)) edgeIds.push(`venue.nations->nation`);
		}
		////////////////////////////////////////////////////////////////////////////
		else if (typeOrAlias('sgo', from)) {
			if (typeOrAlias('competition', to)) edgeIds.push(`sgo.competitions->competition`);
			else if (typeOrAlias('sgo', to)) edgeIds.push(`sgo.sgos->sgo`);
			else if (typeOrAlias('venue', to)) edgeIds.push(`sgo.venues->venue`);
			else if (typeOrAlias('club', to)) edgeIds.push(`sgo.clubs->club`);
			else if (typeOrAlias('nation', to)) edgeIds.push(`sgo.nations->nation`);
			else if (typeOrAlias('team', to)) edgeIds.push(`sgo.teams->team`);
		}
		////////////////////////////////////////////////////////////////////////////
		else if (typeOrAlias('staff', from)) {
			if (typeOrAlias('team', to)) edgeIds.push(`staff.teams->team`);
			else if (typeOrAlias('club', to)) edgeIds.push(`staff.clubs->club`);
			else if (typeOrAlias('nation', to)) edgeIds.push(`staff.nations->nation`);
			else if (typeOrAlias('sportsPerson', to)) edgeIds.push(`staff.sportsPersons->sportsPerson`);
		}
		////////////////////////////////////////////////////////////////////////////
		else if (typeOrAlias('sportsPerson', from)) {
			if (typeOrAlias('staff', to)) edgeIds.push(`sportsPerson.staff->staff`);
			else if (typeOrAlias('team', to)) edgeIds.push(`sportsPerson.teams->team`);
			else if (typeOrAlias('club', to)) edgeIds.push(`sportsPerson.clubs->club`);
			else if (typeOrAlias('ranking', to)) edgeIds.push(`sportsPerson.rankings->ranking`);
			else if (typeOrAlias('keyMoment', to)) edgeIds.push(`sportsPerson.keyMoments->keyMoment`);
			else if (typeOrAlias('event', to)) edgeIds.push(`sportsPerson.events->event`);
		}
		////////////////////////////////////////////////////////////////////////////
		else if (typeOrAlias('ranking', from)) {
			if (typeOrAlias('event', to)) edgeIds.push(`ranking.events->event`);
			else if (typeOrAlias('stage', to)) edgeIds.push(`ranking.stages->stage`);
			else if (typeOrAlias('team', to)) edgeIds.push(`ranking.teams->team`);
			else if (typeOrAlias('sportsPerson', to)) edgeIds.push(`ranking.sportsPersons->sportsPerson`);
		}
		////////////////////////////////////////////////////////////////////////////
		else if (typeOrAlias('keyMoment', from)) {
			if (typeOrAlias('team', to)) edgeIds.push(`keyMoment.teams->team`);
			else if (typeOrAlias('sportsPerson', to)) edgeIds.push(`keyMoment.sportsPersons->sportsPerson`);
			else if (typeOrAlias('event', to)) edgeIds.push(`keyMoment.events->event`);
		}
	}
	return edgeIds;
}

////////////////////////////////////////////////////////////////////////////////
function buildAllowedEdgeMap(edgeIds, EDGES) {
	const allowed = {}; // from -> field -> to
	for (const raw of edgeIds) {
		const edge = String(raw).replace(/→/g, '->').trim();
		const [fromAndField, to] = edge.split('->');
		if (!fromAndField || !to) continue;
		const dot = fromAndField.indexOf('.');
		if (dot <= 0) continue;
		const from = fromAndField.slice(0, dot);
		const field = fromAndField.slice(dot + 1);
		const outs = EDGES[from];
		if (!outs) continue;
		const declaredTo = outs[field];
		if (!declaredTo || declaredTo !== to) continue; // silently ignore invalid/mismatched edges
		if (!allowed[from]) allowed[from] = {};
		allowed[from][field] = to;
	}
	return allowed;
}

////////////////////////////////////////////////////////////////////////////////
function autoKey(to, via) {
	const base = `${to}_${shortHash(via.join('~'))}`;
	return base;
}

////////////////////////////////////////////////////////////////////////////////
function shortHash(s) {
	let h = 2166136261 >>> 0; // FNV-1a
	for (let i = 0; i < s.length; i++) {
		h ^= s.charCodeAt(i);
		h = Math.imul(h, 16777619);
	}
	return (h >>> 0).toString(36).slice(0, 6);
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { clientAggregationPipelineRouteBuilder };
