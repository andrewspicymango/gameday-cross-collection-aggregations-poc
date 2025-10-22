const _ = require('lodash');
const { ClientAggregationError, ServerAggregationError } = require('./clientAggregationError.js');

////////////////////////////////////////////////////////////////////////////////
// Constants & config
////////////////////////////////////////////////////////////////////////////////
const idField = 'gamedayId';
const aggregationCollectionName = 'materialisedAggregations';
const EDGES = require('./clientAggregationPipelineBuilderEdges.js');
const COLLECTIONS = require('./clientAggregationPipelineBuilderCollections.js');
const { deriveRoutesFromTargets } = require('./clientAggregationDeriveRoutes.js');

////////////////////////////////////////////////////////////////////////////////
/**
 * Builds a MongoDB aggregation pipeline for cross-collection resource materialization.
 *
 * Constructs a complex pipeline that traverses a directed graph of resources starting from a root
 * resource, following specified routes to materialize related resources with budget constraints.
 * The pipeline handles graph traversal, deduplication, budget allocation, and final materialization
 * via $facet operations with optional field projections.
 *
 * @param {Object} config - Configuration object
 * @param {string} config.rootType - The resource type to start traversal from
 * @param {string} config.rootExternalKey - External key identifying the root resource
 * @param {number} config.maxNumberOfMaterialisedResources - Maximum total resources to materialize
 * @param {Array<Object>} [config.routes] - Route definitions with key, to, and via properties. Optional and auto created if absent.
 * @param {Array<string>} config.resourceTypesToMaterialise - Types to include in results (controls budget order)
 * @param {Object} [config.fieldProjections] - Field inclusion/exclusion mapping by resource type
 * @param {Object} [config.edges] - Graph edges defining resource relationships
 * @param {Object} [config.collectionMap] - Mapping of resource types to MongoDB collections
 * @returns {Array<Object>} MongoDB aggregation pipeline stages
 * @throws {ClientAggregationError} When validation fails or resources are unreachable
 */
function clientAggregationPipelineBuilder({
	rootType,
	rootExternalKey,
	maxNumberOfMaterialisedResources,
	routes, // [{ key, to, via: ["from.field->to", ...] }, ...]  REQUIRED
	resourceTypesToMaterialise, // ["team", "venue", ...] REQUIRED (controls materialisation + budget order)
	fieldProjections,
	edges,
	collectionMap,
}) {
	//////////////////////////////////////////////////////////////////////////////
	// Directed, field-labelled graph
	const EDGES_FOR_PIPELINE = edges || EDGES;

	//////////////////////////////////////////////////////////////////////////////
	// resourceType -> real collection (for final materialisation)
	const COLLECTIONS_FOR_PIPELINE = collectionMap || COLLECTIONS;

	let effectiveRoutes = routes;
	if (!effectiveRoutes || effectiveRoutes.length === 0) {
		effectiveRoutes = deriveRoutesFromTargets({
			EDGES_MAP: EDGES_FOR_PIPELINE,
			rootType,
			targets: resourceTypesToMaterialise,
			maxDepth: 8, // optional
		});
	}

	//////////////////////////////////////////////////////////////////////////////
	// Validation
	if (!rootType || !rootExternalKey) {
		throw new Error('rootType and rootExternalKey are required');
	}
	if (!Array.isArray(effectiveRoutes) || effectiveRoutes.length === 0) {
		throw new ClientAggregationError('routes must be a non-empty array of route definitions.');
	}
	if (!Array.isArray(resourceTypesToMaterialise) || resourceTypesToMaterialise.length === 0) {
		throw new ClientAggregationError('includeTypes must be a non-empty array of types to materialise.');
	}
	if (!Number.isInteger(maxNumberOfMaterialisedResources) || maxNumberOfMaterialisedResources < 0) {
		throw new ClientAggregationError('totalMax must be a non-negative integer');
	}
	//////////////////////////////////////////////////////////////////////////////
	// Validate fieldProjections if provided
	if (fieldProjections && typeof fieldProjections !== 'object') {
		throw new ClientAggregationError('fieldProjections must be an object mapping resource types to inclusions and exclusions');
	}

	//////////////////////////////////////////////////////////////////////////////
	// Normalise includeTypes order & uniqueness (budget is applied in this order)
	resourceTypesToMaterialise = [...new Set(resourceTypesToMaterialise)];
	const rootIsRequested = resourceTypesToMaterialise.includes(rootType);

	//////////////////////////////////////////////////////////////////////////////
	// Validate that we can materialise root + requested include types
	const allToMaterialise = new Set([rootType, ...resourceTypesToMaterialise]);
	for (const t of allToMaterialise) {
		if (!COLLECTIONS_FOR_PIPELINE[t]) {
			const availableMaterialisedTypes = [];
			for (const coll in COLLECTIONS_FOR_PIPELINE) availableMaterialisedTypes.push(coll);
			throw new ClientAggregationError(`No collection mapping for resourceType='${t}'. Available collections are: ${availableMaterialisedTypes.join(', ')}`);
		}
	}

	//////////////////////////////////////////////////////////////////////////////
	// Parse routes strictly (throws on any non-contiguous / invalid hop, cycles, or duplicate edges)
	const parsedRoutes = effectiveRoutes.map((r) => ({
		key: r.key,
		to: r.to,
		path: parseExplicitRouteStrictNoCyclesNoDup({ EDGES: EDGES_FOR_PIPELINE, rootType, route: r }),
	}));

	//////////////////////////////////////////////////////////////////////////////
	// ---- Reachability validation ----
	// What the caller wants to materialise (including root if requested)
	const requested = new Set(resourceTypesToMaterialise);
	if (resourceTypesToMaterialise.includes(rootType)) requested.add(rootType);
	//////////////////////////////////////////////////////////////////////////////
	// What the provided routes actually reach (by their final `to` types)
	const reachableByRoutes = new Set(parsedRoutes.map((r) => r.to));
	//if (resourceTypesToMaterialise.includes(rootType)) reachableByRoutes.add(rootType);
	// Root is always reachable by definition — it doesn't require a route.
	reachableByRoutes.add(rootType);
	//////////////////////////////////////////////////////////////////////////////
	// What the graph would allow in principle
	const reachableByGraph = computeReachableTypesFromEdges(EDGES_FOR_PIPELINE, rootType);
	const unreachableByGraph = [...requested].filter((t) => !reachableByGraph.has(t));
	if (unreachableByGraph.length) {
		throw new ClientAggregationError(
			`Requested materialised type(s) not reachable from root '${rootType}' via supplied EDGES: ${unreachableByGraph.join(', ')}`,
			'UNREACHABLE_BY_GRAPH',
			{ rootType, unreachableByGraph }
		);
	}
	//////////////////////////////////////////////////////////////////////////////
	// What the routes actually traverse in the graph
	const unreachableByRoutes = [...requested].filter((t) => !reachableByRoutes.has(t));
	if (unreachableByRoutes.length) {
		throw new ClientAggregationError(
			`Requested materialised type(s) are reachable in the graph but not traversed by any provided route: ${unreachableByRoutes.join(', ')}`,
			'UNREACHABLE_BY_ROUTES',
			{ rootType, unreachableByRoutes, calculatedRoutes: effectiveRoutes }
		);
	}

	//////////////////////////////////////////////////////////////////////////////
	// Shared-hop planning (dedupe common traversals across routes)
	const steps = planSteps(parsedRoutes.map((r) => r.path));

	//////////////////////////////////////////////////////////////////////////////
	// Build the aggregation pipeline
	const stages = [{ $match: { resourceType: rootType, externalKey: rootExternalKey } }, { $addFields: { _rootKey: '$externalKey', _rootIds: [`$${idField}`] } }];

	//////////////////////////////////////////////////////////////////////////////
	// Traverse materialisedAggregations using planned steps
	for (const step of steps) {
		const { from, field, dependsOnKey, outputName, depth } = step;

		////////////////////////////////////////////////////////////////////////////
		// First hop reads from the root doc field
		if (depth === 0) {
			stages.push({ $addFields: { [outputName]: { $ifNull: [`$${field}`, []] } } });
		}
		////////////////////////////////////////////////////////////////////////////
		// Subsequent hops read from previous step's output
		else {
			const prev = steps.find((s) => s.key === dependsOnKey);
			if (!prev) throw new Error(`Internal: missing dependency for step ${step.key}`);
			const prevOutput = prev.outputName;
			const lkField = `${outputName}__lk`;
			stages.push({
				$lookup: {
					from: aggregationCollectionName,
					let: { ids: `$${prevOutput}` },
					pipeline: [
						{ $match: { resourceType: from } },
						{ $match: { $expr: { $in: [`$${idField}`, '$$ids'] } } },
						{ $project: { nextIds: { $ifNull: [`$${field}`, []] } } },
						{ $unwind: { path: '$nextIds', preserveNullAndEmptyArrays: false } },
						{ $group: { _id: null, ids: { $addToSet: '$nextIds' } } },
					],
					as: lkField,
				},
			});
			stages.push({
				$addFields: {
					[outputName]: { $ifNull: [{ $arrayElemAt: [`$${lkField}.ids`, 0] }, []] },
				},
			});
		}
	}
	//////////////////////////////////////////////////////////////////////////////
	// For each route, compute its final array of IDs (from the last hop output)
	for (const r of parsedRoutes) {
		const idsVar = `_route_${safeVar(r.key)}_ids`;
		const sourceFieldExpr =
			r.path.length === 0
				? '$_rootIds'
				: (() => {
						const lastHop = r.path[r.path.length - 1];
						const lastKey = makeKey(lastHop.from, lastHop.field, lastHop.to);
						const lastStep = steps.find((s) => s.key === lastKey);
						if (!lastStep) throw new Error(`Internal: cannot find step for route '${r.key}'`);
						return `$${lastStep.outputName}`;
				  })();
		stages.push({ $addFields: { [idsVar]: sourceFieldExpr } });
	}

	//////////////////////////////////////////////////////////////////////////////
	// Union per included type (normalised sets across all routes with the same .to)
	for (const t of resourceTypesToMaterialise) {
		const contributingVars = parsedRoutes.filter((r) => r.to === t).map((r) => `$_route_${safeVar(r.key)}_ids`);
		const unionVar = `_union_${t}_ids`;
		if (contributingVars.length === 0) {
			stages.push({ $addFields: { [unionVar]: [] } });
		} else if (contributingVars.length === 1) {
			stages.push({ $addFields: { [unionVar]: contributingVars[0] } });
		} else {
			const unionExpr = contributingVars.reduce((acc, cur) => {
				if (acc === null) return cur;
				return { $setUnion: [acc, cur] };
			}, null);
			stages.push({ $addFields: { [unionVar]: unionExpr } });
		}
	}

	//////////////////////////////////////////////////////////////////////////////
	// Budget: root first, then includeTypes in given order
	stages.push({ $addFields: { _remaining: maxNumberOfMaterialisedResources } });
	//////////////////////////////////////////////////////////////////////////////
	stages.push({
		$addFields: {
			_rootIncludedIds: { $cond: [{ $and: [{ $gt: ['$_remaining', 0] }, { $literal: rootIsRequested }] }, { $slice: ['$_rootIds', 1] }, []] },
			_rootOverflowIds: { $cond: [{ $and: [{ $gt: ['$_remaining', 0] }, { $literal: rootIsRequested }] }, { $slice: ['$_rootIds', 0] }, '$_rootIds'] },
			_remaining: { $cond: [{ $and: [{ $gt: ['$_remaining', 0] }, { $literal: rootIsRequested }] }, { $subtract: ['$_remaining', 1] }, '$_remaining'] },
		},
	});

	//////////////////////////////////////////////////////////////////////////////
	// For each included type: slice from its union set, track overflow, decrement remaining
	for (const t of resourceTypesToMaterialise) {
		const idsVar = `_union_${t}_ids`;
		const includedVar = `_inc_${t}_ids`;
		const overflowVar = `_ovf_${t}_ids`;
		stages.push({
			$addFields: {
				[includedVar]: {
					$let: {
						vars: { sz: { $size: `$${idsVar}` } },
						in: { $cond: [{ $gt: ['$_remaining', 0] }, { $slice: [`$${idsVar}`, { $min: ['$_remaining', '$$sz'] }] }, []] },
					},
				},
				[overflowVar]: {
					$cond: [
						{ $gt: ['$_remaining', 0] },
						{
							$let: {
								vars: { take: { $min: ['$_remaining', { $size: `$${idsVar}` }] }, arraySize: { $size: `$${idsVar}` } },
								in: { $cond: [{ $eq: ['$$take', '$$arraySize'] }, [], { $slice: [`$${idsVar}`, '$$take', { $subtract: ['$$arraySize', '$$take'] }] }] },
							},
						},
						`$${idsVar}`,
					],
				},
				_remaining: {
					$let: {
						vars: { take: { $min: ['$_remaining', { $size: `$${idsVar}` }] } },
						in: { $subtract: ['$_remaining', '$$take'] },
					},
				},
			},
		});
	}

	//////////////////////////////////////////////////////////////////////////////
	// Materialise via $facet: root + one facet per included type
	const sortStageFor = (type) => {
		if (type === 'competition') {
			return [{ $sort: { start: -1, _id: 1 } }];
		} else if (type === 'event' || type === 'keyMoment') {
			return [{ $sort: { dateTime: -1, _id: 1 } }];
		} else if (type === 'team' || type === 'venue' || type === 'club' || type === 'nation' || type === 'sgo') {
			return [{ $sort: { name: -1, _id: 1 } }];
		} else if (type === 'sportsPerson' || type === 'staff') {
			return [{ $sort: { lastName: -1, _id: 1 } }];
		} else if (type === 'ranking') {
			return [{ $sort: { _externalStageId: -1, _externalEventId: -1, ranking: -1, _id: 1 } }];
		} else {
			return [{ $sort: { _id: 1 } }];
		}
	};
	const facet = {};
	//////////////////////////////////////////////////////////////////////////////
	const projectionStages = buildProjectionPhases(rootType, fieldProjections);
	const rootLookupPipeline = [{ $match: { $expr: { $in: ['$_id', '$$ids'] } } }, ...sortStageFor(rootType), ...projectionStages];
	facet[rootType] = [
		{ $project: { includedIds: '$_rootIncludedIds', overflowIds: '$_rootOverflowIds' } },
		{ $lookup: { from: COLLECTIONS_FOR_PIPELINE[rootType], let: { ids: '$includedIds' }, pipeline: rootLookupPipeline, as: 'docs' } },
		{ $replaceWith: { items: '$docs', overflow: { resourceType: rootType, overflowIds: '$overflowIds' } } },
	];

	//////////////////////////////////////////////////////////////////////////////
	// Included types facets
	for (const t of resourceTypesToMaterialise) {
		if (t === rootType) continue;
		const includedVar = `_inc_${t}_ids`;
		const overflowVar = `_ovf_${t}_ids`;
		const typeProjectionStages = buildProjectionPhases(t, fieldProjections);
		const typeLookupPipeline = [{ $match: { $expr: { $in: ['$_id', '$$ids'] } } }, ...sortStageFor(t), ...typeProjectionStages];
		facet[t] = [
			{ $project: { includedIds: `$${includedVar}`, overflowIds: `$${overflowVar}` } },
			{ $lookup: { from: COLLECTIONS_FOR_PIPELINE[t], let: { ids: '$includedIds' }, pipeline: typeLookupPipeline, as: 'docs' } },
			{ $replaceWith: { items: '$docs', overflow: { resourceType: t, overflowIds: '$overflowIds' } } },
		];
	}
	//////////////////////////////////////////////////////////////////////////////
	stages.push({ $facet: facet });
	//////////////////////////////////////////////////////////////////////////////
	// Final shape
	const resultsProjection = {};
	for (const t of resourceTypesToMaterialise) {
		resultsProjection[t] = {
			$ifNull: [{ $arrayElemAt: [`$${t}`, 0] }, { items: [], overflow: { resourceType: t, overflowIds: [] } }],
		};
	}
	stages.push({
		$project: {
			root: { type: { $literal: rootType }, externalKey: '$_rootKey' },
			results: resultsProjection,
		},
	});
	//////////////////////////////////////////////////////////////////////////////
	return stages;
}

////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////
/**
 * Computes all reachable node types from a given root node by traversing edges in a graph.
 * Uses breadth-first search to find all nodes that can be reached from the starting node.
 *
 * @param {Object} EDGES_MAP - A map where keys are node types and values are objects
 *                            containing outgoing edges to other node types
 * @param {string} root - The starting node type to begin traversal from
 * @returns {Set<string>} A Set containing all node types reachable from the root,
 *                        including the root itself
 *
 * @example
 * const edges = {
 *   'A': { edge1: 'B', edge2: 'C' },
 *   'B': { edge3: 'D' },
 *   'C': { edge4: 'D' }
 * };
 * const reachable = computeReachableTypesFromEdges(edges, 'A');
 * // Returns Set(['A', 'B', 'C', 'D'])
 */
function computeReachableTypesFromEdges(EDGES_MAP, root) {
	const seen = new Set([root]);
	const q = [root];
	while (q.length) {
		const cur = q.shift();
		const outs = EDGES_MAP[cur];
		if (!outs) continue;
		for (const to of Object.values(outs)) {
			if (!seen.has(to)) {
				seen.add(to);
				q.push(to);
			}
		}
	}
	return seen;
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Parses and validates an explicit route definition, ensuring strict validation rules.
 *
 * @param {Object} params - The parameters object
 * @param {Object} params.EDGES - Graph edge definitions mapping types to their field->target mappings
 * @param {string} params.rootType - The starting node type for the route
 * @param {Object} params.route - Route definition object
 * @param {string} params.route.key - Unique identifier for the route
 * @param {string} params.route.to - Expected final destination node type
 * @param {string[]} params.route.via - Array of edge identifiers in format '<from>.<field>-><to>'
 *
 * @returns {Array<Object>} Array of hop objects with {from, field, to} properties representing the validated path
 *
 * @throws {Error} If route is invalid, non-contiguous, contains cycles, duplicate edges,
 *                 or references non-existent edges in the graph
 *
 * @description Validates that the route forms a linear path with no cycles, no duplicate edges,
 *              contiguous hops, and all edges exist in the provided EDGES graph definition.
 *              Prevents branching, backtracking, and ensures the final node matches route.to.
 *              - First hop's `from` must equal rootType.
 *              - Each next hop's `from` must equal the previous hop's `to` (contiguous).
 *              - Edge must exist in EDGES[from] and match declared `to`.
 *              - No cycles: may not revisit any previously reached node within the same route.
 *              - No duplicate edges: the same '<from>.<field>-><to>' may not appear twice in a route.
 *              - Final node must equal route.to.
 *  Throws detailed errors describing exactly which hop failed and why.
 */
function parseExplicitRouteStrictNoCyclesNoDup({ EDGES, rootType, route }) {
	//////////////////////////////////////////////////////////////////////////////
	// Validation
	if (!route || !route.key || !route.to || !Array.isArray(route.via) || route.via.length === 0) {
		throw new Error(`Invalid route: expected { key, to, via[] }. Got: ${JSON.stringify(route)}`);
	}
	//////////////////////////////////////////////////////////////////////////////
	const path = [];
	let expectedFrom = rootType;

	//////////////////////////////////////////////////////////////////////////////
	// Track visited nodes to prevent cycles (includes root)
	const visitedNodes = new Set([rootType]);

	//////////////////////////////////////////////////////////////////////////////
	// Track visited edges to prevent duplicates
	const visitedEdges = new Set();

	//////////////////////////////////////////////////////////////////////////////
	route.via.forEach((edgeKey, idx) => {
		const label = `route '${route.key}', hop ${idx + 1}`;
		const [fromAndField, to] = String(edgeKey).split('->');
		if (!fromAndField || !to) throw new Error(`${label}: bad edge identifier '${edgeKey}' (expected '<from>.<field>-><to>')`);
		const dot = fromAndField.indexOf('.');
		if (dot <= 0) throw new Error(`${label}: bad edge identifier '${edgeKey}' (missing '.')`);
		const from = fromAndField.slice(0, dot);
		const field = fromAndField.slice(dot + 1);
		////////////////////////////////////////////////////////////////////////////
		// Contiguity check
		if (from !== expectedFrom) {
			throw new Error(`${label}: non-contiguous hop. Expected 'from'='${expectedFrom}' but got '${from}'. ` + `Routes must be linear (no branching/backtracking).`);
		}
		////////////////////////////////////////////////////////////////////////////
		// Graph existence check
		const outs = EDGES[from];
		if (!outs) throw new Error(`${label}: no edges declared for type '${from}' in EDGES.`);
		const declaredTo = outs[field];
		if (!declaredTo) throw new Error(`${label}: field '${field}' not declared on EDGES['${from}'].`);
		if (declaredTo !== to) {
			throw new Error(`${label}: edge targets '${declaredTo}', but route specified '${to}'.`);
		}
		////////////////////////////////////////////////////////////////////////////
		// Duplicate-edge detection
		if (visitedEdges.has(edgeKey)) {
			throw new Error(`${label}: duplicate edge '${edgeKey}' detected within the same route.`);
		}
		////////////////////////////////////////////////////////////////////////////
		// Cycle detection: cannot revisit a node we've already reached on this route
		if (visitedNodes.has(to)) {
			const trail = [rootType, ...path.map((h) => h.to)].join(' -> ');
			throw new Error(`${label}: cycle detected when moving '${from}' -> '${to}'. ` + `Node '${to}' was already visited in this route (${trail}).`);
		}
		////////////////////////////////////////////////////////////////////////////
		// Append hop
		path.push({ from, field, to });
		visitedNodes.add(to);
		visitedEdges.add(edgeKey);
		expectedFrom = to;
	});

	//////////////////////////////////////////////////////////////////////////////
	// Final node check
	if (expectedFrom !== route.to) {
		throw new Error(`route '${route.key}': final node '${expectedFrom}' does not match declared 'to'='${route.to}'.`);
	}

	return path;
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Plans aggregation pipeline steps from an array of relationship paths.
 * Processes each hop in the paths to create unique steps with dependency tracking.
 *
 * @param {Array<Array<Object>>} paths - Array of paths, where each path is an array of hop objects
 * @param {string} paths[].from - Source collection name
 * @param {string} paths[].field - Field name for the relationship
 * @param {string} paths[].to - Target collection name
 * @returns {Array<Object>} Array of step objects sorted by depth then by key
 * @returns {string} returns[].key - Unique identifier for the step
 * @returns {string} returns[].from - Source collection name
 * @returns {string} returns[].field - Field name for the relationship
 * @returns {string} returns[].to - Target collection name
 * @returns {number} returns[].depth - Step depth in the path (0-indexed)
 * @returns {string|null} returns[].dependsOnKey - Key of the previous step or null for first steps
 * @returns {string} returns[].outputName - Generated output field name for the step
 */
function planSteps(paths) {
	const seen = new Map();
	const steps = [];

	for (const path of paths) {
		for (let i = 0; i < path.length; i++) {
			const hop = path[i];
			const key = makeKey(hop.from, hop.field, hop.to);
			const dependsOnKey = i > 0 ? makeKey(path[i - 1].from, path[i - 1].field, path[i - 1].to) : null;
			if (!seen.has(key)) {
				const outputName = `${hop.to}Ids__${hashKey(key)}__d${i}`;
				const step = { key, from: hop.from, field: hop.field, to: hop.to, depth: i, dependsOnKey, outputName };
				seen.set(key, step);
				steps.push(step);
			}
		}
	}
	steps.sort((a, b) => a.depth - b.depth || a.key.localeCompare(b.key));
	return steps;
}

////////////////////////////////////////////////////////////////////////////////
// Helper: turn ["a","b*","c*","d"] into match expression (no regex)
// - exact: "$$t.name" === "a" or "d"
// - startsWith: indexOfCP("b") === 0 or indexOfCP("c") === 0
/**
 * Builds a MongoDB aggregation expression to match names against exact values or prefix patterns.
 *
 * @param {string[]} namesOrPrefixes - Array of names or prefixes to match against.
 *                                   Names ending with '*' are treated as prefix matches.
 * @param {string} inputExpr - MongoDB expression representing the field to match
 *                           (e.g. "$$t.name", "$fieldName")
 * @returns {Object|null} MongoDB aggregation match expression using $eq, $in, $or operators,
 *                       or null if no valid names provided
 *
 * @example
 * // Exact match: { $eq: ["$$t.name", "John"] }
 * buildNameMatchExpr(["John"], "$$t.name")
 *
 * @example
 * // Prefix match: { $eq: [{ $indexOfCP: ["$$t.name", "Jo"] }, 0] }
 * buildNameMatchExpr(["Jo*"], "$$t.name")
 *
 * @example
 * // Multiple conditions: { $or: [{ $in: ["$$t.name", ["John", "Jane"]] }, { $eq: [{ $indexOfCP: ["$$t.name", "Jo"] }, 0] }] }
 * buildNameMatchExpr(["John", "Jane", "Jo*"], "$$t.name")
 */
function buildNameMatchExpr(namesOrPrefixes, inputExpr /* e.g. "$$t.name" */) {
	if (!Array.isArray(namesOrPrefixes) || namesOrPrefixes.length === 0) return null;
	const exact = [];
	const starts = [];
	for (const n of namesOrPrefixes) {
		if (n.endsWith('*')) starts.push(n.slice(0, -1));
		else exact.push(n);
	}
	const orConditions = [];
	if (exact.length === 1) orConditions.push({ $eq: [inputExpr, exact[0]] });
	else if (exact.length > 1) orConditions.push({ $in: [inputExpr, exact] });
	for (const p of starts) orConditions.push({ $eq: [{ $indexOfCP: [inputExpr, p] }, 0] });
	if (orConditions.length === 0) return null;
	return orConditions.length === 1 ? orConditions[0] : { $or: orConditions };
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Builds MongoDB aggregation projection stages for filtering resource fields and nested data.
 *
 * @param {string} resourceType - The type of resource being projected (e.g., 'event', 'team')
 * @param {Object} fieldProjections - Configuration object containing inclusion/exclusion rules
 * @param {Object} [fieldProjections.inclusions] - Fields to include, with 'all' and type-specific keys
 * @param {Object} [fieldProjections.exclusions] - Fields to exclude, with 'all' and type-specific keys
 * @param {boolean} include - True for inclusion mode, false for exclusion mode
 *
 * @returns {Object} Projection configuration object
 * @returns {Object|null} returns.setStage - MongoDB $set stage for complex field transformations (tags, participants)
 * @returns {Object|null} returns.projectStage - MongoDB $project stage for simple field inclusion/exclusion (1/0 values)
 *
 * @description
 * Handles special field projections:
 * - "tags" - Root level tags
 * - "tags,name1,name2" - filter tags with these names
 * - "participants.sp", "participants.team" - include or exclude teams or sports person participants
 * - "participants.sp.tags", "participants.team.tags" - include or exclude teams or sports person participant tags arrays
 * - "participants.sp.tags,name4,name5*" - * means startsWith - include or exclude teams or sports person participant tags arrays with these name filters
 *
 * Supports prefix-based filtering using comma-separated syntax: "tags,prefix1,prefix2*"
 *
 * "tags,a,b*" keeps (or excludes) tags where:
 * - name is exactly "a", or
 * - name starts with "b" (checked via indexOfCP(name, "b") === 0).
 * - Same for participants.team.tags,role*,shirtNo.
 *
 * In inclusion mode, only specified fields/prefixes are kept.
 * In exclusion mode, specified fields/prefixes are removed.
 *
 * @example
 * // Include only tags starting with "match" and team participants
 * buildProjectionForType('event', {
 *   inclusions: { all: { 'tags,match*': true, 'participants.team': true } }
 * }, true);
 */
function buildProjectionForType(resourceType, fieldProjections, include) {
	//////////////////////////////////////////////////////////////////////////////
	const cfg = include ? fieldProjections?.inclusions : fieldProjections?.exclusions;
	const forAll = _.cloneDeep(cfg?.all) || {};
	const forType = _.cloneDeep(cfg?.[resourceType]) || {};
	const projectStage = {}; // will accumulate classic 1/0 projections *for non-special keys*
	//////////////////////////////////////////////////////////////////////////////
	// The control object
	const ctl = {
		includeTags: undefined, // true/false/undefined (undefined = no change)
		tagNamePrefixes: undefined, // array of prefixes (optional)
		excludeListedTagPrefixes: undefined, // true/false (only for exclusions mode)
		includeTeamParticipants: undefined, // team participants
		includeSpParticipants: undefined, // sportsPerson participants
		includeTeamParticipantTags: undefined, // nested team participant tags
		excludeListedTeamTagPrefixes: undefined, // true/false (only for exclusions mode)
		teamParticipantTagPrefixes: undefined, // array of prefixes (optional) for nested team participant tags
		includeSpParticipantTags: undefined, // nested sportsPerson participant tags
		excludeListedSpTagPrefixes: undefined, // true/false (only for exclusions mode)
		spParticipantTagPrefixes: undefined, // array of prefixes (optional) for nested sportsPerson participant tags
		isExclusionMode: !include, // true = exclusions; false = inclusions
	};

	//////////////////////////////////////////////////////////////////////////////
	// track whether "tags" was explicitly referenced (so we can add tags:1 to inclusion project)
	// track whether "participants" was explicitly referenced (so we can add participants:1 to inclusion project)
	let sawTagsKey = false;
	let sawParticipantsKey = false;

	//////////////////////////////////////////////////////////////////////////////
	// Parse “tags,name1,name2*” => { base:'tags', prefixes:['name1','name2*'] }
	const parseTagsKey = (raw) => {
		const parts = raw
			.split('>')
			.map((s) => s.trim())
			.filter(Boolean);
		const base = parts.shift(); // "tags" | "participants.sp.tags" | ...
		return { base, prefixes: parts.length ? parts : null };
	};

	//////////////////////////////////////////////////////////////////////////////
	// Apply one key into ctl / projectStage (returns true if it was a special key)
	const applyKey = (key) => {
		const isIncludeMode = !ctl.isExclusionMode;
		const { base, prefixes } = parseTagsKey(key);

		////////////////////////////////////////////////////////////////////////////
		// Process root level tags
		if (base === 'tags') {
			sawTagsKey = true;
			ctl.includeTags = Boolean(isIncludeMode || prefixes);
			ctl.excludeListedTagPrefixes = Boolean(!isIncludeMode && prefixes);
			ctl.tagNamePrefixes = prefixes || null;
			if (isIncludeMode) projectStage.tags = 1;
			return true;
		}
		////////////////////////////////////////////////////////////////////////////
		// SP participants
		if (base === 'participants.sp') {
			sawParticipantsKey = true;
			ctl.includeSpParticipants = isIncludeMode ? true : false;
			if (isIncludeMode) projectStage.participants = 1;
			return true;
		}
		////////////////////////////////////////////////////////////////////////////
		// Team participants
		if (base === 'participants.team') {
			sawParticipantsKey = true;
			ctl.includeTeamParticipants = isIncludeMode ? true : false;
			if (isIncludeMode) projectStage.participants = 1;
			return true;
		}
		////////////////////////////////////////////////////////////////////////////
		// Team participant tags
		if (base === 'participants.team.tags') {
			sawParticipantsKey = true;
			ctl.includeTeamParticipantTags = isIncludeMode || (!isIncludeMode && prefixes) ? true : false;
			ctl.excludeListedTeamTagPrefixes = !isIncludeMode && prefixes ? true : false;
			ctl.teamParticipantTagPrefixes = prefixes || null;
			if (isIncludeMode) projectStage.participants = 1;
			return true;
		}
		////////////////////////////////////////////////////////////////////////////
		// SP participant tags
		if (base === 'participants.sp.tags') {
			sawParticipantsKey = true;
			ctl.includeSpParticipantTags = isIncludeMode || (!isIncludeMode && prefixes) ? true : false;
			ctl.excludeListedSpTagPrefixes = !isIncludeMode && prefixes ? true : false;
			ctl.spParticipantTagPrefixes = prefixes || null;
			if (isIncludeMode) projectStage.participants = 1;
			return true;
		}
		////////////////////////////////////////////////////////////////////////////
		// All participant tags regardless of type
		if (base === 'participants.tags') {
			sawParticipantsKey = true;
			ctl.includeTeamParticipantTags = isIncludeMode || (!isIncludeMode && prefixes) ? true : false;
			ctl.excludeListedTeamTagPrefixes = !isIncludeMode && prefixes ? true : false;
			ctl.teamParticipantTagPrefixes = prefixes || null;
			ctl.includeSpParticipantTags = isIncludeMode || (!isIncludeMode && prefixes) ? true : false;
			ctl.excludeListedSpTagPrefixes = !isIncludeMode && prefixes ? true : false;
			ctl.spParticipantTagPrefixes = prefixes || null;
			if (isIncludeMode) projectStage.participants = 1;
			return true;
		}

		////////////////////////////////////////////////////////////////////////////
		// Normal (non-special) field: turn into 1/0 in projection
		projectStage[key] = isIncludeMode ? 1 : 0;
		return false;
	};

	//////////////////////////////////////////////////////////////////////////////
	// Participant tag filter builders (team / sp)
	// kind = 'team' | 'sp';
	const buildPTagExpr = (kind) => {
		const includeFlag = kind === 'team' ? ctl.includeTeamParticipantTags : ctl.includeSpParticipantTags;
		const names = kind === 'team' ? ctl.teamParticipantTagPrefixes : ctl.spParticipantTagPrefixes;
		const excludeListed = kind === 'team' ? ctl.excludeListedTeamTagPrefixes : ctl.excludeListedSpTagPrefixes;
		if (includeFlag === false) return '$$REMOVE'; // drop tags entirely
		if (includeFlag === true && Array.isArray(names) && names.length) {
			const match = buildNameMatchExpr(names, '$$pt.name');
			return {
				$let: {
					// MongoDB $let variables must start with a letter (not underscore)
					vars: { src: { $ifNull: ['$$p.tags', []] } },
					in: match ? { $filter: { input: '$$src', as: 'pt', cond: excludeListed ? { $not: [match] } : match } } : '$$src',
				},
			};
		}
		return null;
	};

	//////////////////////////////////////////////////////////////////////////////
	// Ingest keys from `all` and `[resourceType]`
	for (const key of Object.keys(forAll)) applyKey(key);
	for (const key of Object.keys(forType)) {
		applyKey(key);
	}
	if (include && sawTagsKey) projectStage.tags = 1;
	if (include && sawParticipantsKey) projectStage.participants = 1;

	//////////////////////////////////////////////////////////////////////////////
	// For participant type detection:
	// For participant type detection (treat missing as null so they don't pass)
	const isTeamExpr = { $and: [{ $ne: [{ $ifNull: ['$$p._externalTeamId', null] }, null] }, { $ne: [{ $ifNull: ['$$p._externalTeamIdScope', null] }, null] }] };
	const isPersonExpr = {
		$and: [{ $ne: [{ $ifNull: ['$$p._externalSportsPersonId', null] }, null] }, { $ne: [{ $ifNull: ['$$p._externalSportsPersonIdScope', null] }, null] }],
	};

	//////////////////////////////////////////////////////////////////////////////
	let tagsExpr = '$tags';
	//////////////////////////////////////////////////////////////////////////////
	// Tag exclusion
	if (ctl.includeTags === false) {
		tagsExpr = '$$REMOVE';
	}
	//////////////////////////////////////////////////////////////////////////////
	// Tag name filtering
	else if (ctl.includeTags === true && Array.isArray(ctl.tagNamePrefixes)) {
		const match = buildNameMatchExpr(ctl.tagNamePrefixes, '$$t.name');
		if (match) tagsExpr = { $filter: { input: { $ifNull: ['$tags', []] }, as: 't', cond: ctl.excludeListedTagPrefixes ? { $not: [match] } : match } };
		else tagsExpr = { $ifNull: ['$tags', []] };
	}

	//////////////////////////////////////////////////////////////////////////////
	// Participants base filter (by type)
	let participantsInput = { $ifNull: ['$participants', []] };
	if (ctl.includeTeamParticipants !== undefined || ctl.includeSpParticipants !== undefined) {
		const conditionals = [];
		////////////////////////////////////////////////////////////////////////////
		// include mode: keep the selected types
		if (ctl.includeTeamParticipants === true) conditionals.push(isTeamExpr);
		if (ctl.includeSpParticipants === true) conditionals.push(isPersonExpr);
		////////////////////////////////////////////////////////////////////////////
		// exclusion mode: if explicitly disabled, negate those
		if (ctl.includeTeamParticipants === false) conditionals.push({ $not: [isTeamExpr] });
		if (ctl.includeSpParticipants === false) conditionals.push({ $not: [isPersonExpr] });
		if (conditionals.length === 1) participantsInput = { $filter: { input: { $ifNull: ['$participants', []] }, as: 'p', cond: conditionals[0] } };
		else if (conditionals.length > 1) participantsInput = { $filter: { input: { $ifNull: ['$participants', []] }, as: 'p', cond: { $and: conditionals } } };
	}

	//////////////////////////////////////////////////////////////////////////////
	const teamTagsExpr = buildPTagExpr('team');
	const spTagsExpr = buildPTagExpr('sp');
	//////////////////////////////////////////////////////////////////////////////
	// Build participant tags expression
	let participantTagsExpr;
	if (teamTagsExpr === '$$REMOVE' && spTagsExpr === '$$REMOVE') {
		participantTagsExpr = '$$REMOVE';
	} else {
		const teamTagsValue = teamTagsExpr === null ? '$$p.tags' : teamTagsExpr;
		const spTagsValue = spTagsExpr === null ? '$$p.tags' : spTagsExpr;
		participantTagsExpr = { $cond: [isTeamExpr, teamTagsValue, spTagsValue] };
	}
	//////////////////////////////////////////////////////////////////////////////
	// Build participants expression
	const participantsExpr =
		ctl.includeTeamParticipants !== undefined ||
		ctl.includeSpParticipants !== undefined ||
		ctl.includeTeamParticipantTags !== undefined ||
		ctl.includeSpParticipantTags !== undefined
			? {
					$map: {
						input: participantsInput,
						as: 'p',
						in: {
							_externalSportsPersonId: '$$p._externalSportsPersonId',
							_externalSportsPersonIdScope: '$$p._externalSportsPersonIdScope',
							_externalTeamId: '$$p._externalTeamId',
							_externalTeamIdScope: '$$p._externalTeamIdScope',
							tags: participantTagsExpr,
						},
					},
			  }
			: null;

	const setAssignments = {};
	if (ctl.includeTags !== undefined) setAssignments.tags = tagsExpr;
	if (participantsExpr) setAssignments.participants = participantsExpr;
	const setStage = Object.keys(setAssignments).length ? setAssignments : null;
	const normalizedProject = Object.keys(projectStage).length ? projectStage : null;
	return { setStage, projectStage: normalizedProject };
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Builds MongoDB aggregation pipeline stages for field projections with proper ordering.
 *
 * This function creates a sequence of $set and $project stages that handle both field
 * exclusions and inclusions while maintaining correct precedence rules.
 *
 * The stages are ordered as follows:
 * 1. Exclusion $set stage - may use $$REMOVE to exclude fields
 * 2. Inclusion $set stage - computes shapes for remaining fields
 * 3. Inclusion $project stage - projects included fields
 * 4. Exclusion $project stage - final exclusions (takes precedence)
 *
 * This ordering ensures that excluded fields cannot reappear even if they were
 * included in earlier stages, as the exclusion $project stage runs last.
 *
 * @param {string} resourceType - The type of resource being projected
 * @param {Object} fieldProjections - Object defining which fields to include/exclude
 * @returns {Array<Object>} Array of MongoDB aggregation pipeline stages ($set and $project)
 */
function buildProjectionPhases(resourceType, fieldProjections) {
	//////////////////////////////////////////////////////////////////////////////
	// 1) Build exclusion phase
	const { setStage: exSet, projectStage: exProj } = buildProjectionForType(resourceType, fieldProjections, false);
	//////////////////////////////////////////////////////////////////////////////
	// 2) Build inclusion phase
	const { setStage: inSet, projectStage: inProj } = buildProjectionForType(resourceType, fieldProjections, true);
	//////////////////////////////////////////////////////////////////////////////
	// Order matters:
	// - Run $set stages first (exclusions then inclusions). Exclusion $set may $$REMOVE fields;
	//   inclusion $set can still compute shapes for the survivors.
	// - Run $project stages after that. Put *exclusion* $project LAST so it wins.
	//   This guarantees that fields excluded earlier won't reappear even if included later.
	const pipelineStages = [exSet && { $set: exSet }, inSet && { $set: inSet }, inProj && { $project: inProj }, exProj && { $project: exProj }].filter(Boolean);
	//////////////////////////////////////////////////////////////////////////////
	return pipelineStages;
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Creates a unique key string representing a field mapping between two collections.
 *
 * @param {string} from - The source collection name
 * @param {string} field - The field name being mapped
 * @param {string} to - The target collection name
 * @returns {string} A formatted key string in the format "from.field->to"
 *
 * @example
 * // Returns "users.profileId->profiles"
 * makeKey("users", "profileId", "profiles");
 */
function makeKey(from, field, to) {
	return `${from}.${field}->${to}`;
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Generates a hash key from a string using the djb2 hash algorithm with XOR variation.
 * The hash is computed by iterating through each character of the input string,
 * applying bit shifts and XOR operations, then converting the result to base-36.
 *
 * @param {string} s - The input string to hash
 * @returns {string} A base-36 encoded hash string representing the input
 *
 * @example
 * // Returns a hash like "2w8qc9"
 * hashKey("hello world");
 *
 * @example
 * // Returns a hash like "1x9k2p"
 * hashKey("test string");
 */
function hashKey(s) {
	let h = 5381;
	for (let i = 0; i < s.length; i++) h = ((h << 5) + h) ^ s.charCodeAt(i);
	return (h >>> 0).toString(36);
}

////////////////////////////////////////////////////////////////////////////////
// Safe variable label
/**
 * Converts a string to a safe variable name by replacing non-word characters with underscores.
 *
 * This function takes any input, converts it to a string, and replaces all characters
 * that are not alphanumeric or underscore (non-word characters) with underscores.
 * This is useful for creating safe identifiers or variable names from arbitrary strings.
 *
 * @param {*} s - The input value to be converted to a safe variable name
 * @returns {string} A string with all non-word characters replaced by underscores
 *
 * @example
 * safeVar("hello-world") // returns "hello_world"
 * safeVar("user@domain.com") // returns "user_domain_com"
 * safeVar(123) // returns "123"
 * safeVar("valid_name") // returns "valid_name"
 */
function safeVar(s) {
	return String(s).replace(/[^\w]/g, '_');
}

////////////////////////////////////////////////////////////////////////////////
module.exports = clientAggregationPipelineBuilder;
