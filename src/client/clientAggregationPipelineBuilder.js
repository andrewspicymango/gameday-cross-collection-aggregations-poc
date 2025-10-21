const _ = require('lodash');

////////////////////////////////////////////////////////////////////////////////
// Constants & config
////////////////////////////////////////////////////////////////////////////////
const idField = 'gamedayId';
const aggregationCollectionName = 'materialisedAggregations';
const EDGES = require('./clientAggregationPipelineBuilderEdges.js');
const COLLECTIONS = require('./clientAggregationPipelineBuilderCollections.js');

// Fields to hide when materialising final resources
const fieldsToExcludeFromMaterialisedResources = {
	_stickies: 0,
	_original: 0,
};

////////////////////////////////////////////////////////////////////////////////
// API (explicit routes only, strict parsing + cycle & duplicate-edge detection)
////////////////////////////////////////////////////////////////////////////////
function clientAggregationPipelineBuilder({
	rootType,
	rootExternalKey,
	totalMax,
	routes, // [{ key, to, via: ["from.field->to", ...] }, ...]  REQUIRED
	includeTypes, // ["team", "venue", ...] REQUIRED (controls materialisation + budget order)
	fieldProjections,
	edges,
	collectionMap,
}) {
	//////////////////////////////////////////////////////////////////////////////
	// Validation
	if (!rootType || !rootExternalKey) throw new Error('rootType and rootExternalKey are required');
	if (!Array.isArray(routes) || routes.length === 0) {
		throw new Error('routes must be a non-empty array of explicit routes.');
	}
	if (!Array.isArray(includeTypes) || includeTypes.length === 0) {
		throw new Error('includeTypes must be a non-empty array of types to materialise.');
	}
	if (!Number.isInteger(totalMax) || totalMax < 0) {
		throw new Error('totalMax must be a non-negative integer');
	}
	//////////////////////////////////////////////////////////////////////////////
	// Validate fieldProjections if provided
	if (fieldProjections && typeof fieldProjections !== 'object') {
		throw new Error('fieldProjections must be an object mapping resource types to inclusions and exclusions');
	}

	//////////////////////////////////////////////////////////////////////////////
	// Normalise includeTypes order & uniqueness (budget is applied in this order)
	includeTypes = [...new Set(includeTypes)];

	//////////////////////////////////////////////////////////////////////////////
	// Directed, field-labelled graph
	const EDGES_FOR_PIPELINE = edges || EDGES;

	//////////////////////////////////////////////////////////////////////////////
	// resourceType -> real collection (for final materialisation)
	const COLLECTIONS_FOR_PIPELINE = collectionMap || COLLECTIONS;

	//////////////////////////////////////////////////////////////////////////////
	// Validate that we can materialise root + requested include types
	const allToMaterialise = new Set([rootType, ...includeTypes]);
	for (const t of allToMaterialise) {
		if (!COLLECTIONS_FOR_PIPELINE[t]) {
			throw new Error(`No collection mapping for resourceType='${t}'. Provide 'collectionMap' or extend defaults.`);
		}
	}

	//////////////////////////////////////////////////////////////////////////////
	// Parse routes strictly (throws on any non-contiguous / invalid hop, cycles, or duplicate edges)
	const parsedRoutes = routes.map((r) => ({
		key: r.key,
		to: r.to,
		path: parseExplicitRouteStrictNoCyclesNoDup({ EDGES: EDGES_FOR_PIPELINE, rootType, route: r }),
	}));

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
	for (const t of includeTypes) {
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
	stages.push({ $addFields: { _remaining: totalMax } });
	//////////////////////////////////////////////////////////////////////////////
	stages.push({
		$addFields: {
			_rootIncludedIds: { $cond: [{ $gt: ['$_remaining', 0] }, { $slice: ['$_rootIds', 1] }, []] },
			_rootOverflowIds: { $cond: [{ $gt: ['$_remaining', 0] }, { $slice: ['$_rootIds', 0] }, '$_rootIds'] },
			_remaining: { $cond: [{ $gt: ['$_remaining', 0] }, { $subtract: ['$_remaining', 1] }, '$_remaining'] },
		},
	});

	//////////////////////////////////////////////////////////////////////////////
	// For each included type: slice from its union set, track overflow, decrement remaining
	for (const t of includeTypes) {
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
								vars: {
									take: { $min: ['$_remaining', { $size: `$${idsVar}` }] },
									arraySize: { $size: `$${idsVar}` },
								},
								in: {
									$cond: [{ $eq: ['$$take', '$$arraySize'] }, [], { $slice: [`$${idsVar}`, '$$take', { $subtract: ['$$arraySize', '$$take'] }] }],
								},
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
	// Root facet
	const rootProjectionInclusions = buildProjectionForType(rootType, fieldProjections, true);
	const rootProjectionExclusions = buildProjectionForType(rootType, fieldProjections, false);
	const rootLookupPipeline = [{ $match: { $expr: { $in: ['$_id', '$$ids'] } } }, ...sortStageFor(rootType)];
	if (rootProjectionExclusions) rootLookupPipeline.push({ $project: rootProjectionExclusions });
	if (rootProjectionInclusions) rootLookupPipeline.push({ $project: rootProjectionInclusions });

	facet[rootType] = [
		{ $project: { includedIds: '$_rootIncludedIds', overflowIds: '$_rootOverflowIds' } },
		{
			$lookup: {
				from: COLLECTIONS_FOR_PIPELINE[rootType],
				let: { ids: '$includedIds' },
				pipeline: rootLookupPipeline,
				as: 'docs',
			},
		},
		{ $replaceWith: { items: '$docs', overflow: { resourceType: rootType, overflowIds: '$overflowIds' } } },
	];

	//////////////////////////////////////////////////////////////////////////////
	// Included types facets
	for (const t of includeTypes) {
		const includedVar = `_inc_${t}_ids`;
		const overflowVar = `_ovf_${t}_ids`;
		const typeProjectionInclusions = buildProjectionForType(t, fieldProjections, true);
		const typeProjectionExclusions = buildProjectionForType(t, fieldProjections, false);
		const typeLookupPipeline = [{ $match: { $expr: { $in: ['$_id', '$$ids'] } } }, ...sortStageFor(t)];
		if (typeProjectionExclusions) typeLookupPipeline.push({ $project: typeProjectionExclusions });
		if (typeProjectionInclusions) typeLookupPipeline.push({ $project: typeProjectionInclusions });
		facet[t] = [
			{ $project: { includedIds: `$${includedVar}`, overflowIds: `$${overflowVar}` } },
			{
				$lookup: {
					from: COLLECTIONS_FOR_PIPELINE[t],
					let: { ids: '$includedIds' },
					pipeline: typeLookupPipeline,
					as: 'docs',
				},
			},
			{ $replaceWith: { items: '$docs', overflow: { resourceType: t, overflowIds: '$overflowIds' } } },
		];
	}
	//////////////////////////////////////////////////////////////////////////////
	stages.push({ $facet: facet });
	//////////////////////////////////////////////////////////////////////////////
	// Final shape
	const resultsProjection = {};
	for (const t of includeTypes) {
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

// STRICT + CYCLE + DUPLICATE-EDGE-DETECTING parser for explicit routes.
// Rules:
//  - First hop's `from` must equal rootType.
//  - Each next hop's `from` must equal the previous hop's `to` (contiguous).
//  - Edge must exist in EDGES[from] and match declared `to`.
//  - No cycles: may not revisit any previously reached node within the same route.
//  - No duplicate edges: the same '<from>.<field>-><to>' may not appear twice in a route.
//  - Final node must equal route.to.
// Throws detailed errors describing exactly which hop failed and why.
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
// Consolidate multiple paths into unique steps (shared-hop elimination)
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
// Helper function to build projection stage for a resource type
function buildProjectionForType(resourceType, fieldProjections, include) {
	//////////////////////////////////////////////////////////////////////////////
	let projection = null;
	//////////////////////////////////////////////////////////////////////////////
	// Inclusions
	if (include === true) {
		if (_.isObject(fieldProjections?.inclusions?.all) && Object.keys(fieldProjections.inclusions.all).length > 0) {
			for (const key in fieldProjections.inclusions.all) fieldProjections.inclusions.all[key] = 1;
			if (projection == null) projection = fieldProjections.inclusions.all;
			projection = fieldProjections.inclusions.all;
		}
		if (_.isObject(fieldProjections?.inclusions?.[resourceType]) && Object.keys(fieldProjections.inclusions[resourceType]).length > 0) {
			for (const key in fieldProjections.inclusions[resourceType]) fieldProjections.inclusions[resourceType][key] = 1;
			if (projection == null) projection = {};
			projection = { ...projection, ...fieldProjections.inclusions[resourceType] };
		}
		return projection;
	}
	//////////////////////////////////////////////////////////////////////////////
	// Exclusions
	else {
		if (_.isObject(fieldProjections?.exclusions?.all) && Object.keys(fieldProjections.exclusions.all).length > 0) {
			for (const key in fieldProjections.inclusions.all) fieldProjections.exclusions.all[key] = 0;
			if (projection == null) projection = fieldProjections.exclusions.all;
			projection = fieldProjections.exclusions.all;
		}
		if (_.isObject(fieldProjections?.exclusions?.[resourceType]) && Object.keys(fieldProjections.exclusions[resourceType]).length > 0) {
			for (const key in fieldProjections.exclusions[resourceType]) fieldProjections.exclusions[resourceType][key] = 0;
			if (projection == null) projection = {};
			projection = { ...projection, ...fieldProjections.exclusions[resourceType] };
		}
		return projection;
	}
}

////////////////////////////////////////////////////////////////////////////////
// Hop key helper
function makeKey(from, field, to) {
	return `${from}.${field}->${to}`;
}

////////////////////////////////////////////////////////////////////////////////
// Non-crypto small hash for stable field names
function hashKey(s) {
	let h = 5381;
	for (let i = 0; i < s.length; i++) h = ((h << 5) + h) ^ s.charCodeAt(i);
	return (h >>> 0).toString(36);
}

////////////////////////////////////////////////////////////////////////////////
// Safe variable label
function safeVar(s) {
	return String(s).replace(/[^\w]/g, '_');
}

////////////////////////////////////////////////////////////////////////////////
module.exports = clientAggregationPipelineBuilder;
