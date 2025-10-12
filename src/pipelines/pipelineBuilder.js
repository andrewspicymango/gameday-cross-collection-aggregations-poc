////////////////////////////////////////////////////////////////////////////////
const idField = 'gamedayId';
const aggregationCollectionName = 'materialisedAggregations';

////////////////////////////////////////////////////////////////////////////////
// {
//  // Exclude specific fields by setting them to 0
//  fieldToExclude1: 0,
//  fieldToExclude2: 0,
//  // Or include only specific fields by setting them to 1
//  // _id: 1,
//  // fieldToInclude1: 1,
//  // fieldToInclude2: 1
// },
const fieldsToExcludeFromMaterialisedResources = {
	_stickies: 0,
	_original: 0,
};

////////////////////////////////////////////////////////////////////////////////
// Example: From a COMPETITION → fetch EVENTS + TEAMS + SGOS
const pipeline = buildMaterialisedListsPipelineTotalMax({
	rootType: 'competition',
	rootExternalKey: '289175 @ fifa',
	targetTypes: ['event', 'team'], // 'team', 'sgo'],
	totalMax: 4,
});

////////////////////////////////////////////////////////////////////////////////
// materialiser.totalMax.resources.js
// Optimised totalMax multi-target materialiser:
// - Traverse *materialisedAggregations* once to compute ID sets (shared hops)
// - Materialise documents from the correct resource collections (e.g., 'stages', 'events', 'teams')
// - Apply a single totalMax budget across root + target types in the caller-specified order

////////////////////////////////////////////////////////////////////////////////
/**
 * Build a total-budget (totalMax) multi-target pipeline with shared-hop traversal.
 *
 * @param {Object} params
 * @param {string} params.rootType
 * @param {string} params.rootExternalKey
 * @param {string[]} params.targetTypes             // order controls budget allocation (after root)
 * @param {number} params.totalMax                  // total number of docs to materialise (including root)
 * @param {Object} [params.edges]                   // override the default graph edges if needed
 * @param {Object} [params.collectionMap]          // map resourceType -> collection name for *materialisation*
 * @param {('id'|'lastUpdated'|null)} [params.sortBy='id'] // per-type sort inside materialised arrays
 * @param {1|-1} [params.sortDir=1]
 * @returns {import('mongodb').Document[]}
 */
function buildMaterialisedListsPipelineTotalMax({ rootType, rootExternalKey, targetTypes, totalMax, edges, collectionMap, sortBy = 'id', sortDir = 1 }) {
	if (!rootType || !rootExternalKey) throw new Error('rootType and rootExternalKey are required');
	if (!Array.isArray(targetTypes) || targetTypes.length === 0) throw new Error('targetTypes must be a non-empty array');
	if (!Number.isInteger(totalMax) || totalMax < 0) throw new Error('totalMax must be a non-negative integer');
	////////////////////////////////////////////////////////////////////////////////
	// Directed, field-labeled graph of materialised edges (extend as your views evolve)
	const EDGES = edges || {
		competition: { stages: 'stage', sgos: 'sgo' },
		stage: { events: 'event', competitions: 'competition' },
		event: { teams: 'team', venues: 'venue', sportsPersons: 'sportsPerson', stages: 'stage' },
		team: { clubs: 'club', events: 'event', nations: 'nation' },
		venue: { events: 'event' },
		club: { teams: 'team' },
		sgo: { competitions: 'competition' },
		nation: { teams: 'team' },
	};
	////////////////////////////////////////////////////////////////////////////////
	// resourceType -> resource collection name (for final materialisation lookups)
	const COLLECTIONS = collectionMap || {
		competition: 'competitions',
		stage: 'stages',
		event: 'events',
		team: 'teams',
		venue: 'venues',
		club: 'clubs',
		sgo: 'sgos',
		nation: 'nations',
		sportsPerson: 'sportsPersons',
	};
	//////////////////////////////////////////////////////////////////////////////
	// Validate that we can materialise all requested types (root + targets)
	const allTypes = new Set([rootType, ...targetTypes]);
	for (const t of allTypes) {
		if (!COLLECTIONS[t]) {
			throw new Error(`No collection mapping for resourceType='${t}'. Provide 'collectionMap' or extend defaults.`);
		}
	}
	//////////////////////////////////////////////////////////////////////////////
	// 1) Compute shortest hop paths from root → each target
	const pathsByTarget = {};
	for (const t of targetTypes) {
		const p = findPath(EDGES, rootType, t);
		if (p === null) throw new Error(`No materialised path from '${rootType}' to '${t}'. Add an edge if it should exist.`);
		pathsByTarget[t] = p; // [] => root == target
	}

	//////////////////////////////////////////////////////////////////////////////
	// 2) Merge paths into unique traversal steps (shared hops computed once)
	const steps = planSteps(Object.values(pathsByTarget));

	//////////////////////////////////////////////////////////////////////////////
	// 3) Build pipeline (traverse in *materialisedAggregations*)
	const stages = [
		{ $match: { resourceType: rootType, externalKey: rootExternalKey } },
		{ $addFields: { _rootKey: '$externalKey' } },
		// expose the root's own id as an array for uniform budget handling later
		{ $addFields: { _rootIds: [`$${idField}`] } },
	];

	for (const step of steps) {
		const { from, field, dependsOnKey, outputName, depth } = step;

		if (depth === 0) {
			// First hop: read array directly from the root doc
			stages.push({ $addFields: { [outputName]: { $ifNull: [`$${field}`, []] } } });
		} else {
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
			stages.push({ $addFields: { [outputName]: { $ifNull: [{ $arrayElemAt: [`$${lkField}.ids`, 0] }, []] } } });
		}
	}

	//////////////////////////////////////////////////////////////////////////////
	// 4) Budget allocation (root first, then in the order of targetTypes)
	stages.push({ $addFields: { _remaining: totalMax } });

	//////////////////////////////////////////////////////////////////////////////
	// Root allocation (counts as 1 if budget > 0)
	stages.push({
		$addFields: {
			_rootIncludedIds: {
				$cond: [{ $gt: ['$_remaining', 0] }, { $slice: ['$_rootIds', 1] }, []],
			},
			_rootOverflowIds: {
				$cond: [{ $gt: ['$_remaining', 0] }, { $slice: ['$_rootIds', 0] }, '$_rootIds'],
			},
			_remaining: {
				$cond: [{ $gt: ['$_remaining', 0] }, { $subtract: ['$_remaining', 1] }, '$_remaining'],
			},
		},
	});

	//////////////////////////////////////////////////////////////////////////////
	// For each target, attach its final ID set and consume budget
	const finalArrayFieldByType = {};
	for (const t of targetTypes) {
		const path = pathsByTarget[t];
		let sourceFieldExpr;
		if (path.length === 0) {
			sourceFieldExpr = '$_rootIds';
		} else {
			const lastHop = path[path.length - 1];
			const lastKey = makeKey(lastHop.from, lastHop.field, lastHop.to);
			const lastStep = steps.find((s) => s.key === lastKey);
			if (!lastStep) throw new Error(`Internal: cannot find step for '${t}'`);
			sourceFieldExpr = `$${lastStep.outputName}`;
		}

		const idsVar = `_src_${t}_ids`;
		finalArrayFieldByType[t] = idsVar;

		stages.push({ $addFields: { [idsVar]: sourceFieldExpr } });

		const includedVar = `_inc_${t}_ids`;
		const overflowVar = `_ovf_${t}_ids`;

		stages.push({
			$addFields: {
				[includedVar]: {
					$let: {
						vars: { sz: { $size: `$${idsVar}` } },
						in: {
							$cond: [{ $gt: ['$_remaining', 0] }, { $slice: [`$${idsVar}`, { $min: ['$_remaining', '$$sz'] }] }, []],
						},
					},
				},
				[overflowVar]: {
					$cond: [
						{ $gt: ['$_remaining', 0] },
						{
							$let: {
								vars: { take: { $min: ['$_remaining', { $size: `$${idsVar}` }] } },
								in: { $slice: [`$${idsVar}`, '$$take', { $subtract: [{ $size: `$${idsVar}` }, '$$take'] }] },
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
	// 5) Materialise from *resource* collections via $facet
	const sortStageFor = (type) => {
		return [{ $sort: { _id: 1 } }];
	};

	const facet = {};

	//////////////////////////////////////////////////////////////////////////////
	// Root facet (materialise the single root doc from its resource collection)
	facet[rootType] = [
		{ $project: { includedIds: '$_rootIncludedIds', overflowIds: '$_rootOverflowIds' } },
		{
			$lookup: {
				from: COLLECTIONS[rootType],
				let: { ids: '$includedIds' },
				pipeline: [
					{ $match: { $expr: { $in: [`$_id`, '$$ids'] } } },
					...sortStageFor(rootType),
					,
					{
						$project: fieldsToExcludeFromMaterialisedResources,
					},
				],
				as: 'docs',
			},
		},
		{ $replaceWith: { items: '$docs', overflow: { resourceType: rootType, overflowIds: '$overflowIds' } } },
	];

	//////////////////////////////////////////////////////////////////////////////
	// Target type facets
	for (const t of targetTypes) {
		const includedVar = `_inc_${t}_ids`;
		const overflowVar = `_ovf_${t}_ids`;

		facet[t] = [
			{ $project: { includedIds: `$${includedVar}`, overflowIds: `$${overflowVar}` } },
			{
				$lookup: {
					from: COLLECTIONS[t], // <-- materialise from the real resource collection
					let: { ids: '$includedIds' },
					pipeline: [
						{ $match: { $expr: { $in: [`$_id`, '$$ids'] } } },
						...sortStageFor(t),
						{
							$project: fieldsToExcludeFromMaterialisedResources,
						},
					],
					as: 'docs',
				},
			},
			{ $replaceWith: { items: '$docs', overflow: { resourceType: t, overflowIds: '$overflowIds' } } },
		];
	}

	stages.push({ $facet: facet });

	//////////////////////////////////////////////////////////////////////////////
	// 6) Final shape
	const resultsProjection = {};
	for (const t of allTypes) {
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

	return stages;
}

//////////////////////////////////////////////////////////////////////////////
// Helper Graph Functions
////////////////////////////////////////////////////////////////////////////////

//////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////
function findPath(EDGES, startType, endType) {
	if (startType === endType) return [];
	const queue = [[startType, []]];
	const visited = new Set([startType]);

	while (queue.length) {
		const [cur, path] = queue.shift();
		const outs = EDGES[cur] || {};
		for (const [field, to] of Object.entries(outs)) {
			const hop = { from: cur, field, to };
			const nextPath = [...path, hop];
			if (to === endType) return nextPath;
			if (!visited.has(to)) {
				visited.add(to);
				queue.push([to, nextPath]);
			}
		}
	}
	return null;
}

////////////////////////////////////////////////////////////////////////////////
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
function makeKey(from, field, to) {
	return `${from}.${field}->${to}`;
}

////////////////////////////////////////////////////////////////////////////////
function hashKey(s) {
	let h = 5381;
	for (let i = 0; i < s.length; i++) h = ((h << 5) + h) ^ s.charCodeAt(i);
	return (h >>> 0).toString(36);
}

////////////////////////////////////////////////////////////////////////////////
module.exports = {
	buildMaterialisedListsPipelineTotalMax,
	findPath,
	planSteps,
	makeKey,
	hashKey,
};
