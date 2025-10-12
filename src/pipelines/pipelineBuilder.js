// pipelineBuilder.js

////////////////////////////////////////////////////////////////////////////////
const idField = 'gamedayId';
const collection = 'materialisedAggregations';
////////////////////////////////////////////////////////////////////////////////
// Example: From a COMPETITION → fetch EVENTS + TEAMS + SGOS
const pipeline = buildMaterialisedListsPipelineTotalMax({
	rootType: 'competition',
	rootExternalKey: '289175 @ fifa',
	targetTypes: ['event', 'team'], // 'team', 'sgo'],
	totalMax: 4,
});

////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////
// materialiser.totalMax.js
// Build one aggregation pipeline that:
//  - Finds shortest paths from root → each target
//  - Merges shared hops (compute once, reuse across targets)
//  - Applies a single totalMax budget across (root + targets in the order given)
//  - Returns per-type materialised items and overflowIds
////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////
/**
 * Build a total-budget (totalMax) multi-target pipeline with shared-hop traversal.
 *
 * @param {Object} params
 * @param {string} params.rootType                       // e.g. "competition"
 * @param {string} params.rootExternalKey                // e.g. "289175 @ fifa"
 * @param {string[]} params.targetTypes                  // order determines budget allocation after root
 * @param {number} params.totalMax                       // total number of docs to materialise (including root)
 * @param {Object} [params.edges]                        // optional override of default EDGES
 * @param {('id'|'lastUpdated'|null)} [params.sortBy='id'] // sort materialised items within each type
 * @param {1|-1} [params.sortDir=1]
 * @returns {import('mongodb').Document[]}
 */
function buildMaterialisedListsPipelineTotalMax({ rootType, rootExternalKey, targetTypes, totalMax, edges, sortBy = 'id', sortDir = 1 }) {
	if (!rootType || !rootExternalKey) throw new Error('rootType and rootExternalKey are required');
	if (!Array.isArray(targetTypes) || targetTypes.length === 0) throw new Error('targetTypes must be a non-empty array');
	if (!Number.isInteger(totalMax) || totalMax < 0) throw new Error('totalMax must be a non-negative integer');

	//////////////////////////////////////////////////////////////////////////////
	// Directed, field-labeled graph of materialised edges (extend as needed)
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

	//////////////////////////////////////////////////////////////////////////////
	// 1) Compute shortest paths (by hop count) from root → each target
	const pathsByTarget = {};
	for (const t of targetTypes) {
		// p is an array of the joins between path elements, e.g.
		// [ { from: "competition", field: "stages", to: "stage"}, { from: "stage", field: "events", to: "event"} ]
		const p = findPath(EDGES, rootType, t);
		if (p === null) throw new Error(`No materialised path from '${rootType}' to '${t}'. Add an edge if it should exist.`);
		pathsByTarget[t] = p; // [] means root == target
	}

	//////////////////////////////////////////////////////////////////////////////
	// 2) Merge paths into unique traversal steps (shared hops computed once)
	const steps = planSteps(Object.values(pathsByTarget));

	//////////////////////////////////////////////////////////////////////////////
	// 3) Build pipeline
	const stages = [
		{ $match: { resourceType: rootType, externalKey: rootExternalKey } },
		{ $addFields: { _rootKey: '$externalKey' } },
		// Always expose the root's own id as an array for uniform handling
		{ $addFields: { _rootIds: [`$${idField}`] } },
	];

	//////////////////////////////////////////////////////////////////////////////
	// Compute each unique step once and store its output in a named field.
	for (const step of steps) {
		const { from, field, dependsOnKey, outputName, depth } = step;

		if (depth === 0) {
			// Seed from ROOT: read the first-hop array right off the root doc
			stages.push({ $addFields: { [outputName]: { $ifNull: [`$${field}`, []] } } });
		} else {
			const prev = steps.find((s) => s.key === dependsOnKey);
			if (!prev) throw new Error(`Internal: missing dependency for step ${step.key}`);
			const prevOutput = prev.outputName;
			const lkField = `${outputName}__lk`;

			stages.push({
				$lookup: {
					from: collection,
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
	// 4) Budget allocation (root first, then targetTypes order)
	// We'll compute included/overflow per type step-by-step, consuming 'remaining'.
	stages.push({ $addFields: { _remaining: totalMax } });

	//////////////////////////////////////////////////////////////////////////////
	// Root allocation
	// include 1 root if remaining > 0; else overflow contains the rootId
	stages.push({
		$addFields: {
			_rootIncludedIds: {
				$cond: [{ $gt: ['$_remaining', 0] }, { $slice: ['$_rootIds', 1] }, []],
			},
			_rootOverflowIds: {
				$cond: [
					{ $gt: ['$_remaining', 0] },
					{ $slice: ['$_rootIds', 0] }, // empty
					'$_rootIds', // all rootIds overflow if budget 0
				],
			},
			_remaining: {
				$cond: [{ $gt: ['$_remaining', 0] }, { $subtract: ['$_remaining', 1] }, '$_remaining'],
			},
		},
	});

	//////////////////////////////////////////////////////////////////////////////
	// For each target type, pick its final ID array source,
	// then allocate from _remaining in the order provided.
	const finalArrayFieldByType = {};
	for (const t of targetTypes) {
		const path = pathsByTarget[t];
		let sourceFieldExpr;

		if (path.length === 0) {
			// root == target → use rootIds
			sourceFieldExpr = '$_rootIds';
		} else {
			const lastHop = path[path.length - 1];
			const lastKey = makeKey(lastHop.from, lastHop.field, lastHop.to);
			const lastStep = steps.find((s) => s.key === lastKey);
			if (!lastStep) throw new Error(`Internal: cannot find step for '${t}'`);
			sourceFieldExpr = `$${lastStep.outputName}`;
		}

		// Store source for later materialisation
		const idsVar = `_src_${t}_ids`;
		finalArrayFieldByType[t] = idsVar;

		// Attach the source ids for this type
		stages.push({ $addFields: { [idsVar]: sourceFieldExpr } });

		// Allocate budget for this type (included / overflow)
		const includedVar = `_inc_${t}_ids`;
		const overflowVar = `_ovf_${t}_ids`;

		stages.push({
			$addFields: {
				[includedVar]: {
					$let: {
						vars: { sz: { $size: `$${idsVar}` } },
						in: {
							$cond: [
								{ $gt: ['$_remaining', 0] },
								{
									$slice: [`$${idsVar}`, { $min: ['$_remaining', '$$sz'] }],
								},
								[],
							],
						},
					},
				},
				[overflowVar]: {
					$cond: [
						{ $gt: ['$_remaining', 0] },
						{
							$let: {
								vars: {
									take: { $min: ['$_remaining', { $size: `$${idsVar}` }] },
								},
								in: {
									$slice: [`$${idsVar}`, '$$take', { $subtract: [{ $size: `$${idsVar}` }, '$$take'] }],
								},
							},
						},
						`$${idsVar}`, // if remaining == 0, all overflow
					],
				},
				_remaining: {
					$let: {
						vars: {
							take: { $min: ['$_remaining', { $size: `$${idsVar}` }] },
						},
						in: { $subtract: ['$_remaining', '$$take'] },
					},
				},
			},
		});
	}

	//////////////////////////////////////////////////////////////////////////////
	// 5) Materialise per type (root + each target) using a single $facet
	// Sorting within each type is optional (id or lastUpdated or none)
	const sortStageFor = (type) => {
		if (sortBy === 'lastUpdated') return [{ $sort: { lastUpdated: sortDir } }];
		if (sortBy === 'id') return [{ $sort: { [idField]: sortDir } }];
		return [];
	};

	const facet = {};

	//////////////////////////////////////////////////////////////////////////////
	// Root type facet
	facet[rootType] = [
		{ $project: { includedIds: '$_rootIncludedIds', overflowIds: '_rootOverflowIds' } },
		{
			$lookup: {
				from: collection,
				let: { ids: '$includedIds' },
				pipeline: [{ $match: { resourceType: rootType } }, { $match: { $expr: { $in: [`$${idField}`, '$$ids'] } } }, ...sortStageFor(rootType)],
				as: 'docs',
			},
		},
		{ $replaceWith: { items: '$docs', overflow: { resourceType: rootType, overflowIds: '$overflowIds' } } },
	];

	//////////////////////////////////////////////////////////////////////////////
	// Target types facets
	for (const t of targetTypes) {
		const includedVar = `_inc_${t}_ids`;
		const overflowVar = `_ovf_${t}_ids`;

		facet[t] = [
			{ $project: { includedIds: `$${includedVar}`, overflowIds: `$${overflowVar}` } },
			{
				$lookup: {
					from: collection,
					let: { ids: '$includedIds' },
					pipeline: [{ $match: { resourceType: t } }, { $match: { $expr: { $in: [`$${idField}`, '$$ids'] } } }, ...sortStageFor(t)],
					as: 'docs',
				},
			},
			{ $replaceWith: { items: '$docs', overflow: { overflowIds: '$overflowIds' } } },
		];
	}

	stages.push({ $facet: facet });

	//////////////////////////////////////////////////////////////////////////////
	// 6) Final shape: always include keys for rootType + every target type
	const resultsProjection = {};
	const allTypes = [rootType, ...targetTypes];
	for (const t of allTypes) {
		resultsProjection[t] = {
			$ifNull: [{ $arrayElemAt: [`$${t}`, 0] }, { items: [], overflow: { overflowIds: [] } }],
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

////////////////////////////////////////////////////////////////////////////////
/** ===== helper graph functions (same as the optimised version) ===== */

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

/* =========================
Example usage:

const { MongoClient } = require('mongodb');
const { buildMaterialisedListsPipelineTotalMax } = require('./materialiser.totalMax');

(async () => {
  const mongo = await MongoClient.connect(process.env.MONGO_URI);
  const coll = mongo.db().collection('materialisedAggregations');

  // Example: totalMax = 4
  // Allocation order = root (competition) → stages → events → teams
  const pipeline = buildMaterialisedListsPipelineTotalMax({
    rootType: 'competition',
    rootExternalKey: '289175 @ fifa',
    targetTypes: ['stage', 'event', 'team'],  // order controls who gets the remaining budget first
    totalMax: 4,
    idField: 'gamedayId',
    // sortBy: 'lastUpdated', sortDir: -1,
  });

  const [result] = await coll.aggregate(pipeline, { allowDiskUse: true }).toArray();
  console.log(JSON.stringify(result, null, 2));
  await mongo.close();
})();

========================= */

/*
Indexing (once):

db.materialisedAggregations.createIndex({ resourceType: 1, externalKey: 1 });  // root lookup
db.materialisedAggregations.createIndex({ resourceType: 1, gamedayId: 1 });   // joins/materialise
// If sorting by lastUpdated:
db.materialisedAggregations.createIndex({ resourceType: 1, lastUpdated: -1, gamedayId: 1 });
*/
