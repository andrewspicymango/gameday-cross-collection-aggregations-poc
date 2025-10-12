// pipelineBuilder.js
////////////////////////////////////////////////////////////////////////////////
// Example: From a COMPETITION → fetch EVENTS + TEAMS + SGOS
const pipeline = buildMaterialisedListsPipeline({
	rootType: 'competition',
	rootExternalKey: '289175 @ fifa',
	targetTypes: ['event'], // 'team', 'sgo'],
	maxCount: 100, // per-type default limit
	perTypeMax: { team: 50 }, // optional overrides
});
////////////////////////////////////////////////////////////////////////////////
const idField = 'gamedayId';
const collection = 'materialisedAggregations';

////////////////////////////////////////////////////////////////////////////////
// optimisedMaterialiser.js
// Build a single aggregation pipeline that:
//  - Loads a root doc by { resourceType, externalKey }
//  - Computes all shared traversal hops only once (across multiple targets)
//  - For each requested target type, limits, overflows, and materialises docs
//
// Exports:
//   buildMaterialisedListsPipelineOptimised(params) -> aggregation pipeline array
//
// Usage example is at the end of this file.

////////////////////////////////////////////////////////////////////////////////
/**
 * Build an optimised, shared-hop MongoDB aggregation pipeline.
 *
 * @param {Object} params
 * @param {string} params.rootType                       // e.g. "competition"
 * @param {string} params.rootExternalKey                // e.g. "289175 @ fifa"
 * @param {string[]} params.targetTypes                  // e.g. ["event","team","sgo"]
 * @param {number} params.maxCount                       // default per-target limit
 * @param {Object} [params.perTypeMax={}]                // e.g. { team: 50 }
 * @param {Object} [params.edges]                        // optional override of default EDGES
 * @param {('id'|'lastUpdated'|null)} [params.sortBy='id'] // materialised item sort: 'id' | 'lastUpdated' | null
 * @param {1|-1} [params.sortDir=1]                      // 1 asc, -1 desc
 * @returns {import('mongodb').Document[]}
 */
function buildMaterialisedListsPipelineOptimised({ rootType, rootExternalKey, targetTypes, maxCount, perTypeMax = {}, edges, sortBy = 'id', sortDir = 1 }) {
	//////////////////////////////////////////////////////////////////////////////
	// ---- validation
	if (!rootType || !rootExternalKey) throw new Error('rootType and rootExternalKey are required');
	if (!Array.isArray(targetTypes) || targetTypes.length === 0) throw new Error('targetTypes must be a non-empty array');
	if (!Number.isInteger(maxCount) || maxCount < 0) throw new Error('maxCount must be a non-negative integer');

	//////////////////////////////////////////////////////////////////////////////
	// ---- default EDGES (directed, field -> neighbor type)
	// Add/adjust here as your materialised docs evolve.
	const EDGES = edges || {
		// competition
		competition: { stages: 'stage', sgos: 'sgo' },

		// stage
		stage: { events: 'event', competitions: 'competition' },

		// event
		event: { teams: 'team', venues: 'venue', sportsPersons: 'sportsPerson', stages: 'stage' },

		// team
		team: { clubs: 'club', events: 'event', nations: 'nation' },

		// venue
		venue: { events: 'event' },

		// club
		club: { teams: 'team' },

		// sgo
		sgo: { competitions: 'competition' },

		// nation
		nation: { teams: 'team' },
	};

	//////////////////////////////////////////////////////////////////////////////
	// ---- 1) Compute a shortest hop path per target
	const pathsByTarget = {};
	for (const t of targetTypes) {
		const path = findPath(EDGES, rootType, t);
		if (path === null) {
			throw new Error(`No materialised path from '${rootType}' to '${t}'. Add an edge if this should exist.`);
		}
		pathsByTarget[t] = path; // [] means root == target
	}
	//////////////////////////////////////////////////////////////////////////////
	// ---- 2) Merge paths into unique steps (prefix-merged traversal)
	const steps = planSteps(Object.values(pathsByTarget));

	//////////////////////////////////////////////////////////////////////////////
	// ---- 3) Build pipeline
	const stages = [{ $match: { resourceType: rootType, externalKey: rootExternalKey } }, { $addFields: { _rootKey: '$externalKey' } }];

	// Compute each unique step once and store its output in a named field.
	// Step output naming is deterministic and reused.
	for (const step of steps) {
		const { key, from, field, to, dependsOnKey, depth, outputName } = step;

		if (depth === 0) {
			// First hop from ROOT: read array directly off the root doc
			stages.push({
				$addFields: { [outputName]: { $ifNull: [`$${field}`, []] } },
			});
		} else {
			// Dependent hop: use the previous step's output as $$ids
			const prev = steps.find((s) => s.key === dependsOnKey);
			if (!prev) throw new Error(`Internal: missing dependency for step ${key}`);
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
			stages.push({
				$addFields: { [outputName]: { $ifNull: [{ $arrayElemAt: [`$${lkField}.ids`, 0] }, []] } },
			});
		}
	}

	//////////////////////////////////////////////////////////////////////////////
	// ---- 4) Per-target: take its final ID set, limit/overflow, and materialise docs
	const facet = {};
	for (const t of targetTypes) {
		const path = pathsByTarget[t];
		const limit = Number.isInteger(perTypeMax[t]) ? perTypeMax[t] : maxCount;

		let sourceArrayExpr;
		if (path.length === 0) {
			// Root == target → single id -> wrap as array
			// (in aggregation, ["$field"] is valid and resolves to the field value)
			sourceArrayExpr = [`$${idField}`];
		} else {
			const lastHop = path[path.length - 1];
			const lastKey = makeKey(lastHop.from, lastHop.field, lastHop.to);
			const lastStep = steps.find((s) => s.key === lastKey);
			if (!lastStep) throw new Error(`Internal: cannot find step for target '${t}'`);
			sourceArrayExpr = `$${lastStep.outputName}`;
		}

		const sortStage = sortBy === 'lastUpdated' ? [{ $sort: { lastUpdated: sortDir } }] : sortBy === 'id' ? [{ $sort: { [idField]: sortDir } }] : []; // no sorting

		facet[t] = [
			{ $project: { _ids: sourceArrayExpr } },
			{
				$addFields: {
					includedIds: { $slice: ['$_ids', limit] },
					overflowIds: { $setDifference: ['$_ids', { $slice: ['$_ids', limit] }] },
				},
			},
			{
				$lookup: {
					from: collection,
					let: { ids: '$includedIds' },
					pipeline: [{ $match: { resourceType: t } }, { $match: { $expr: { $in: [`$${idField}`, '$$ids'] } } }, ...sortStage],
					as: 'docs',
				},
			},
			{ $replaceWith: { items: '$docs', overflow: { resourceType: t, overflowIds: '$overflowIds' } } },
		];
	}

	stages.push({ $facet: facet });

	//////////////////////////////////////////////////////////////////////////////
	// ---- 5) Final response shape
	const resultsProjection = {};
	for (const t of targetTypes) {
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

////////////////////////////////////////////////////////////////////////////////
/**
 * Produce a shortest hop path (by number of edges) from startType to endType
 * over a directed, field-labeled graph.
 *
 * @param {Record<string, Record<string, string>>} EDGES
 * @param {string} startType
 * @param {string} endType
 * @returns {Array<{from:string, field:string, to:string}>|[]|null}
 */
function findPath(EDGES, startType, endType) {
	if (startType === endType) return [];

	// BFS over types (nodes). Edges carry the "field" label.
	const queue = [[startType, []]];
	const visited = new Set([startType]);

	while (queue.length) {
		const [cur, path] = queue.shift();
		const outs = EDGES[cur] || {};
		// Optionally stabilise order:
		// const ordered = Object.entries(outs).sort(([a],[b]) => a.localeCompare(b));
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
/**
 * Given multiple hop paths, return a deduped, dependency-ordered list of steps.
 * Each step:
 *  - key:        "from.field->to" (unique ID)
 *  - from, field, to: hop metadata
 *  - depth:      index within its path (0 for first hop from the root)
 *  - dependsOnKey: key of the previous hop in that path (null for depth 0)
 *  - outputName: stable field name to store this step's resulting ID array
 *
 * @param {Array<Array<{from:string, field:string, to:string}>>} paths
 * @returns {Array<{key:string, from:string, field:string, to:string, depth:number, dependsOnKey:string|null, outputName:string}>}
 */
function planSteps(paths) {
	const seen = new Map(); // key -> step
	const steps = [];

	for (const path of paths) {
		for (let i = 0; i < path.length; i++) {
			const hop = path[i];
			const key = makeKey(hop.from, hop.field, hop.to);
			const dependsOnKey = i > 0 ? makeKey(path[i - 1].from, path[i - 1].field, path[i - 1].to) : null;

			if (!seen.has(key)) {
				const outputName = `${hop.to}Ids__${hashKey(key)}__d${i}`;
				const step = {
					key,
					from: hop.from,
					field: hop.field,
					to: hop.to,
					depth: i,
					dependsOnKey,
					outputName,
				};
				seen.set(key, step);
				steps.push(step);
			}
			// If we've seen it, we keep the first-seen definition;
			// later occurrences will depend on the same key and reuse the same output field.
		}
	}

	// Ensure parent steps appear before dependents (simple topological pass).
	// Because each step depends on the immediate previous hop, sorting by depth then key is sufficient.
	steps.sort((a, b) => a.depth - b.depth || a.key.localeCompare(b.key));
	return steps;
}

////////////////////////////////////////////////////////////////////////////////
/** Build a unique key for a hop. */
function makeKey(from, field, to) {
	return `${from}.${field}->${to}`;
}

////////////////////////////////////////////////////////////////////////////////
/** Simple deterministic hash for readable, unique-ish output field names. */
function hashKey(s) {
	// DJB2-ish
	let h = 5381;
	for (let i = 0; i < s.length; i++) h = ((h << 5) + h) ^ s.charCodeAt(i);
	return (h >>> 0).toString(36);
}

////////////////////////////////////////////////////////////////////////////////
module.exports = {
	buildMaterialisedListsPipelineOptimised,
	findPath, // exported for testing
	planSteps, // exported for testing
	makeKey, // exported for testing
	hashKey, // exported for testing
};

/* =======================
   Example usage:

const { MongoClient } = require('mongodb');
const { buildMaterialisedListsPipelineOptimised } = require('./optimisedMaterialiser');

(async () => {
  const mongo = await MongoClient.connect(process.env.MONGO_URI);
  const coll = mongo.db().collection('materialisedAggregations');

  const pipeline = buildMaterialisedListsPipelineOptimised({
    rootType: 'competition',
    rootExternalKey: '289175 @ fifa',
    targetTypes: ['event', 'team', 'sgo'],
    maxCount: 100,
    perTypeMax: { team: 50 },
    idField: 'gamedayId',
    // sortBy: 'lastUpdated', sortDir: -1, // optional
  });

  const [result] = await coll.aggregate(pipeline, { allowDiskUse: true }).toArray();

  console.log(JSON.stringify(result, null, 2));
  await mongo.close();
})();

   ======================= */

/*
Indexing checklist (run once):

db.materialisedAggregations.createIndex({ resourceType: 1, externalKey: 1 });  // root match
db.materialisedAggregations.createIndex({ resourceType: 1, gamedayId: 1 });   // joins & materialise lookups
// Optional if sorting by lastUpdated frequently:
db.materialisedAggregations.createIndex({ resourceType: 1, lastUpdated: -1, gamedayId: 1 });
*/
