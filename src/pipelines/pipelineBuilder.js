// pipelineBuilder.js

// Example: From a COMPETITION → fetch EVENTS + TEAMS + SGOS
const pipeline = buildMaterialisedListsPipeline({
	rootType: 'competition',
	rootExternalKey: '289175 @ fifa',
	targetTypes: ['event'], // 'team', 'sgo'],
	maxCount: 100, // per-type default limit
	perTypeMax: { team: 50 }, // optional overrides
});

////////////////////////////////////////////////////////////////////////////////
/**
 * Build a MongoDB aggregation pipeline that:
 *  - loads a root doc (resourceType + externalKey) from 'materialisedAggregations',
 *  - for EACH requested targetType, walks the materialised edges to collect target ObjectIds,
 *  - returns, per targetType:
 *      items: fully materialised docs (limited),
 *      overflow: { resourceType, overflowIds: [...] } for IDs beyond the limit.
 *
 * Output shape:
 * [
 *   {
 *     root: { type, externalKey },
 *     results: {
 *       <targetType1>: { items: [...], overflow: { resourceType: <t1>, overflowIds: [...] } },
 *       <targetType2>: { ... },
 *       ...
 *     }
 *   }
 * ]
 *
 * @param {Object} params
 * @param {string} params.rootType
 * @param {string} params.rootExternalKey
 * @param {string[]} params.targetTypes                  // one or more
 * @param {number} params.maxCount                       // limit applied to every target type
 * @param {Object} [params.perTypeMax]                   // optional overrides, e.g. { event: 200, team: 50 }
 * @param {string} [params.collection='materialisedAggregations']
 * @returns {import('mongodb').Document[]} aggregation pipeline
 */
function buildMaterialisedListsPipeline({ rootType, rootExternalKey, targetTypes, maxCount, perTypeMax = {}, collection = 'materialisedAggregations' }) {
	if (!rootType || !rootExternalKey) throw new Error('rootType and rootExternalKey are required');
	if (!Array.isArray(targetTypes) || targetTypes.length === 0) throw new Error('targetTypes must be a non-empty array');
	if (!Number.isInteger(maxCount) || maxCount < 0) throw new Error('maxCount must be a non-negative integer');

	// Directed, field-based edges, derived from your materialised docs.
	// (You can extend any time without changing the rest of the code.)
	// competition.sgos <-> sgo.competitions; competition.stages <-> stage.competitions
	// stage.events <-> event.stages
	// event.teams | event.venues | event.sportsPersons
	// team.clubs | team.events | team.nations
	// venue.events
	// club.teams
	const EDGES = {
		competition: { stages: 'stage', sgos: 'sgo' },
		stage: { events: 'event', competitions: 'competition' },
		event: { teams: 'team', venues: 'venue', sportsPersons: 'sportsPerson', stages: 'stage' },
		team: { clubs: 'club', events: 'event', nations: 'nation' },
		venue: { events: 'event' },
		club: { teams: 'team' },
		sgo: { competitions: 'competition' },
		nation: { teams: 'team' }, // assume materialised; safe even if empty
	};

	const stages = [{ $match: { resourceType: rootType, externalKey: rootExternalKey } }, { $addFields: { _rootKey: '$externalKey' } }];

	// Build a facet branch for every requested targetType.
	const facet = {};
	for (const tgt of targetTypes) {
		const path = findPath(EDGES, rootType, tgt);
		if (!path) throw new Error(`No materialised path from '${rootType}' to '${tgt}'. Add an edge if this should exist.`);

		const limit = Number.isInteger(perTypeMax[tgt]) ? perTypeMax[tgt] : maxCount;
		facet[tgt] = buildBranchPipeline({ path, targetType: tgt, limit, collection });
	}

	stages.push({ $facet: facet });

	// Reshape: each facet key holds an array with a single {items, overflow} object
	const resultsProjection = {};
	for (const tgt of targetTypes) {
		resultsProjection[tgt] = { $ifNull: [{ $arrayElemAt: [`$${tgt}`, 0] }, { items: [], overflow: { resourceType: tgt, overflowIds: [] } }] };
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
 * Build the per-targetType facet branch.
 * - Walks the hop path to produce targetIds
 * - Splits into includedIds/overflowIds by `limit`
 * - Looks up fully materialised docs for includedIds
 * - Emits one object: { items: [...], overflow: { resourceType, overflowIds } }
 */
function buildBranchPipeline({ path, targetType, limit, collection }) {
	const branch = [];

	if (path.length === 0) {
		// Root == Target
		branch.push({ $project: { targetIds: ['$_id'] } });
	} else {
		// Seed from root (first hop’s field lives on the root doc)
		const first = path[0];
		branch.push({ $project: { ids0: { $ifNull: [`$${first.field}`, []] } } });

		// For subsequent hops, chain lookups from the previous id set
		for (let i = 1; i < path.length; i++) {
			const hop = path[i]; // { from, field, to }
			const prev = `ids${i - 1}`;
			const cur = `ids${i}`;
			const lkField = `hop${i}`;

			branch.push({
				$lookup: {
					from: collection,
					let: { ids: `$${prev}` },
					pipeline: [
						{
							$match: {
								$expr: {
									$and: [{ $in: ['$_id', '$$ids'] }, { $eq: ['$resourceType', hop.from] }],
								},
							},
						},
						{ $project: { nextIds: { $ifNull: [`$${hop.field}`, []] } } },
						{ $unwind: { path: '$nextIds', preserveNullAndEmptyArrays: false } },
						{ $group: { _id: null, ids: { $addToSet: '$nextIds' } } },
					],
					as: lkField,
				},
			});
			branch.push({
				$addFields: {
					[cur]: { $ifNull: [{ $arrayElemAt: [`$${lkField}.ids`, 0] }, []] },
				},
			});
		}
		branch.push({ $project: { targetIds: { $ifNull: [`$ids${path.length - 1}`, []] } } });
	}

	// Include / overflow
	branch.push({
		$addFields: {
			includedIds: { $slice: ['$targetIds', limit] },
			overflowIds: { $setDifference: ['$targetIds', { $slice: ['$targetIds', limit] }] },
		},
	});

	// Fetch fully materialised docs for this target type
	branch.push({
		$lookup: {
			from: collection,
			let: { ids: '$includedIds' },
			pipeline: [
				{
					$match: {
						$expr: {
							$and: [{ $in: ['$_id', '$$ids'] }, { $eq: ['$resourceType', targetType] }],
						},
					},
				},
				{ $sort: { _id: 1 } },
			],
			as: 'docs',
		},
	});

	// Emit a single object for this facet branch
	branch.push({
		$replaceWith: {
			items: '$docs',
			overflow: { resourceType: targetType, overflowIds: '$overflowIds' },
		},
	});

	return branch;
}

////////////////////////////////////////////////////////////////////////////////
/**
 * BFS over field-based edges; returns [{from, field, to}, ...]
 */
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

module.exports = { buildMaterialisedListsPipeline };
