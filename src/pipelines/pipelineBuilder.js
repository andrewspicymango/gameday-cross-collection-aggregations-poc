// pipelineBuilder.js
/**
 * Build a pipeline that:
 *  - loads the root doc (resourceType + externalKey),
 *  - walks materialised relations to collect target ObjectIds,
 *  - returns a list of fully materialised target docs (limited),
 *  - plus an overflow doc with remaining ObjectIds.
 *
 * @param {Object} params
 * @param {string} params.rootType              e.g. "competition"
 * @param {string} params.rootExternalKey       e.g. "289175 @ fifa"
 * @param {string} params.targetType            e.g. "event" | "team" | "venue" | "club" | "sportsPerson" | "stage" | "sgo"
 * @param {number} params.maxCount              e.g. 100
 * @param {string} [params.collection="materialisedAggregations"]
 * @returns {import('mongodb').Document[]} aggregation pipeline
 */
function buildMaterialisedListPipeline({ rootType, rootExternalKey, targetType, maxCount, collection = 'materialisedAggregations' }) {
	if (!rootType || !rootExternalKey || !targetType || !Number.isInteger(maxCount) || maxCount < 0) {
		throw new Error('rootType, rootExternalKey, targetType, and non-negative integer maxCount are required');
	}

	// Describe traversable edges using your materialised arrays (from doc → field → next type)
	// Derived from your sample docs:
	// - competition.stages -> stage
	// - stage.events -> event
	// - event.teams -> team, event.venues -> venue, event.sportsPersons -> sportsPerson
	// - team.clubs -> club
	const EDGES = {
		competition: { stages: 'stage' },
		stage: { events: 'event' },
		event: { teams: 'team', venues: 'venue', sportsPersons: 'sportsPerson' },
		team: { clubs: 'club' },
		// add more here if/when you materialise additional relations
	};

	// Find the hop path from rootType to targetType using BFS over EDGES
	const path = findPath(EDGES, rootType, targetType);
	if (!path) {
		throw new Error(`No materialised path found from '${rootType}' to '${targetType}'. Extend EDGES if needed.`);
	}
	// path = array of hops like: [{ from:'competition', field:'stages', to:'stage' }, { from:'stage', field:'events', to:'event' }, ...]

	// Build the hop lookups. We keep aggregating ObjectIds set → next set.
	// Each hop becomes a $lookup that:
	//   - filters documents by _id ∈ $$ids AND resourceType == <from>
	//   - projects the "next" field (array of ObjectIds)
	//   - unwinds and groups inside the subpipeline to return a single doc { ids: [distinct OIDs] }
	// Then we pull that array back up with $addFields.
	const stages = [];

	// 1) Load the root by { resourceType, externalKey }.
	stages.push(
		{ $match: { resourceType: rootType, externalKey: rootExternalKey } },
		// For the very first "ids", if there are no hops (root == target), we will just read the root itself.
		{ $addFields: { _rootKey: '$externalKey' } }
	);

	// If root == target, the "targetIds" are just [root._id]
	if (path.length === 0) {
		stages.push({ $project: { targetIds: ['$_id'], _rootKey: 1 } });
	} else {
		// Seed ids from the first hop field on the root doc
		const firstHop = path[0]; // e.g. competition.stages
		stages.push({ $addFields: { ids0: { $ifNull: [`$${firstHop.field}`, []] } } });

		// For subsequent hops, chain lookups
		path.forEach((hop, i) => {
			if (i === 0) return; // already seeded from root
			const prev = `ids${i - 1}`;
			const cur = `ids${i}`;
			const lkField = `hop${i}`;

			stages.push({
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
			stages.push({
				$addFields: {
					[cur]: {
						$ifNull: [{ $arrayElemAt: [`$${lkField}.ids`, 0] }, []],
					},
				},
			});
		});

		// targetIds are from the last hop step:
		const lastIdx = path.length - 1;
		stages.push({ $addFields: { targetIds: `$ids${lastIdx}` } });
	}

	// 3) Limit & overflow
	stages.push({
		$addFields: {
			includedIds: { $slice: ['$targetIds', maxCount] },
			overflowIds: { $setDifference: ['$targetIds', { $slice: ['$targetIds', maxCount] }] },
		},
	});

	// 4) Fetch the materialised target documents + build the overflow doc
	stages.push({
		$facet: {
			items: [
				{
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
							// Optional: stable order by _id ascending for determinism
							{ $sort: { _id: 1 } },
						],
						as: 'docs',
					},
				},
				{ $replaceWith: '$docs' }, // emit the array itself
			],
			overflow: [
				{
					$project: {
						_id: 0,
						resourceType: { $literal: targetType },
						overflowIds: '$overflowIds',
					},
				},
			],
		},
	});

	// 5) Flatten the facet into a single response document:
	stages.push({
		$project: {
			resourceType: { $literal: targetType },
			root: { type: { $literal: rootType }, externalKey: '$_rootKey' },
			items: { $ifNull: [{ $arrayElemAt: ['$items', 0] }, []] },
			overflow: { $ifNull: [{ $arrayElemAt: ['$overflow', 0] }, { resourceType: targetType, overflowIds: [] }] },
		},
	});

	return stages;
}

/**
 * Find a shortest hop-path from 'startType' to 'endType' using EDGES.
 * Returns array of hops like: [{from, field, to}, ...]
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

module.exports = { buildMaterialisedListPipeline };
