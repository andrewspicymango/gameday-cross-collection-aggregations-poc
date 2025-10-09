const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
/**
 * stageSgoFacet
 *
 * Aggregation pipeline (array of stages) that resolves related "sgo" documents for an input document
 * by following competition -> sgoMemberships -> sgos. Intended to be used as a facet/inner pipeline
 * (for example inside a $facet or as a sub-pipeline for another aggregation stage).
 *
 * Behavior summary:
 *  - Expects the input document to contain:
 *      - _externalCompetitionId         (string) external id of a competition
 *      - _externalCompetitionIdScope    (string) external id scope of that competition
 *  - Performs a $lookup into the "competitions" collection to find the matching competition
 *    (matching both external id and external id scope).
 *  - From the competition document, normalizes sgoMemberships to an array and maps each membership
 *    to a { id, scope, key } pair where:
 *      - id    = membership._externalSgoId
 *      - scope = membership._externalSgoIdScope
 *      - key   = id + keySeparator + scope
 *    Only membership entries where both id and scope exist and are non-empty strings are kept.
 *  - Uses the constructed keys to $lookup into the "sgos" collection (matching by the concatenation
 *    of each sgo's _externalId + keySeparator + _externalIdScope).
 *  - Produces a final projection with two de-duplicated sets:
 *      - ids : unique array of matching sgo _id values (ObjectId)
 *      - keys: unique array of matching sgo key strings (externalId + keySeparator + externalIdScope)
 *
 * Important notes / requirements:
 *  - A variable named `keySeparator` must be available in the aggregation scope (string). It is
 *    used to build/compare composite keys (externalId + keySeparator + externalIdScope).
 *  - If no competition or no memberships are found, the pipeline yields empty arrays for both ids
 *    and keys.
 *  - The pipeline uses $limit:1 after extracting pairs from competitions to only consider the first
 *    matching competition document.
 *
 * Output shape (per input document):
 *  {
 *    ids:  [ /* unique ObjectId values of matched sgos *\/ ],
 *    keys: [ /* unique composite key strings of matched sgos *\/ ]
 *  }
 *
 * Type:
 *  @constant {Array<Object>} stageSgoFacet
 */
const stageSgoFacet = [
	{
		$lookup: {
			from: 'competitions',
			let: { cid: '$_externalCompetitionId', cs: '$_externalCompetitionIdScope' },
			pipeline: [
				{ $match: { $expr: { $and: [{ $eq: ['$_externalId', '$$cid'] }, { $eq: ['$_externalIdScope', '$$cs'] }] } } },
				{ $project: { _id: 0, sgoMemberships: { $ifNull: ['$sgoMemberships', []] } } },
				{
					$project: {
						pairs: {
							$setUnion: [
								{
									$map: {
										input: '$sgoMemberships',
										as: 'm',
										in: {
											$cond: [
												{
													$and: [
														{ $eq: [{ $type: '$$m._externalSgoId' }, 'string'] },
														{ $ne: ['$$m._externalSgoId', ''] },
														{ $eq: [{ $type: '$$m._externalSgoIdScope' }, 'string'] },
														{ $ne: ['$$m._externalSgoIdScope', ''] },
													],
												},
												{ id: '$$m._externalSgoId', scope: '$$m._externalSgoIdScope', key: { $concat: ['$$m._externalSgoId', keySeparator, '$$m._externalSgoIdScope'] } },
												null,
											],
										},
									},
								},
								[],
							],
						},
					},
				},
				{ $project: { pairs: { $filter: { input: '$pairs', as: 'p', cond: { $ne: ['$$p', null] } } } } },
				{ $limit: 1 },
			],
			as: 'compAgg',
		},
	},
	{ $project: { sgoPairs: { $ifNull: [{ $getField: { field: 'pairs', input: { $first: '$compAgg' } } }, []] } } },
	{
		$lookup: {
			from: 'sgos',
			let: { sgoKeys: { $setUnion: [{ $map: { input: '$sgoPairs', as: 'p', in: '$$p.key' } }, []] } },
			pipeline: [
				{ $match: { $expr: { $and: [{ $gt: [{ $size: '$$sgoKeys' }, 0] }, { $in: [{ $concat: ['$_externalId', keySeparator, '$_externalIdScope'] }, '$$sgoKeys'] }] } } },
				{ $project: { _id: 1, key: { $concat: ['$_externalId', keySeparator, '$_externalIdScope'] } } },
			],
			as: 'sgoHits',
		},
	},
	{
		$project: {
			_id: 0,
			ids: { $setUnion: [{ $map: { input: '$sgoHits', as: 'h', in: '$$h._id' } }, []] },
			keys: { $setUnion: [{ $map: { input: '$sgoHits', as: 'p', in: '$$p.key' } }, []] },
		},
	},
];

////////////////////////////////////////////////////////////////////////////////
exports.stageSgoFacet = stageSgoFacet;
