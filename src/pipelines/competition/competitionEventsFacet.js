const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
/**
 * eventFacet
 *
 * MongoDB aggregation $facet pipeline fragment that, for a competition document,
 * resolves related stage and event identifiers and keys.
 *
 * Pipeline behavior:
 * 1) $lookup from "stages"
 *    - Joins stages where:
 *        stage._externalCompetitionId == source._externalId
 *        and stage._externalCompetitionIdScope == source._externalIdScope
 *    - Projects a synthetic stageKey = concat(stage._externalId, keySeparator, stage._externalIdScope)
 * 2) $project stageKeys
 *    - Builds a deduplicated array of stageKey strings (uses $map + $setUnion)
 * 3) $lookup from "events"
 *    - Matches events whose concatenated stage id ( _externalStageId + keySeparator + _externalStageIdScope )
 *      is contained in stageKeys. This match is guarded by a $gt check to ensure stageKeys is non-empty.
 *    - Projects each matched event to { _id, key } where key = concat(event._externalId, keySeparator, event._externalIdScope)
 * 4) $project final output
 *    - ids: deduplicated array of event ObjectId values (extracted from matched events)
 *    - keys: deduplicated array of event key strings
 *
 * Notes:
 * - Depends on an outer-scope string variable `keySeparator`.
 * - Expects source documents to contain `_externalId` and `_externalIdScope`.
 * - Referenced collections: "stages" and "events".
 * - Deduplication is achieved with $setUnion; union with an empty array ensures safe handling of empty inputs.
 * - Result shape produced by the facet: { ids: Array<ObjectId>, keys: Array<string> }
 *
 * @type {Array<Object>}
 */
const competitionEventsFacet = [
	{
		$lookup: {
			from: 'stages',
			let: { cid: '$_externalId', cs: '$_externalIdScope' },
			pipeline: [
				{ $match: { $expr: { $and: [{ $eq: ['$_externalCompetitionId', '$$cid'] }, { $eq: ['$_externalCompetitionIdScope', '$$cs'] }] } } },
				{ $project: { _id: 0, stageKey: { $concat: ['$_externalId', keySeparator, '$_externalIdScope'] } } },
			],
			as: 'stages',
		},
	},
	{ $project: { stageKeys: { $setUnion: [{ $map: { input: '$stages', as: 's', in: '$$s.stageKey' } }, []] } } },
	{
		$lookup: {
			from: 'events',
			let: { stageKeys: '$stageKeys' },
			pipeline: [
				{
					$match: {
						$expr: { $and: [{ $gt: [{ $size: '$$stageKeys' }, 0] }, { $in: [{ $concat: ['$_externalStageId', keySeparator, '$_externalStageIdScope'] }, '$$stageKeys'] }] },
					},
				},
				{ $project: { _id: 1, key: { $concat: ['$_externalId', keySeparator, '$_externalIdScope'] } } },
			],
			as: 'events',
		},
	},
	{
		$project: {
			ids: { $setUnion: [{ $map: { input: '$events', as: 'e', in: '$$e._id' } }, []] },
			keys: { $setUnion: [{ $map: { input: '$events', as: 'e', in: '$$e.key' } }, []] },
		},
	},
];
exports.competitionEventsFacet = competitionEventsFacet;
