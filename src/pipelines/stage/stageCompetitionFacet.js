const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
/**
 * MongoDB aggregation facet that looks up competition documents and creates ID mappings.
 *
 * This facet performs a lookup operation to find competitions that match the stage's
 * external competition ID and scope, then creates two output formats:
 * - `ids`: Array of ObjectIds for matched competitions
 * - `keys`: Object mapping competition keys (externalId + scope) to their ObjectIds
 *
 * The lookup matches competitions where:
 * - Competition's _externalId equals stage's _externalCompetitionId
 * - Competition's _externalIdScope equals stage's _externalCompetitionIdScope
 *
 * @type {Array<Object>} MongoDB aggregation pipeline stages
 *
 * @example
 * // Input stage document:
 * { _externalCompetitionId: "comp123", _externalCompetitionIdScope: "source1" }
 *
 * // Output:
 * {
 *   ids: [ObjectId("..."), ObjectId("...")],
 *   keys: { "comp123|source1": ObjectId("...") }
 * }
 */
const stageCompetitionFacet = [
	{
		$lookup: {
			from: 'competitions',
			let: { targetCompetitionId: '$_externalCompetitionId', targetCompetitionIdScope: '$_externalCompetitionIdScope' },
			pipeline: [
				{
					$match: {
						$expr: {
							$and: [{ $eq: ['$_externalId', '$$targetCompetitionId'] }, { $eq: ['$_externalIdScope', '$$targetCompetitionIdScope'] }],
						},
					},
				},
				{ $project: { _id: 1, _externalId: 1, _externalIdScope: 1 } },
				{ $set: { key: { $concat: ['$_externalId', keySeparator, '$_externalIdScope'] } } },
			],
			as: 'competitions',
		},
	},
	{
		$project: {
			ids: { $setUnion: [{ $map: { input: '$competitions', as: 'c', in: '$$c._id' } }, []] },
			keys: { $cond: [{ $gt: [{ $size: '$competitions' }, 0] }, { $arrayToObject: { $map: { input: '$competitions', as: 's', in: ['$$s.key', '$$s._id'] } } }, {}] },
		},
	},
];

////////////////////////////////////////////////////////////////////////////////
exports.stageCompetitionFacet = stageCompetitionFacet;
