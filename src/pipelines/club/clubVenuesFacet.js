const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
/**
 * Aggregation facet pipeline that resolves a club's related venue documents and derives
 * normalized identifier collections.
 *
 * Stages:
 * 1. $lookup (pipeline form) against the "venues" collection matching both _externalId
 *    and _externalIdScope to the club's corresponding external venue fields. Projects
 *    only {_id, _externalId, _externalIdScope} into "venues".
 * 2. $project builds:
 *    - ids: a de-duplicated array (set) of matched venue ObjectIds.
 *    - keys: a de-duplicated array of composite string keys combining each venue's
 *      externalId, a keySeparator (from outer scope), and its externalIdScope.
 *
 * When no venues match, ids and keys become empty arrays. Designed for reuse inside
 * a larger aggregation (e.g., $facet) to supply venue reference metadata.
 *
 * @constant
 * @type {import('mongodb').Document[]} MongoDB aggregation pipeline stages.
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/lookup/
 */
const clubVenuesFacet = [
	{
		$lookup: {
			from: 'venues',
			let: { vid: '$_externalVenueId', vids: '$_externalVenueIdScope' },
			pipeline: [
				{ $match: { $expr: { $and: [{ $eq: ['$_externalId', '$$vid'] }, { $eq: ['$_externalIdScope', '$$vids'] }] } } },
				{ $project: { _id: 1, _externalId: 1, _externalIdScope: 1 } },
			],
			as: 'venues',
		},
	},
	{
		$project: {
			ids: { $setUnion: [{ $map: { input: '$venues', as: 'v', in: '$$v._id' } }, []] },
			keys: { $setUnion: [{ $map: { input: '$venues', as: 'v', in: { $concat: ['$$v._externalId', keySeparator, '$$v._externalIdScope'] } } }, []] },
		},
	},
];

exports.clubVenuesFacet = clubVenuesFacet;
