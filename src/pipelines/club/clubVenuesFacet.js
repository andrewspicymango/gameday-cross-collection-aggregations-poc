const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////

/**
 * MongoDB aggregation facet that performs a lookup to join club venues with venue documents.
 *
 * @description This facet:
 * 1. Performs a $lookup to match venues by external ID and scope
 * 2. Creates a compound key from venue ID and scope with a separator
 * 3. Projects the results into two formats:
 *    - `ids`: Array of unique venue ObjectIds
 *    - `keys`: Object mapping compound keys to venue ObjectIds
 *
 * @type {Array<Object>} MongoDB aggregation pipeline stages
 *
 * @example
 * // Input document structure expected:
 * // { _externalVenueId: "venue123", _externalVenueIdScope: "scope1" }
 *
 * // Output structure:
 * // { ids: [ObjectId("...")], keys: { "venue123|scope1": ObjectId("...") } }
 */
const clubVenuesFacet = [
	{
		$lookup: {
			from: 'venues',
			let: { targetVenueId: '$_externalVenueId', targetVenueIdScope: '$_externalVenueIdScope' },
			pipeline: [
				{ $match: { $expr: { $and: [{ $eq: ['$_externalId', '$$targetVenueId'] }, { $eq: ['$_externalIdScope', '$$targetVenueIdScope'] }] } } },
				{ $project: { _id: 1, _externalId: 1, _externalIdScope: 1 } },
				{ $set: { key: { $concat: ['$$targetVenueId', keySeparator, '$$targetVenueIdScope'] } } },
			],
			as: 'venues',
		},
	},
	{
		$project: {
			ids: { $setUnion: [{ $map: { input: '$venues', as: 'v', in: '$$v._id' } }, []] },
			keys: { $cond: [{ $gt: [{ $size: '$venues' }, 0] }, { $arrayToObject: { $map: { input: '$venues', as: 's', in: ['$$s.key', '$$s._id'] } } }, {}] },
		},
	},
];

exports.clubVenuesFacet = clubVenuesFacet;
