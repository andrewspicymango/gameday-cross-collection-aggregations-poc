const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
/**
 * MongoDB aggregation pipeline facet for looking up venue data from events.
 *
 * This facet performs a cross-collection lookup to join events with their corresponding
 * venues based on external venue ID and scope matching. It transforms the results into
 * both an array of venue IDs and a key-value mapping for efficient access.
 *
 * @type {Array<Object>} MongoDB aggregation pipeline stages
 *
 * Pipeline stages:
 * 1. $lookup - Joins with 'venues' collection matching _externalVenueId and _externalVenueIdScope
 * 2. $project - Transforms results into 'ids' array and 'keys' object
 *
 * Output format:
 * - ids: Array of venue ObjectIds that match the event
 * - keys: Object mapping venue keys (externalId + separator + scope) to venue ObjectIds
 *
 * @example
 * // Result structure:
 * {
 *   ids: [ObjectId("..."), ObjectId("...")],
 *   keys: { "venue123__scope1": ObjectId("...") }
 * }
 */
const eventVenuesFacet = [
	{
		$lookup: {
			from: 'venues',
			let: { targetVenueId: '$_externalVenueId', targetVenueIdScope: '$_externalVenueIdScope' },
			pipeline: [
				{ $match: { $expr: { $and: [{ $eq: ['$_externalId', '$$targetVenueId'] }, { $eq: ['$_externalIdScope', '$$targetVenueIdScope'] }] } } },
				{ $project: { _id: 1, _externalId: 1, _externalIdScope: 1 } },
				{ $set: { key: { $concat: ['$_externalId', keySeparator, '$_externalIdScope'] } } },
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

exports.eventVenuesFacet = eventVenuesFacet;
