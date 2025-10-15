const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
/**
 * MongoDB aggregation pipeline facet that performs a lookup to find all events
 * associated with a stage and transforms the results into convenient formats.
 *
 * The facet:
 * 1. Looks up events from the 'events' collection matching the stage's external ID and scope
 * 2. Projects only essential fields (_id, _externalId, _externalIdScope) from matched events
 * 3. Creates a composite key by concatenating externalId and externalIdScope
 * 4. Returns two outputs:
 *    - `ids`: Array of event ObjectIds for the stage
 *    - `keys`: Object mapping composite keys to ObjectIds (empty object if no events)
 *
 * @type {Array<Object>} MongoDB aggregation pipeline stages
 */
const stageEventsFacet = [
	{
		$lookup: {
			from: 'events',
			let: { thisStageId: '$_externalId', thisStageIdScope: '$_externalIdScope' },
			pipeline: [
				{ $match: { $expr: { $and: [{ $eq: ['$_externalStageId', '$$thisStageId'] }, { $eq: ['$_externalStageIdScope', '$$thisStageIdScope'] }] } } },
				{ $project: { _id: 1, _externalId: 1, _externalIdScope: 1 } },
				{ $set: { key: { $concat: ['$_externalId', keySeparator, '$_externalIdScope'] } } },
			],
			as: 'events',
		},
	},
	{
		$project: {
			ids: { $setUnion: [{ $map: { input: '$events', as: 'e', in: '$$e._id' } }, []] },
			keys: { $cond: [{ $gt: [{ $size: '$events' }, 0] }, { $arrayToObject: { $map: { input: '$events', as: 's', in: ['$$s.key', '$$s._id'] } } }, {}] },
		},
	},
];

////////////////////////////////////////////////////////////////////////////////
exports.stageEventsFacet = stageEventsFacet;
