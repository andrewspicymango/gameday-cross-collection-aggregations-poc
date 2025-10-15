const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
/**
 * MongoDB aggregation pipeline facet for looking up and processing stage data related to events.
 *
 * This facet performs a lookup operation to find stages that match the event's external stage ID
 * and scope, then transforms the results into two useful formats:
 * - `ids`: An array of MongoDB ObjectIds for the matched stages
 * - `keys`: An object mapping composite keys (externalId + separator + scope) to stage ObjectIds
 *
 * The lookup uses the following matching criteria:
 * - `$_externalStageId` from the event matches `$_externalId` in stages collection
 * - `$_externalStageIdScope` from the event matches `$_externalIdScope` in stages collection
 *
 * @type {Array<Object>} MongoDB aggregation pipeline stages
 * @requires keySeparator - External variable used to construct composite keys
 *
 * @example
 * // Usage in aggregation pipeline
 * db.events.aggregate([
 *   { $facet: { eventStages: eventStageFacet } }
 * ])
 */
const eventStageFacet = [
	{
		$lookup: {
			from: 'stages',
			let: { targetStageId: '$_externalStageId', targetStageIdScope: '$_externalStageIdScope' },
			pipeline: [
				{ $match: { $expr: { $and: [{ $eq: ['$_externalId', '$$targetStageId'] }, { $eq: ['$_externalIdScope', '$$targetStageIdScope'] }] } } },
				{ $project: { _id: 1, _externalId: 1, _externalIdScope: 1 } },
				{ $set: { key: { $concat: ['$_externalId', keySeparator, '$_externalIdScope'] } } },
			],
			as: 'stages',
		},
	},
	{
		$project: {
			ids: { $setUnion: [{ $map: { input: '$stages', as: 's', in: '$$s._id' } }, []] },
			keys: { $cond: [{ $gt: [{ $size: '$stages' }, 0] }, { $arrayToObject: { $map: { input: '$stages', as: 's', in: ['$$s.key', '$$s._id'] } } }, {}] },
		},
	},
];

exports.eventStageFacet = eventStageFacet;
