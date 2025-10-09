const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
/**
 * stageEventsFacet
 *
 * MongoDB aggregation stages that attach related event identifiers and deduplicated composite keys
 * for a stage document by performing a correlated lookup into the "events" collection.
 *
 * Behavior:
 * - $lookup
 *   - from: "events"
 *   - let: { sid: '$_externalId', ss: '$_externalIdScope' }
 *   - pipeline: matches event documents where
 *       $_externalStageId == $$sid AND $_externalStageIdScope == $$ss
 *     and projects only the fields: _id, _externalId, _externalIdScope
 *
 * - $project
 *   - ids: a deduplicated array of event _id values. Computed as:
 *       $setUnion of (map over the looked-up events returning $$e._id) and an empty array,
 *     which ensures uniqueness and returns [] when there are no events.
 *   - keys: a deduplicated array of composite string keys for each event. Computed as:
 *       $setUnion of (map over the looked-up events returning the concatenation
 *       of $$e._externalId, keySeparator, and $$e._externalIdScope) and an empty array.
 *     Note: keySeparator must be defined in the surrounding scope (e.g. a variable injected
 *     into the aggregation context) and should be a string.
 *
 * Output shape (per input document):
 * {
 *   ids: Array<Any>      // typically ObjectId values from events._id, deduplicated
 *   keys: Array<string>  // deduplicated composite keys: "<eventExternalId><sep><eventScope>"
 * }
 *
 * Requirements / assumptions:
 * - Input documents must have _externalId and _externalIdScope fields that identify the stage.
 * - The events collection uses $_externalStageId and $_externalStageIdScope to reference stages.
 * - keySeparator is expected to be available and is used to build the composite key string.
 *
 * Intended use:
 * - Include this array of stages inside an aggregation pipeline (e.g. as part of a $facet
 *   or spliced into a pipeline for stage documents) to efficiently gather related event ids
 *   and keys for each stage record.
 *
 * @type {Array<Object>}
 */
const stageEventsFacet = [
	{
		$lookup: {
			from: 'events',
			let: { sid: '$_externalId', ss: '$_externalIdScope' },
			pipeline: [
				{
					$match: {
						$expr: {
							$and: [{ $eq: ['$_externalStageId', '$$sid'] }, { $eq: ['$_externalStageIdScope', '$$ss'] }],
						},
					},
				},
				{ $project: { _id: 1, _externalId: 1, _externalIdScope: 1 } },
			],
			as: 'events',
		},
	},
	{
		$project: {
			ids: { $setUnion: [{ $map: { input: '$events', as: 'e', in: '$$e._id' } }, []] },
			keys: { $setUnion: [{ $map: { input: '$events', as: 'e', in: { $concat: ['$$e._externalId', keySeparator, '$$e._externalIdScope'] } } }, []] },
		},
	},
];

////////////////////////////////////////////////////////////////////////////////
exports.stageEventsFacet = stageEventsFacet;
