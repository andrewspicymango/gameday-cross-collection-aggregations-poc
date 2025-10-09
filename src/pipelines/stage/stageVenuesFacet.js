const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
/**
 * Aggregation facet pipeline for resolving venue references for a given stage.
 *
 * This pipeline is intended to be executed in the context of a stage document that exposes
 * the following fields on the current pipeline input doc:
 *   - _externalId
 *   - _externalIdScope
 *
 * Behaviour summary:
 *   1. Build the stage key string using the global `keySeparator` value:
 *      "<_externalId><keySeparator><_externalIdScope>".
 *   2. Lookup events (collection: "events") whose derived stage key
 *      ("_externalStageId<keySeparator>_externalStageIdScope") matches this stage's key.
 *   3. From each matched event, extract venue information if both _externalVenueId and
 *      _externalVenueIdScope are non-empty strings. Create a pair object: { id, scope, key }
 *      where key is "<id><keySeparator><scope>". Null/invalid venues are dropped.
 *   4. Reduce all venue arrays across matched events into a single unique set of pair objects
 *      (unique by the full pair object using $setUnion) and limit to a single aggregated document.
 *   5. Lookup venue documents (collection: "venues") using the derived pair keys,
 *      matching on "<_externalId><keySeparator><_externalIdScope>". Project their _id and key.
 *   6. Project a final object containing:
 *        - ids: unique set of matched venues._id (typically ObjectId)
 *        - venueKeys: unique set of external key strings ("<id><keySeparator><scope>") for the venues
 *
 * Important notes / assumptions:
 *   - A variable keySeparator must be available in the outer JS scope where this pipeline is defined;
 *     it is used to compose the stable composite keys in all lookups.
 *   - Only events with string-typed, non-empty _externalVenueId and _externalVenueIdScope are considered.
 *   - Uniqueness is enforced via $setUnion at multiple stages; the final projection produces
 *     de-duplicated arrays.
 *   - If no matches are found at any stage, the pipeline yields empty arrays for both ids and venueKeys.
 *
 * Result shape (per stage input document):
 *   {
 *     ids:       [ /* Array of venues._id values (unique) *\/ ],
 *     venueKeys: [ /* Array of "<id><keySeparator><scope>" strings (unique) *\/ ]
 *   }
 *
 * Collections referenced:
 *   - "events"
 *   - "venues"
 *
 * Usage:
 *   - Can be used as a facet or inline pipeline stage when aggregating stages to resolve
 *     the set of venue references associated with the stage's events.
 *
 * @constant
 * @type {Array<Object>}
 * @name stageVenuesFacet
 */
const stageVenuesFacet = [
	{ $addFields: { stageKey: { $concat: ['$_externalId', keySeparator, '$_externalIdScope'] } } },
	{
		$lookup: {
			from: 'events',
			let: { stageKey: '$stageKey' },
			pipeline: [
				{ $match: { $expr: { $eq: [{ $concat: ['$_externalStageId', keySeparator, '$_externalStageIdScope'] }, '$$stageKey'] } } },
				{
					$project: {
						pairs: {
							$cond: [
								{
									$and: [
										{ $eq: [{ $type: '$_externalVenueId' }, 'string'] },
										{ $ne: ['$_externalVenueId', ''] },
										{ $eq: [{ $type: '$_externalVenueIdScope' }, 'string'] },
										{ $ne: ['$_externalVenueIdScope', ''] },
									],
								},
								[
									{
										id: '$_externalVenueId',
										scope: '$_externalVenueIdScope',
										key: { $concat: ['$_externalVenueId', keySeparator, '$_externalVenueIdScope'] },
									},
								],
								[],
							],
						},
					},
				},
				{ $group: { _id: null, arrs: { $push: '$pairs' } } },
				{ $project: { _id: 0, pairs: { $reduce: { input: '$arrs', initialValue: [], in: { $setUnion: ['$$value', '$$this'] } } } } },
				{ $limit: 1 },
			],
			as: 'venueAgg',
		},
	},
	{ $project: { venuePairs: { $ifNull: [{ $getField: { field: 'pairs', input: { $first: '$venueAgg' } } }, []] } } },
	{
		$lookup: {
			from: 'venues',
			let: { vKeys: { $setUnion: [{ $map: { input: '$venuePairs', as: 'v', in: '$$v.key' } }, []] } },
			pipeline: [
				{ $match: { $expr: { $and: [{ $gt: [{ $size: '$$vKeys' }, 0] }, { $in: [{ $concat: ['$_externalId', keySeparator, '$_externalIdScope'] }, '$$vKeys'] }] } } },
				{ $project: { _id: 1, key: { $concat: ['$_externalId', keySeparator, '$_externalIdScope'] } } },
			],
			as: 'venuesHit',
		},
	},
	{
		$project: {
			_id: 0,
			ids: { $setUnion: [{ $map: { input: '$venuesHit', as: 'v', in: '$$v._id' } }, []] },
			keys: { $setUnion: [{ $map: { input: '$venuesHit', as: 'v', in: '$$v.key' } }, []] },
		},
	},
];

////////////////////////////////////////////////////////////////////////////////
exports.stageVenuesFacet = stageVenuesFacet;
