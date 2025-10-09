const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
/**
 * Aggregation facet pipeline for resolving sports-person references for a given stage.
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
 *   3. From each matched event, extract participant entries that have BOTH non-empty string values
 *      for _externalSportsPersonId and _externalSportsPersonIdScope. For each valid participant
 *      create a pair object: { id, scope, key } where key is "<id><keySeparator><scope>".
 *      Null / invalid participants are dropped.
 *   4. Reduce all participant arrays across matched events into a single unique set of pair objects
 *      (unique by the full pair object using $setUnion) and limit to a single aggregated document.
 *   5. Lookup sports-person documents (collection: "sportsPersons") using the derived pair keys,
 *      matching on "<_externalId><keySeparator><_externalIdScope>". Project their _id and key.
 *   6. Project a final object containing:
 *        - ids: unique set of matched sportsPersons._id (typically ObjectId)
 *        - keys: unique set of external key strings ("<id><keySeparator><scope>") for the sports-persons
 *
 * Important notes / assumptions:
 *   - A variable keySeparator must be available in the outer JS scope where this pipeline is defined;
 *     it is used to compose the stable composite keys in all lookups.
 *   - Only participant entries with string-typed, non-empty _externalSportsPersonId and
 *     _externalSportsPersonIdScope are considered.
 *   - Uniqueness is enforced via $setUnion at multiple stages; the final projection produces
 *     de-duplicated arrays.
 *   - If no matches are found at any stage, the pipeline yields empty arrays for both ids and keys.
 *
 * Result shape (per stage input document):
 *   {
 *     ids:   [ \/* Array of sportsPersons._id values (unique) *\/ ],
 *     keys:  [ \/* Array of "<id><keySeparator><scope>" strings (unique) *\/ ]
 *   }
 *
 *
 * Usage:
 *   - Can be used as a facet or inline pipeline stage when aggregating stages to resolve
 *     the set of sports-person references associated with the stage's events.
 *
 * @constant
 * @type {Array<Object>}
 * @name stageSportsPersonsFacet
 */
const stageSportsPersonsFacet = [
	{ $addFields: { stageKey: { $concat: ['$_externalId', keySeparator, '$_externalIdScope'] } } },
	{
		$lookup: {
			from: 'events',
			let: { stageKey: '$stageKey' },
			pipeline: [
				{ $match: { $expr: { $eq: [{ $concat: ['$_externalStageId', keySeparator, '$_externalStageIdScope'] }, '$$stageKey'] } } },
				{
					$project: {
						sportsPersonParticipants: {
							$setUnion: [
								{
									$map: {
										input: { $ifNull: ['$participants', []] },
										as: 'p',
										in: {
											$cond: [
												{
													$and: [
														{ $eq: [{ $type: '$$p._externalSportsPersonId' }, 'string'] },
														{ $ne: ['$$p._externalSportsPersonId', ''] },
														{ $eq: [{ $type: '$$p._externalSportsPersonIdScope' }, 'string'] },
														{ $ne: ['$$p._externalSportsPersonIdScope', ''] },
													],
												},
												{
													id: '$$p._externalSportsPersonId',
													scope: '$$p._externalSportsPersonIdScope',
													key: { $concat: ['$$p._externalSportsPersonId', keySeparator, '$$p._externalSportsPersonIdScope'] },
												},
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
				{ $project: { sportsPersonParticipants: { $filter: { input: '$sportsPersonParticipants', as: 'sp', cond: { $ne: ['$$sp', null] } } } } },
				{ $group: { _id: null, arrs: { $push: '$sportsPersonParticipants' } } },
				{ $project: { _id: 0, pairs: { $reduce: { input: '$arrs', initialValue: [], in: { $setUnion: ['$$value', '$$this'] } } } } },
				{ $limit: 1 },
			],
			as: 'eventSportsAgg',
		},
	},
	{ $project: { sportsPairs: { $ifNull: [{ $getField: { field: 'pairs', input: { $first: '$eventSportsAgg' } } }, []] } } },
	{
		$lookup: {
			from: 'sportsPersons',
			let: { spKeys: { $setUnion: [{ $map: { input: '$sportsPairs', as: 'sp', in: '$$sp.key' } }, []] } },
			pipeline: [
				{ $match: { $expr: { $and: [{ $gt: [{ $size: '$$spKeys' }, 0] }, { $in: [{ $concat: ['$_externalId', keySeparator, '$_externalIdScope'] }, '$$spKeys'] }] } } },
				{ $project: { _id: 1, key: { $concat: ['$_externalId', keySeparator, '$_externalIdScope'] } } },
			],
			as: 'sportsHits',
		},
	},
	{
		$project: {
			_id: 0,
			ids: { $setUnion: [{ $map: { input: '$sportsHits', as: 'h', in: '$$h._id' } }, []] },
			keys: { $setUnion: [{ $map: { input: '$sportsHits', as: 'sp', in: '$$sp.key' } }, []] },
		},
	},
];

exports.stageSportsPersonsFacet = stageSportsPersonsFacet;
