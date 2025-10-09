// competitionSportsPersonsFacet.js
const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
/**
 * Aggregation facet pipeline for resolving sports-person references for a given competition.
 *
 * This pipeline is intended to be executed in the context of a competition document that exposes
 * the following fields on the current pipeline input doc:
 *   - _externalId
 *   - _externalIdScope
 *
 * Behaviour summary:
 *   1. Lookup matching stage documents (collection: "stages") for the competition by comparing
 *      stages' _externalCompetitionId and _externalCompetitionIdScope to the competition's
 *      _externalId and _externalIdScope. Project a derived stage key string using a global
 *      `keySeparator` value: "<_externalId><keySeparator><_externalIdScope>".
 *   2. Collect unique stage keys (setUnion).
 *   3. Lookup events (collection: "events") whose derived stage key
 *      ("_externalStageId<keySeparator>_externalStageIdScope") is in the set of stage keys.
 *   4. From each matched event, extract participant entries that have BOTH non-empty string values
 *      for _externalSportsPersonId and _externalSportsPersonIdScope. For each valid participant
 *      create a pair object: { id, scope, key } where key is "<id><keySeparator><scope>".
 *      Null / invalid participants are dropped.
 *   5. Reduce all participant arrays across matched events into a single unique set of pair objects
 *      (unique by the full pair object using $setUnion) and limit to a single aggregated document.
 *   6. Lookup sports-person documents (collection: "sportsPersons") using the derived pair keys,
 *      matching on "<_externalId><keySeparator><_externalIdScope>". Project their _id and key.
 *   7. Project a final object containing:
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
 * Result shape (per competition input document):
 *   {
 *     ids:   [ \/* Array of sportsPersons._id values (unique) *\/ ],
 *     keys:  [ \/* Array of "<id><keySeparator><scope>" strings (unique) *\/ ]
 *   }
 *
 * Collections referenced:
 *   - "stages"
 *   - "events"
 *   - "sportsPersons"
 *
 * Usage:
 *   - Can be used as a facet or inline pipeline stage when aggregating competitions to resolve
 *     the set of sports-person references associated with the competition's events.
 *
 * @constant
 * @type {Array<Object>}
 * @name competitionSportsPersonsFacet
 */
const competitionSportsPersonsFacet = [
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
	{
		$project: {
			stageKeys: { $setUnion: [{ $map: { input: '$stages', as: 's', in: '$$s.stageKey' } }, []] },
		},
	},
	{
		$lookup: {
			from: 'events',
			let: { stageKeys: '$stageKeys' },
			pipeline: [
				{
					$match: {
						$expr: {
							$and: [{ $gt: [{ $size: '$$stageKeys' }, 0] }, { $in: [{ $concat: ['$_externalStageId', keySeparator, '$_externalStageIdScope'] }, '$$stageKeys'] }],
						},
					},
				},
				{
					// Keep only sports-person entries with BOTH non-empty strings
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
												// else: drop (null)
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
				// Remove nulls from the mapped array
				{ $project: { sportsPersonParticipants: { $filter: { input: '$sportsPersonParticipants', as: 'sp', cond: { $ne: ['$$sp', null] } } } } },
				// Reduce all arrays from matched events into a single unique set
				{ $group: { _id: null, arrs: { $push: '$sportsPersonParticipants' } } },
				{ $project: { _id: 0, pairs: { $reduce: { input: '$arrs', initialValue: [], in: { $setUnion: ['$$value', '$$this'] } } } } },
				{ $limit: 1 },
			],
			as: 'eventSportsAgg',
		},
	},
	{
		$project: {
			sportsPairs: {
				$ifNull: [{ $getField: { field: 'pairs', input: { $first: '$eventSportsAgg' } } }, []],
			},
		},
	},
	{
		$lookup: {
			from: 'sportsPersons',
			let: { spKeys: { $setUnion: [{ $map: { input: '$sportsPairs', as: 'sp', in: '$$sp.key' } }, []] } },
			pipeline: [
				{
					$match: {
						$expr: {
							$and: [{ $gt: [{ $size: '$$spKeys' }, 0] }, { $in: [{ $concat: ['$_externalId', keySeparator, '$_externalIdScope'] }, '$$spKeys'] }],
						},
					},
				},
				{ $project: { _id: 1, key: { $concat: ['$_externalId', keySeparator, '$_externalIdScope'] } } },
			],
			as: 'sportsHits',
		},
	},
	{
		$project: {
			_id: 0,
			ids: { $setUnion: [{ $map: { input: '$sportsHits', as: 'h', in: '$$h._id' } }, []] },
			keys: { $setUnion: [{ $map: { input: '$sportsPairs', as: 'sp', in: '$$sp.key' } }, []] },
		},
	},
];

exports.competitionSportsPersonsFacet = competitionSportsPersonsFacet;
