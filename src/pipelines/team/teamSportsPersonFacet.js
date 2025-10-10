const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
/**
 * Team Sports Persons Facet
 *
 * Aggregation facet pipeline for resolving sports person references for a given team.
 *
 * This pipeline is intended to be executed in the context of a team document that exposes
 * the following fields on the current pipeline input doc:
 *   - members (array of objects with _externalSportsPersonId and _externalSportsPersonIdScope)
 *
 * Behaviour summary:
 *   1. Extract sports person information from the team's members array if both
 *      _externalSportsPersonId and _externalSportsPersonIdScope are non-empty strings.
 *      Create pair objects: { id, scope, key } where key is "<id><keySeparator><scope>".
 *      Null/invalid sports persons are dropped.
 *   2. Create a unique set of sports person pairs using $setUnion to eliminate duplicates.
 *   3. Lookup sports person documents (collection: "sportsPersons") using the derived pair keys,
 *      matching on "<_externalId><keySeparator><_externalIdScope>". Project their _id and key.
 *   4. Project a final object containing:
 *        - ids: unique set of matched sportsPersons._id (typically ObjectId)
 *        - keys: unique set of external key strings ("<id><keySeparator><scope>") for the sports persons
 *
 * Important notes / assumptions:
 *   - A variable keySeparator must be available in the outer JS scope where this pipeline is defined;
 *     it is used to compose the stable composite keys in all lookups.
 *   - Only member entries with string-typed, non-empty _externalSportsPersonId and
 *     _externalSportsPersonIdScope are considered.
 *   - Uniqueness is enforced via $setUnion at multiple stages; the final projection produces
 *     de-duplicated arrays.
 *   - If no matches are found, the pipeline yields empty arrays for both ids and keys.
 *
 * Result shape (per team input document):
 *   {
 *     ids: [ \/* Array of sportsPersons._id values (unique) *\/ ],
 *     keys: [ \/* Array of "<id><keySeparator><scope>" strings (unique) *\/ ]
 *   }
 *
 * Collections referenced:
 *   - "sportsPersons"
 *
 * Usage:
 *   - Can be used as a facet or inline pipeline stage when aggregating teams to resolve
 *     the set of sports person references associated with the team's members.
 *
 * @constant
 * @type {Array<Object>}
 * @name teamSportsPersonFacet
 */
const teamSportsPersonFacet = [
	{
		$project: {
			sportsPersonParticipants: {
				$setUnion: [
					{
						$map: {
							input: { $ifNull: ['$members', []] },
							as: 'm',
							in: {
								$cond: [
									{
										$and: [
											{ $eq: [{ $type: '$$m._externalSportsPersonId' }, 'string'] },
											{ $ne: ['$$m._externalSportsPersonId', ''] },
											{ $eq: [{ $type: '$$m._externalSportsPersonIdScope' }, 'string'] },
											{ $ne: ['$$m._externalSportsPersonIdScope', ''] },
										],
									},
									{
										id: '$$m._externalSportsPersonId',
										scope: '$$m._externalSportsPersonIdScope',
										key: { $concat: ['$$m._externalSportsPersonId', keySeparator, '$$m._externalSportsPersonIdScope'] },
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

	// Remove nulls from the mapped array
	{
		$project: {
			sportsPersonParticipants: {
				$filter: {
					input: '$sportsPersonParticipants',
					as: 'sp',
					cond: { $ne: ['$$sp', null] },
				},
			},
		},
	},

	{
		$lookup: {
			from: 'sportsPersons',
			let: { spKeys: { $setUnion: [{ $map: { input: '$sportsPersonParticipants', as: 'sp', in: '$$sp.key' } }, []] } },
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
			keys: { $setUnion: [{ $map: { input: '$sportsPersonParticipants', as: 'sp', in: '$$sp.key' } }, []] },
		},
	},
];

////////////////////////////////////////////////////////////////////////////////
module.exports = { teamSportsPersonFacet };
