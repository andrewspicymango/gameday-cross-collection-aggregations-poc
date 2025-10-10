const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
/**
 * Team Staff Facet
 *
 * Aggregation facet pipeline for resolving staff references for a given team.
 *
 * This pipeline is intended to be executed in the context of a team document that exposes
 * the following fields on the current pipeline input doc:
 *   - _externalId
 *   - _externalIdScope
 *
 * Behaviour summary:
 *   1. Build the team key string using the global `keySeparator` value:
 *      "<_externalId><keySeparator><_externalIdScope>".
 *   2. Lookup staff documents (collection: "staff") whose derived team key
 *      ("_externalTeamId<keySeparator>_externalTeamIdScope") matches this team's key.
 *   3. From each matched staff document, create a staff key using all 4 required properties:
 *      "<_externalTeamId><keySeparator><_externalTeamIdScope><keySeparator><_externalSportsPersonId><keySeparator><_externalSportsPersonIdScope>".
 *      Only staff with all 4 non-empty string properties are included.
 *   4. Project a final object containing:
 *        - ids: unique set of matched staff._id (typically ObjectId)
 *        - keys: unique set of composite staff key strings
 *
 * Important notes / assumptions:
 *   - A variable keySeparator must be available in the outer JS scope where this pipeline is defined;
 *     it is used to compose the stable composite keys in all lookups.
 *   - Only staff with string-typed, non-empty _externalTeamId, _externalTeamIdScope,
 *     _externalSportsPersonId, and _externalSportsPersonIdScope are considered.
 *   - Staff documents are keyed by 4 properties, not the standard 2 (_externalId/_externalIdScope).
 *   - Uniqueness is enforced via $setUnion; the final projection produces de-duplicated arrays.
 *   - If no matches are found, the pipeline yields empty arrays for both ids and keys.
 *
 * Result shape (per team input document):
 *   {
 *     ids: [ \/* Array of staff._id values (unique) *\/ ],
 *     keys: [ \/* Array of composite staff key strings (unique) *\/ ]
 *   }
 *
 * Collections referenced:
 *   - "staff"
 *
 * Usage:
 *   - Can be used as a facet or inline pipeline stage when aggregating teams to resolve
 *     the set of staff references associated with the team.
 *
 * @constant
 * @type {Array<Object>}
 * @name teamStaffFacet
 */
const teamStaffFacet = [
	// Build the team key for matching
	{ $addFields: { teamKey: { $concat: ['$_externalId', keySeparator, '$_externalIdScope'] } } },

	// Lookup staff that reference this team
	{
		$lookup: {
			from: 'staff',
			let: { teamKey: '$teamKey' },
			pipeline: [
				{ $match: { $expr: { $eq: [{ $concat: ['$_externalTeamId', keySeparator, '$_externalTeamIdScope'] }, '$$teamKey'] } } },
				{
					$project: {
						_id: 1,
						staffKey: {
							$cond: [
								{
									$and: [
										{ $eq: [{ $type: '$_externalTeamId' }, 'string'] },
										{ $ne: ['$_externalTeamId', ''] },
										{ $eq: [{ $type: '$_externalTeamIdScope' }, 'string'] },
										{ $ne: ['$_externalTeamIdScope', ''] },
										{ $eq: [{ $type: '$_externalSportsPersonId' }, 'string'] },
										{ $ne: ['$_externalSportsPersonId', ''] },
										{ $eq: [{ $type: '$_externalSportsPersonIdScope' }, 'string'] },
										{ $ne: ['$_externalSportsPersonIdScope', ''] },
									],
								},
								{
									$concat: [
										'$_externalTeamId',
										keySeparator,
										'$_externalTeamIdScope',
										keySeparator,
										'$_externalSportsPersonId',
										keySeparator,
										'$_externalSportsPersonIdScope',
									],
								},
								null,
							],
						},
					},
				},
				// Filter out staff with incomplete keys
				{
					$match: {
						staffKey: { $ne: null },
					},
				},
			],
			as: 'staffDocs',
		},
	},

	// Final projection with unique staff IDs and keys
	{
		$project: {
			_id: 0,
			ids: { $setUnion: [{ $map: { input: '$staffDocs', as: 's', in: '$$s._id' } }, []] },
			keys: { $setUnion: [{ $map: { input: '$staffDocs', as: 's', in: '$$s.staffKey' } }, []] },
		},
	},
];

////////////////////////////////////////////////////////////////////////////////
exports.teamStaffFacet = teamStaffFacet;
