const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
/**
 * SGO Clubs Facet
 *
 * Aggregation facet pipeline for resolving club references for a given SGO.
 *
 * This pipeline is intended to be executed in the context of an SGO document that exposes
 * the following fields on the current pipeline input doc:
 *   - _externalId
 *   - _externalIdScope
 *
 * Behaviour summary:
 *   1. Build the SGO key string using the global `keySeparator` value:
 *      "<_externalId><keySeparator><_externalIdScope>".
 *   2. Lookup club documents (collection: "clubs") that have this SGO
 *      in their sgoMemberships array by matching the derived SGO key.
 *   3. From each matched club, create club pairs with id, scope, and composite key.
 *      Only clubs with valid _externalId and _externalIdScope are included.
 *   4. Project a final object containing:
 *        - ids: unique set of matched clubs._id (typically ObjectId)
 *        - keys: unique set of external key strings ("<id><keySeparator><scope>") for the clubs
 *
 * Important notes / assumptions:
 *   - A variable keySeparator must be available in the outer JS scope where this pipeline is defined;
 *     it is used to compose the stable composite keys in all lookups.
 *   - Only clubs with string-typed, non-empty _externalId and _externalIdScope are considered.
 *   - SGO membership is determined by matching SGO keys in the sgoMemberships array.
 *   - Uniqueness is enforced via $setUnion; the final projection produces de-duplicated arrays.
 *   - If no matches are found, the pipeline yields empty arrays for both ids and keys.
 *
 * Result shape (per SGO input document):
 *   {
 *     ids: [ /* Array of clubs._id values (unique) *\/ ],
 *     keys: [ /* Array of "<id><keySeparator><scope>" strings (unique) *\/ ]
 *   }
 *
 * Collections referenced:
 *   - "clubs"
 *
 * Usage:
 *   - Can be used as a facet or inline pipeline stage when aggregating SGOs to resolve
 *     the set of club references where the SGO has membership.
 *
 * @constant
 * @type {Array<Object>}
 * @name sgoClubsFacet
 */
const sgoClubsFacet = [
	// Build the SGO key for matching
	{ $addFields: { sgoKey: { $concat: ['$_externalId', keySeparator, '$_externalIdScope'] } } },

	// Lookup clubs where this SGO has membership
	{
		$lookup: {
			from: 'clubs',
			let: { sgoKey: '$sgoKey' },
			pipeline: [
				{
					$match: {
						$expr: {
							$gt: [
								{
									$size: {
										$filter: {
											input: { $ifNull: ['$sgoMemberships', []] },
											as: 'membership',
											cond: {
												$eq: [{ $concat: ['$$membership._externalSgoId', keySeparator, '$$membership._externalSgoIdScope'] }, '$$sgoKey'],
											},
										},
									},
								},
								0,
							],
						},
					},
				},
				{
					$project: {
						_id: 1,
						clubPair: {
							$cond: [
								{
									$and: [
										{ $eq: [{ $type: '$_externalId' }, 'string'] },
										{ $ne: ['$_externalId', ''] },
										{ $eq: [{ $type: '$_externalIdScope' }, 'string'] },
										{ $ne: ['$_externalIdScope', ''] },
									],
								},
								{ id: '$_externalId', scope: '$_externalIdScope', key: { $concat: ['$_externalId', keySeparator, '$_externalIdScope'] } },
								null,
							],
						},
					},
				},
				// Filter out clubs with incomplete keys
				{ $match: { clubPair: { $ne: null } } },
			],
			as: 'clubDocs',
		},
	},

	// Final projection with unique club IDs and keys
	{
		$project: {
			_id: 0,
			ids: { $setUnion: [{ $map: { input: '$clubDocs', as: 't', in: '$$t._id' } }, []] },
			keys: { $setUnion: [{ $map: { input: '$clubDocs', as: 't', in: '$$t.clubPair.key' } }, []] },
		},
	},
];

////////////////////////////////////////////////////////////////////////////////
exports.sgoClubsFacet = sgoClubsFacet;
