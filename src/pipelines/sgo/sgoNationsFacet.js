const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
/**
 * SGO Nations Facet
 *
 * Aggregation facet pipeline for resolving nation references for a given SGO.
 *
 * This pipeline is intended to be executed in the context of an SGO document that exposes
 * the following fields on the current pipeline input doc:
 *   - _externalId
 *   - _externalIdScope
 *
 * Behaviour summary:
 *   1. Build the SGO key string using the global `keySeparator` value:
 *      "<_externalId><keySeparator><_externalIdScope>".
 *   2. Lookup nation documents (collection: "nations") that have this SGO
 *      in their sgoMemberships array by matching the derived SGO key.
 *   3. From each matched nation, create nation pairs with id, scope, and composite key.
 *      Only nations with valid _externalId and _externalIdScope are included.
 *   4. Project a final object containing:
 *        - ids: unique set of matched nations._id (typically ObjectId)
 *        - keys: unique set of external key strings ("<id><keySeparator><scope>") for the nations
 *
 * Important notes / assumptions:
 *   - A variable keySeparator must be available in the outer JS scope where this pipeline is defined;
 *     it is used to compose the stable composite keys in all lookups.
 *   - Only nations with string-typed, non-empty _externalId and _externalIdScope are considered.
 *   - SGO membership is determined by matching SGO keys in the sgoMemberships array.
 *   - Uniqueness is enforced via $setUnion; the final projection produces de-duplicated arrays.
 *   - If no matches are found, the pipeline yields empty arrays for both ids and keys.
 *
 * Result shape (per SGO input document):
 *   {
 *     ids: [ /* Array of nations._id values (unique) *\/ ],
 *     keys: [ /* Array of "<id><keySeparator><scope>" strings (unique) *\/ ]
 *   }
 *
 * Collections referenced:
 *   - "nations"
 *
 * Usage:
 *   - Can be used as a facet or inline pipeline stage when aggregating SGOs to resolve
 *     the set of nation references where the SGO has membership.
 *
 * @constant
 * @type {Array<Object>}
 * @name sgoNationsFacet
 */
const sgoNationsFacet = [
	{ $addFields: { sgoKey: { $concat: ['$_externalId', keySeparator, '$_externalIdScope'] } } },
	{
		$lookup: {
			from: 'nations',
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
						nationPair: {
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
				{ $match: { nationPair: { $ne: null } } },
			],
			as: 'nationDocs',
		},
	},
	{
		$project: {
			_id: 0,
			ids: { $setUnion: [{ $map: { input: '$nationDocs', as: 't', in: '$$t._id' } }, []] },
			keys: { $setUnion: [{ $map: { input: '$nationDocs', as: 't', in: '$$t.nationPair.key' } }, []] },
		},
	},
];

////////////////////////////////////////////////////////////////////////////////
exports.sgoNationsFacet = sgoNationsFacet;
