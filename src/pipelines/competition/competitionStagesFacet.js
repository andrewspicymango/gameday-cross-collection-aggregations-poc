const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
/**
 * Aggregation facet used to resolve related "stages" for a competition document and
 * produce two deduplicated lookup outputs: an array of stage IDs and an array of
 * composite keys for each stage.
 *
 * Behavior:
 * - Performs a $lookup into the "stages" collection using two "let" variables:
 *   - cid: sourced from the input document's _externalId
 *   - cs: sourced from the input document's _externalIdScope
 * - The lookup pipeline:
 *   - $match with $expr to find stages where
 *     _externalCompetitionId === $$cid and _externalCompetitionIdScope === $$cs
 *   - $project to retain only the stage _id, _externalId and _externalIdScope fields
 * - After the lookup, a $project creates two fields:
 *   - ids: a deduplicated array of stage _id values (uses $setUnion to ensure uniqueness
 *     and to normalize an absent/empty "stages" array to an empty array)
 *   - keys: a deduplicated array of composite string keys for each stage. Each key is
 *     formed by concatenating the stage's _externalId, a keySeparator value (expected
 *     to be available in the aggregation context), and the stage's _externalIdScope.
 *
 * Notes:
 * - The resulting "ids" array contains the unique identifiers for matched stages (type depends
 *   on how stage _id is stored, often ObjectId).
 * - The resulting "keys" array contains unique string keys in the form:
 *   "<stage._externalId><keySeparator><stage._externalIdScope>".
 * - keySeparator must be defined in the surrounding aggregation pipeline context (e.g. as a
 *   variable or substituted before running the pipeline).
 *
 * @constant {Array<Object>}
 * @example
 * // Resulting document fragment after this facet stage:
 * // {
 * //   ids: [ObjectId("..."), ...],
 * //   keys: ["extId1::scope1", "extId2::scope2", ...]
 * // }
 */
const competitionStagesFacet = [
	{
		$lookup: {
			from: 'stages',
			let: { cid: '$_externalId', cs: '$_externalIdScope' },
			pipeline: [
				{
					$match: {
						$expr: {
							$and: [{ $eq: ['$_externalCompetitionId', '$$cid'] }, { $eq: ['$_externalCompetitionIdScope', '$$cs'] }],
						},
					},
				},
				{ $project: { _id: 1, _externalId: 1, _externalIdScope: 1 } },
			],
			as: 'stages',
		},
	},
	{
		$project: {
			ids: { $setUnion: [{ $map: { input: '$stages', as: 's', in: '$$s._id' } }, []] },
			keys: { $setUnion: [{ $map: { input: '$stages', as: 's', in: { $concat: ['$$s._externalId', keySeparator, '$$s._externalIdScope'] } } }, []] },
		},
	},
];
exports.competitionStagesFacet = competitionStagesFacet;
