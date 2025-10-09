const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
/**
 * Aggregation facet stages that resolve external competition identifiers into internal competition ids and
 * composite external keys.
 *
 * Behavior:
 * - 1st stage ($lookup):
 *   - Looks up matching documents in the "competitions" collection.
 *   - Uses let bindings: cid <- _externalCompetitionId, cids <- _externalCompetitionIdScope.
 *   - Matches competitions where _externalId == $$cid AND _externalIdScope == $$cids.
 *   - Projects only the fields: {_id, _externalId, _externalIdScope}.
 *   - Result is stored in the "competitions" array on each input document.
 *
 * - 2nd stage ($project):
 *   - ids: produces a de-duplicated set (setUnion) of matched competition _id values.
 *   - keys: produces a de-duplicated set of concatenated external keys built as
 *           _externalId + keySeparator + _externalIdScope for each matched competition.
 *
 * Notes / Requirements:
 * - Input documents must contain the fields _externalCompetitionId and _externalCompetitionIdScope.
 * - The symbol keySeparator must be available in the aggregation scope (e.g., defined earlier) and is used as
 *   the delimiter when building composite keys.
 * - The facet returns arrays (possibly empty) and is intended for use inside a larger aggregation pipeline
 *   (for example within a $facet or as a sub-pipeline) to produce canonical internal ids and external identifier keys
 *   for downstream operations (joins, grouping, lookups).
 *
 * Output shape (per facet document):
 *   {
 *     ids: ObjectId[],   // unique internal competition _id values
 *     keys: string[]     // unique composite external key strings
 *   }
 *
 * @constant {Array<Object>} stageCompetitionFacet - Aggregation stages implementing the competition lookup + projection.
 */
const stageCompetitionFacet = [
	{
		$lookup: {
			from: 'competitions',
			let: { cid: '$_externalCompetitionId', cids: '$_externalCompetitionIdScope' },
			pipeline: [
				{
					$match: {
						$expr: {
							$and: [{ $eq: ['$_externalId', '$$cid'] }, { $eq: ['$_externalIdScope', '$$cids'] }],
						},
					},
				},
				{ $project: { _id: 1, _externalId: 1, _externalIdScope: 1 } },
			],
			as: 'competitions',
		},
	},
	{
		$project: {
			ids: { $setUnion: [{ $map: { input: '$competitions', as: 'c', in: '$$c._id' } }, []] },
			keys: { $setUnion: [{ $map: { input: '$competitions', as: 'c', in: { $concat: ['$$c._externalId', keySeparator, '$$c._externalIdScope'] } } }, []] },
		},
	},
];

////////////////////////////////////////////////////////////////////////////////
exports.stageCompetitionFacet = stageCompetitionFacet;
