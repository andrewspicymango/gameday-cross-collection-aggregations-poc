////////////////////////////////////////////////////////////////////////////////
/**
 * Aggregation facet pipeline that projects and renames stage metadata fields.
 *
 * This pipeline array is intended to be used as the value of a $facet entry
 * (or embedded into an aggregation pipeline) to produce a focused projection
 * of stage-related metadata. It:
 *  - removes the default _id field
 *  - renames `_externalId` to `stageId`
 *  - renames `_externalIdScope` to `stageIdScope`
 *  - preserves `resourceType`
 *
 * Resulting document shape:
 * {
 *   stageId: <value from _externalId>,
 *   stageIdScope: <value from _externalIdScope>,
 *   resourceType: <value from resourceType>
 * }
 *
 * @constant {Array<Object>} stageMetaFacet - Aggregation pipeline array for the facet.
 * @example
 * // Use as a facet:
 * db.collection.aggregate([
 *   { $facet: { stageMeta: stageMetaFacet } }
 * ]);
 */
const stageMetaFacet = [
	{
		$project: {
			_id: 1,
			stageId: '$_externalId',
			stageIdScope: '$_externalIdScope',
			resourceType: '$resourceType',
			name: { $getField: { field: '$defaultLanguage', input: '$name' } },
		},
	},
];

////////////////////////////////////////////////////////////////////////////////
exports.stageMetaFacet = stageMetaFacet;
