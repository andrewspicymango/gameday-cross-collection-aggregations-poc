const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
/**
 * Aggregation facet pipeline that resolves SGO references found on a document's
 * sgoMemberships array into unique MongoDB ObjectIds and composed keys.
 *
 * Stages and purpose:
 *  - $project { sgoMemberships: 1 }:
 *      Keep only the sgoMemberships array for downstream processing.
 *  - $unwind '$sgoMemberships':
 *      Expand the array so each membership becomes its own document.
 *  - $match { 'sgoMemberships._externalSgoId': { $type: 'string' }, 'sgoMemberships._externalSgoIdScope': { $type: 'string' } }:
 *      Filter to memberships that contain both external id and scope as strings.
 *  - $group { _id: { id, scope } }:
 *      Deduplicate memberships by external id + scope.
 *  - $project { sgoId, sgoScope }:
 *      Expose those deduped external id and scope values as fields for lookup.
 *  - $lookup from 'sgos' with pipeline using $expr:
 *      Look up matching documents in the "sgos" collection where
 *        _externalId == sgoId AND _externalIdScope == sgoScope.
 *      Inside the lookup pipeline a key is constructed by concatenating
 *      sgoId, a keySeparator, and sgoScope. Note: keySeparator must be
 *      available when the pipeline is built (e.g. injected into the pipeline
 *      document or otherwise captured by the code that constructs the pipeline).
 *      The lookup projects only {_id, key}.
 *  - $unwind { path: '$sgo', preserveNullAndEmptyArrays: false }:
 *      Discard non-matching lookups (only keep successful matches).
 *  - $group { _id: null, ids: { $addToSet: '$sgo._id' }, keys: { $addToSet: '$sgo.key' } }:
 *      Collect unique ObjectIds and unique key strings.
 *  - $project { ids, keys }:
 *      Return the final shape.
 *
 * Input expectations:
 *  - The input document should contain an optional array field:
 *      sgoMemberships: Array<{ _externalSgoId?: string, _externalSgoIdScope?: string, ... }>
 *
 * Output shape:
 *  - { ids: ObjectId[], keys: string[] }
 *    - ids: unique ObjectId values from the matched documents in the "sgos" collection
 *    - keys: unique composed string keys (sgoId + keySeparator + sgoScope)
 *
 * Important notes:
 *  - Memberships lacking string external id or scope are ignored.
 *  - Non-matching lookups are removed by the unwind with preserveNullAndEmptyArrays: false.
 *  - Deduplication is performed via $group + $addToSet.
 *  - The pipeline expects the "sgos" collection to expose _externalId and _externalIdScope fields.
 *
 * @constant {Array<Object>} sgoFacet  Aggregation pipeline array to be used as a facet or sub-pipeline.
 */
const competitionSgoFacet = [
	{ $project: { sgoMemberships: 1 } },
	{ $unwind: '$sgoMemberships' },
	{ $match: { 'sgoMemberships._externalSgoId': { $type: 'string' }, 'sgoMemberships._externalSgoIdScope': { $type: 'string' } } },
	{ $group: { _id: { id: '$sgoMemberships._externalSgoId', scope: '$sgoMemberships._externalSgoIdScope' } } },
	{ $project: { _id: 0, sgoId: '$_id.id', sgoScope: '$_id.scope' } },
	{
		$lookup: {
			from: 'sgos',
			let: { sgoId: '$sgoId', sgoScope: '$sgoScope' },
			pipeline: [
				{ $match: { $expr: { $and: [{ $eq: ['$_externalId', '$$sgoId'] }, { $eq: ['$_externalIdScope', '$$sgoScope'] }] } } },
				{ $set: { key: { $concat: ['$$sgoId', keySeparator, '$$sgoScope'] } } },
				{ $project: { _id: 1, key: 1 } },
			],
			as: 'sgo',
		},
	},
	{ $unwind: { path: '$sgo', preserveNullAndEmptyArrays: false } },
	{ $group: { _id: null, ids: { $addToSet: '$sgo._id' }, keys: { $addToSet: '$sgo.key' } } },
	{ $project: { _id: 0, ids: 1, keys: 1 } },
];

////////////////////////////////////////////////////////////////////////////////
exports.competitionSgoFacet = competitionSgoFacet;
