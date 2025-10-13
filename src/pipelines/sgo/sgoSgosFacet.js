const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
/**
 * sgoSgosFacet
 *
 * Facet pipeline that resolves SGO → SGO relationships expressed on an SGO document's
 * own `sgoMemberships` array. It converts raw membership entries into canonical
 * (ids, keys) arrays referencing the actual member SGO documents.
 *
 * Functional flow:
 *  1. Keep only the source document's `sgoMemberships` array.
 *  2. $unwind the array so each membership entry is processed independently.
 *  3. Filter to entries where both `_externalSgoId` and `_externalSgoIdScope` exist
 *     and are non-empty strings (guards against malformed data).
 *  4. $group by (id, scope) to deduplicate repeated memberships.
 *  5. For each unique pair, $lookup the corresponding SGO document from `sgos` using
 *     a pipeline (matching on external id + scope) and construct a composite key
 *     "<_externalId><keySeparator><_externalIdScope>".
 *  6. $unwind the lookup result (dropping non-matches).
 *  7. Final $group collects unique ObjectIds and keys via $addToSet.
 *  8. Final $project enforces a stable output shape:
 *       {
 *         ids:  ObjectId[]   (unique member SGO _id values)
 *         keys: string[]     (unique composite external keys)
 *       }
 *
 * Behaviour / guarantees:
 *  - Always returns a single document with `ids` and `keys` arrays (may be empty).
 *  - Ignores membership rows missing valid string id/scope.
 *  - Deduplication occurs both before and after lookup to minimise redundant matches.
 *
 * Input expectation on the originating aggregation document:
 *  - Optional field: sgoMemberships: Array<{
 *        _externalSgoId?: string,
 *        _externalSgoIdScope?: string,
 *        ... (other fields ignored)
 *    }>
 *
 * Output shape (single facet result array element):
 *  [{ ids: ObjectId[], keys: string[] }]
 *
 * @constant {Array<Object>} sgoSgosFacet
 */
const sgoSgosFacet = [
	{ $project: { sgoMemberships: { $ifNull: ['$sgoMemberships', []] } } },
	{ $unwind: '$sgoMemberships' },
	{ $match: { 'sgoMemberships._externalSgoId': { $type: 'string', $ne: '' }, 'sgoMemberships._externalSgoIdScope': { $type: 'string', $ne: '' } } },
	{ $group: { _id: { id: '$sgoMemberships._externalSgoId', scope: '$sgoMemberships._externalSgoIdScope' } } },
	{ $project: { _id: 0, sgoExternalId: '$_id.id', sgoExternalScope: '$_id.scope' } },
	{
		$lookup: {
			from: 'sgos',
			let: { sid: '$sgoExternalId', ss: '$sgoExternalScope' },
			pipeline: [
				{ $match: { $expr: { $and: [{ $eq: ['$_externalId', '$$sid'] }, { $eq: ['$_externalIdScope', '$$ss'] }] } } },
				{ $project: { _id: 1, key: { $concat: ['$_externalId', keySeparator, '$_externalIdScope'] } } },
				{ $match: { key: { $type: 'string' } } },
			],
			as: 'sgoHit',
		},
	},
	{ $unwind: { path: '$sgoHit', preserveNullAndEmptyArrays: false } },
	{ $group: { _id: null, ids: { $addToSet: '$sgoHit._id' }, keys: { $addToSet: '$sgoHit.key' } } },
	{ $project: { _id: 0, ids: { $ifNull: ['$ids', []] }, keys: { $ifNull: ['$keys', []] } } },
];

////////////////////////////////////////////////////////////////////////////////
exports.sgoSgosFacet = sgoSgosFacet;
