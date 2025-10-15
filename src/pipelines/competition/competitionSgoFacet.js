const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
/**
 * MongoDB aggregation pipeline facet for processing competition SGO (Sports Governing Organization) memberships.
 *
 * This pipeline:
 * 1. Extracts and unwinds sgoMemberships from competition documents
 * 2. Filters for valid external SGO IDs and scopes (string types only)
 * 3. Groups by unique SGO ID and scope combinations
 * 4. Performs lookup to find corresponding SGO documents in the 'sgos' collection
 * 5. Creates key-value pairs using SGO external ID, scope, and document ID
 * 6. Aggregates results into arrays of SGO IDs and a key-value mapping object
 *
 * @type {Array<Object>} MongoDB aggregation pipeline stages
 * @returns {Object} Result containing:
 *   - ids: Array of unique SGO document IDs
 *   - keys: Object mapping SGO keys (id + scope) to document IDs
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
			as: 'sgoDocs',
		},
	},
	{ $project: { pairs: { $map: { input: { $ifNull: ['$sgoDocs', []] }, as: 'd', in: { k: '$$d.key', v: '$$d._id' } } } } },
	{ $unwind: { path: '$pairs', preserveNullAndEmptyArrays: false } },
	{ $group: { _id: null, ids: { $addToSet: '$pairs.v' }, kvps: { $addToSet: '$pairs' } } },
	{ $project: { _id: 0, ids: 1, keys: { $arrayToObject: '$kvps' } } },
];

////////////////////////////////////////////////////////////////////////////////
exports.competitionSgoFacet = competitionSgoFacet;
