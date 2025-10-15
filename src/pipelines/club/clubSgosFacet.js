const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
/**
 * MongoDB aggregation pipeline facet for extracting and mapping club SGO (Sports Governing Organization) data.
 *
 * This pipeline processes club documents to:
 * 1. Extract SGO memberships with valid external IDs and scopes
 * 2. Look up corresponding SGO documents from the 'sgos' collection
 * 3. Create key-value pairs mapping SGO keys to their MongoDB ObjectIds
 * 4. Return both an array of unique SGO IDs and a key-object mapping
 *
 * @type {Array<Object>} MongoDB aggregation pipeline stages
 * @returns {Object} Result containing:
 *   - ids: Array of unique SGO ObjectIds
 *   - keys: Object mapping SGO keys (id + scope) to ObjectIds
 */
const clubSgosFacet = [
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
exports.clubSgosFacet = clubSgosFacet;
