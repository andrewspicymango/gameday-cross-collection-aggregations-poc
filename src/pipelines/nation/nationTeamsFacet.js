const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
/**
 * MongoDB aggregation pipeline that performs a lookup to find teams associated with a nation
 * and creates indexed collections of team IDs and keys.
 *
 * @description This pipeline performs the following operations:
 * 1. Looks up teams from the 'teams' collection that match the nation's external ID and scope
 * 2. Projects only essential team fields (_id, _externalId, _externalIdScope)
 * 3. Creates a composite key by concatenating external ID with scope using a separator
 * 4. Generates two output fields:
 *    - `ids`: Array of unique team ObjectIds associated with the nation
 *    - `keys`: Object mapping composite keys to team ObjectIds (empty object if no teams)
 *
 * @type {Array<Object>} MongoDB aggregation pipeline stages
 *
 * @requires keySeparator - Global variable used to separate external ID components in composite keys
 *
 * @example
 * // Usage in aggregation pipeline
 * db.nations.aggregate([
 *   // ... other stages
 *   ...nationTeamsFacet
 * ]);
 */
const nationTeamsFacet = [
	{
		$lookup: {
			from: 'teams',
			let: { nid: '$_externalId', ns: '$_externalIdScope' },
			pipeline: [
				{ $match: { $expr: { $and: [{ $eq: ['$_externalNationId', '$$nid'] }, { $eq: ['$_externalNationIdScope', '$$ns'] }] } } },
				{ $project: { _id: 1, _externalId: 1, _externalIdScope: 1 } },
				{ $set: { key: { $concat: ['$_externalId', keySeparator, '$_externalIdScope'] } } },
			],
			as: 'teams',
		},
	},
	{
		$project: {
			ids: { $setUnion: [{ $map: { input: '$teams', as: 't', in: '$$t._id' } }, []] },
			keys: { $cond: [{ $gt: [{ $size: '$teams' }, 0] }, { $arrayToObject: { $map: { input: '$teams', as: 't', in: ['$$t.key', '$$t._id'] } } }, {}] },
		},
	},
];

////////////////////////////////////////////////////////////////////////////////
exports.nationTeamsFacet = nationTeamsFacet;
