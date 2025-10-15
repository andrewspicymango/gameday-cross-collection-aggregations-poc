const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
/**
 * Aggregation facet that enriches club documents with related teams data.
 *
 * This facet performs a lookup to find teams belonging to a club based on matching
 * external ID and scope, then creates collections of team IDs and external keys.
 *
 * Pipeline stages:
 * 1. $lookup - Joins teams collection where team's _externalClubId matches club's _externalId
 * 2. $project - Creates 'ids' set of ObjectIds and 'keys' object mapping external keys to IDs
 *
 * @type {Array<Object>}
 * @requires keySeparator - String separator for constructing composite keys
 * @returns {Object} Object with 'ids' array and 'keys' object for matched teams
 *
 * @example
 * // Usage in aggregation pipeline
 * db.clubs.aggregate([
 *   { $facet: { teams: clubTeamsFacet } }
 * ])
 */
const clubTeamsFacet = [
	{
		$lookup: {
			from: 'teams',
			let: { thisClubId: '$_externalId', thisClubIdScope: '$_externalIdScope' },
			pipeline: [
				{ $match: { $expr: { $and: [{ $eq: ['$_externalClubId', '$$thisClubId'] }, { $eq: ['$_externalClubIdScope', '$$thisClubIdScope'] }] } } },
				{ $project: { _id: 1, _externalId: 1, _externalIdScope: 1 } },
				{ $set: { key: { $concat: ['$_externalId', keySeparator, '$_externalIdScope'] } } },
			],
			as: 'teams',
		},
	},
	{
		$project: {
			ids: { $setUnion: [{ $map: { input: '$teams', as: 't', in: '$$t._id' } }, []] },
			keys: { $cond: [{ $gt: [{ $size: '$teams' }, 0] }, { $arrayToObject: { $map: { input: '$teams', as: 's', in: ['$$s.key', '$$s._id'] } } }, {}] },
		},
	},
];

exports.clubTeamsFacet = clubTeamsFacet;
