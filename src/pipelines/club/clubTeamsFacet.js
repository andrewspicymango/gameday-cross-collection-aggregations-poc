const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
/**
 * Aggregation facet that enriches a club document with its related teams and
 * derives simplified identifier collections.
 *
 * Stages:
 * 1. $lookup (self-contained pipeline):
 *    - Joins the "teams" collection where a team's _externalClubId and
 *      _externalClubIdScope match the club's _externalId / _externalIdScope.
 *    - Projects only essential team identity fields.
 * 2. $project:
 *    - ids: Set of distinct MongoDB ObjectId values for matched teams.
 *    - keys: Set of distinct composite external keys for teams, formed by
 *      concatenating each team's _externalId, a runtime-supplied keySeparator,
 *      and its _externalIdScope.
 *
 * Expects:
 * - keySeparator (string) to be defined in the surrounding scope.
 *
 * @constant
 * @type {Array<object>}
 * @summary Facet pipeline to attach related teams and derive unique team id/key sets.
 */
const clubTeamsFacet = [
	{
		$lookup: {
			from: 'teams',
			let: { cid: '$_externalId', cs: '$_externalIdScope' },
			pipeline: [
				{
					$match: {
						$expr: {
							$and: [{ $eq: ['$_externalClubId', '$$cid'] }, { $eq: ['$_externalClubIdScope', '$$cs'] }],
						},
					},
				},
				{ $project: { _id: 1, _externalId: 1, _externalIdScope: 1 } },
			],
			as: 'teams',
		},
	},
	{
		$project: {
			ids: { $setUnion: [{ $map: { input: '$teams', as: 't', in: '$$t._id' } }, []] },
			keys: { $setUnion: [{ $map: { input: '$teams', as: 't', in: { $concat: ['$$t._externalId', keySeparator, '$$t._externalIdScope'] } } }, []] },
		},
	},
];

exports.clubTeamsFacet = clubTeamsFacet;
