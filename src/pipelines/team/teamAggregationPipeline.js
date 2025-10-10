const { teamMetaFacet } = require('./teamMetaFacet');
const { teamClubFacet } = require('./teamClubFacet');
const { teamSportsPersonFacet } = require('./teamSportsPersonFacet');
const { teamNationFacet } = require('./teamNationFacet');
const { teamVenueFacet } = require('./teamVenueFacet');
const { teamStaffFacet } = require('./teamStaffFacet');
const { teamEventsFacet } = require('./teamEventsFacet');
const teamAggregationTargetType = [`event`, `sportsPerson`, `venue`, `club`, `nation`, `staff`].join('/');
const keyInAggregation = ['resourceType', '_externalIdScope', '_externalId', 'targetType'];

////////////////////////////////////////////////////////////////////////////////
const pipeline = (config, TEAM_SCOPE, TEAM_ID) => [
	//////////////////////////////////////////////////////////////////////////////
	//$match: filters by _externalId and _externalIdScope (TEAM_ID, TEAM_SCOPE)
	{ $match: { _externalId: TEAM_ID, _externalIdScope: TEAM_SCOPE } },

	//////////////////////////////////////////////////////////////////////////////
	// $facet: runs the provided sub-facets (sgos, stages, events, teams, sportsPersons, venues, meta)
	{
		$facet: {
			meta: teamMetaFacet,
			clubs: teamClubFacet,
			sportsPersons: teamSportsPersonFacet,
			nations: teamNationFacet,
			venues: teamVenueFacet,
			staff: teamStaffFacet,
			events: teamEventsFacet,
		},
	},

	//////////////////////////////////////////////////////////////////////////////
	// $project: extracts the first/meta values and normalizes facet outputs to arrays (defaults to [])
	{
		$project: {
			gamedayId: { $first: '$meta._id' },
			_externalId: { $first: '$meta.teamId' },
			_externalIdScope: { $first: '$meta.teamIdScope' },
			resourceType: { $first: '$meta.resourceType' },
			events: {
				$ifNull: [{ $first: '$events.ids' }, []],
			},
			eventKeys: {
				$ifNull: [{ $first: '$events.keys' }, []],
			},
			clubs: {
				$ifNull: [{ $first: '$clubs.ids' }, []],
			},
			clubKeys: {
				$ifNull: [{ $first: '$clubs.keys' }, []],
			},
			sportsPersons: {
				$ifNull: [{ $first: '$sportsPersons.ids' }, []],
			},
			sportsPersonKeys: {
				$ifNull: [{ $first: '$sportsPersons.keys' }, []],
			},
			nations: {
				$ifNull: [{ $first: '$nations.ids' }, []],
			},
			nationKeys: {
				$ifNull: [{ $first: '$nations.keys' }, []],
			},
			staff: {
				$ifNull: [{ $first: '$staff.ids' }, []],
			},
			staffKeys: {
				$ifNull: [{ $first: '$staff.keys' }, []],
			},
			venues: {
				$ifNull: [{ $first: '$venues.ids' }, []],
			},
			venueKeys: {
				$ifNull: [{ $first: '$venues.keys' }, []],
			},
		},
	},

	//////////////////////////////////////////////////////////////////////////////
	// $addFields: sets output metadata (resourceType, _externalId, _externalIdScope, targetType)
	// and stamps lastUpdated with $$NOW (pipeline execution time)
	{
		$addFields: {
			gamedayId: '$gamedayId',
			resourceType: '$resourceType',
			_externalId: '$_externalId',
			_externalIdScope: '$_externalIdScope',
			targetType: teamAggregationTargetType,
			lastUpdated: '$$NOW', // current pipeline execution time
		},
	},

	//////////////////////////////////////////////////////////////////////////////
	{
		$merge: {
			into: config?.mongo?.matAggCollectionName || 'materialisedAggregations',
			on: keyInAggregation,
			whenMatched: 'replace',
			whenNotMatched: 'insert',
		},
	},
];

////////////////////////////////////////////////////////////////////////////////
function queryForTeamAggregationDoc(teamId, teamIdScope) {
	return { resourceType: 'team', _externalIdScope: teamIdScope, _externalId: teamId, targetType: teamAggregationTargetType };
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { pipeline, queryForTeamAggregationDoc, teamAggregationTargetType };
