const { teamMetaFacet } = require('./teamMetaFacet');
const { teamClubFacet } = require('./teamClubFacet');
const { teamMembersSportsPersonFacet } = require('./teamMembersSportsPersonFacet');
const { teamNationFacet } = require('./teamNationFacet');
const { teamVenueFacet } = require('./teamVenueFacet');
const { teamStaffFacet } = require('./teamStaffFacet');
const { teamEventsFacet } = require('./teamEventsFacet');
const { teamSgoFacet } = require('./teamSgoFacet');
const { teamRankingsFacet } = require('./teamRankingsFacet');
const { keySeparator } = require('../constants');
const { keyInAggregation } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
/**
 * MongoDB aggregation pipeline for team data processing and materialization.
 *
 * Filters team documents by external ID and scope, then uses faceted aggregation
 * to gather related data from multiple collections (clubs, sports persons, nations,
 * venues, staff, events, SGOs, rankings). The results are projected into a normalized
 * format and merged into a materialized aggregations collection.
 *
 * @param {Object} config - Configuration object containing database settings
 * @param {string} TEAM_SCOPE - External ID scope for team filtering
 * @param {string} TEAM_ID - External ID for team filtering
 * @returns {Array} MongoDB aggregation pipeline array with $match, $facet, $project, $addFields, and $merge stages
 *
 * Pipeline stages:
 * 1. $match - Filters by _externalId and _externalIdScope
 * 2. $facet - Runs parallel sub-aggregations for related data
 * 3. $project - Normalizes facet outputs and extracts metadata
 * 4. $addFields - Sets output metadata and timestamps
 * 5. $merge - Upserts results into materialized aggregations collection
 */
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
			sportsPersons: teamMembersSportsPersonFacet,
			nations: teamNationFacet,
			venues: teamVenueFacet,
			staff: teamStaffFacet,
			events: teamEventsFacet,
			sgos: teamSgoFacet,
			rankings: teamRankingsFacet,
		},
	},

	//////////////////////////////////////////////////////////////////////////////
	// $project: extracts the first/meta values and normalizes facet outputs to arrays (defaults to [])
	{
		$project: {
			resourceType: { $first: '$meta.resourceType' },
			externalKey: { $concat: [{ $first: '$meta.teamId' }, keySeparator, { $first: '$meta.teamIdScope' }] },
			gamedayId: { $first: '$meta._id' },
			_externalId: { $first: '$meta.teamId' },
			_externalIdScope: { $first: '$meta.teamIdScope' },
			name: { $first: '$meta.name' },
			clubs: { $ifNull: [{ $first: '$clubs.ids' }, []] },
			clubKeys: { $ifNull: [{ $first: '$clubs.keys' }, []] },
			events: { $ifNull: [{ $first: '$events.ids' }, []] },
			eventKeys: { $ifNull: [{ $first: '$events.keys' }, []] },
			nations: { $ifNull: [{ $first: '$nations.ids' }, []] },
			nationKeys: { $ifNull: [{ $first: '$nations.keys' }, []] },
			sgos: { $ifNull: [{ $first: '$sgos.ids' }, []] },
			sgoKeys: { $ifNull: [{ $first: '$sgos.keys' }, []] },
			sportsPersons: { $ifNull: [{ $first: '$sportsPersons.ids' }, []] },
			sportsPersonKeys: { $ifNull: [{ $first: '$sportsPersons.keys' }, []] },
			staff: { $ifNull: [{ $first: '$staff.ids' }, []] },
			staffKeys: { $ifNull: [{ $first: '$staff.keys' }, []] },
			venues: { $ifNull: [{ $first: '$venues.ids' }, []] },
			venueKeys: { $ifNull: [{ $first: '$venues.keys' }, []] },
			rankings: { $ifNull: [{ $first: '$rankings.ids' }, []] },
			rankingKeys: { $ifNull: [{ $first: '$rankings.keys' }, []] },
		},
	},

	//////////////////////////////////////////////////////////////////////////////
	// $addFields: sets output metadata (resourceType, _externalId, _externalIdScope, targetType)
	// and stamps lastUpdated with $$NOW (pipeline execution time)
	{
		$addFields: {
			resourceType: { $toLower: '$resourceType' },
			externalKey: '$externalKey',
			gamedayId: '$gamedayId',
			_externalId: '$_externalId',
			_externalIdScope: '$_externalIdScope',
			name: '$name',
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
/** Build a query object for a team aggregation document;
 * @param {string} teamId - team identifier;
 * @param {string} teamIdScope - scope appended to the key;
 * @returns {{resourceType: string, externalKey: string}}
 */
function queryForTeamAggregationDoc(teamId, teamIdScope) {
	return { resourceType: 'team', externalKey: `${teamId}${keySeparator}${teamIdScope}` };
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { pipeline, queryForTeamAggregationDoc };
