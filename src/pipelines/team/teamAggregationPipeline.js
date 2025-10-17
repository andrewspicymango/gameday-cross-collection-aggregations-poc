const { teamMetaFacet } = require('./teamMetaFacet');
const { teamClubFacet } = require('./teamClubFacet');
const { teamMembersSportsPersonFacet } = require('./teamMembersSportsPersonFacet');
const { teamNationFacet } = require('./teamNationFacet');
const { teamVenueFacet } = require('./teamVenueFacet');
const { teamStaffFacet } = require('./teamStaffFacet');
const { teamEventsFacet } = require('./teamEventsFacet');
const { teamSgoFacet } = require('./teamSgoFacet');
const { keySeparator } = require('../constants');
const { keyInAggregation } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
/**
 * Builds a MongoDB aggregation pipeline to materialize a team aggregation document.
 *
 * Stages:
 *  - $match: filters documents by _externalId (TEAM_ID) and _externalIdScope (TEAM_SCOPE).
 *  - $facet: runs parallel sub-pipelines producing named facets: meta, clubs, sportsPersons, nations, venues, staff, events.
 *  - $project: extracts first values from meta (resourceType, teamId/_id, scopes), composes externalKey (using keySeparator),
 *      and normalizes each facet output to arrays using $first and $ifNull.
 *  - $addFields: promotes extracted fields to top-level (resourceType, externalKey, gamedayId, _externalId, _externalIdScope)
 *      and stamps lastUpdated with $$NOW (pipeline execution time).
 *  - $merge: persists the resulting document into the materialised aggregations collection
 *      (config.mongo.matAggCollectionName || 'materialisedAggregations'), matching on keyInAggregation,
 *      replacing when matched and inserting when not matched.
 *
 * @param {Object} config - Runtime configuration (used to resolve target collection name).
 * @param {string} TEAM_SCOPE - External ID scope used to match the team document.
 * @param {string|number} TEAM_ID - External team identifier to match.
 * @returns {Array<Object>} Aggregation pipeline array suitable for collection.aggregate().
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
