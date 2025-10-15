const { clubMetaFacet } = require('./clubMetaFacet');
const { clubTeamsFacet } = require('./clubTeamsFacet');
const { clubVenuesFacet } = require('./clubVenuesFacet');
const { clubSgosFacet } = require('./clubSgosFacet');
const { clubStaffFacet } = require('./clubStaffFacet');
const { keyInAggregation } = require('../constants');
const { keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
const pipeline = (config, CLUB_SCOPE, CLUB_ID) => [
	//////////////////////////////////////////////////////////////////////////////
	//$match: filters by _externalId and _externalIdScope (COMP_ID, COMP_SCOPE)
	{ $match: { _externalId: CLUB_ID, _externalIdScope: CLUB_SCOPE } },

	//////////////////////////////////////////////////////////////////////////////
	// $facet: runs the provided sub-facets (sgos, stages, events, teams, sportsPersons, venues, meta)
	{
		$facet: {
			meta: clubMetaFacet,
			sgos: clubSgosFacet,
			staff: clubStaffFacet,
			teams: clubTeamsFacet,
			venues: clubVenuesFacet,
		},
	},

	//////////////////////////////////////////////////////////////////////////////
	// $project: extracts the first/meta values and normalizes facet outputs to arrays (defaults to [])
	{
		$project: {
			resourceType: { $first: '$meta.resourceType' },
			externalKey: { $concat: [{ $first: '$meta.clubId' }, keySeparator, { $first: '$meta.clubIdScope' }] },
			gamedayId: { $first: '$meta._id' },
			_externalId: { $first: '$meta.clubId' },
			_externalIdScope: { $first: '$meta.clubIdScope' },
			name: { $first: '$meta.name' },
			sgos: { $ifNull: [{ $first: '$sgos.ids' }, []] },
			sgoKeys: { $ifNull: [{ $first: '$sgos.keys' }, []] },
			staff: { $ifNull: [{ $first: '$staff.ids' }, []] },
			staffKeys: { $ifNull: [{ $first: '$staff.keys' }, []] },
			teamIds: { $ifNull: [{ $first: '$teams.ids' }, []] },
			teamKeys: { $ifNull: [{ $first: '$teams.keys' }, []] },
			venueIds: { $ifNull: [{ $first: '$venues.ids' }, []] },
			venueKeys: { $ifNull: [{ $first: '$venues.keys' }, []] },
		},
	},

	//////////////////////////////////////////////////////////////////////////////
	{
		$addFields: {
			resourceType: '$resourceType',
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
/**
 * Builds a Mongo facet filter for the club aggregation document.
 *
 * @param {string} clubId - Identifier for the club.
 * @param {string} clubIdScope - Scope appended to the club identifier.
 * @returns {{resourceType: string, externalKey: string}} Query object matching resourceType 'club' and externalKey `${clubId}${keySeparator}${clubIdScope}`.
 */
function queryForClubAggregationDoc(clubId, clubIdScope) {
	return { resourceType: 'club', externalKey: `${clubId}${keySeparator}${clubIdScope}` };
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { pipeline, queryForClubAggregationDoc };
