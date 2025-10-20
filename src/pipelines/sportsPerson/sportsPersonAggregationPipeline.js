const { sportsPersonClubMemberFacet } = require('./sportsPersonClubMemberFacet');
const { sportsPersonTeamMemberFacet } = require('./sportsPersonTeamMemberFacet');
const { sportsPersonEventFacet } = require('./sportsPersonEventFacet');
const { sportsPersonKeyMomentFacet } = require('./sportsPersonKeyMomentFacet');
const { sportsPersonStaffFacet } = require('./sportsPersonStaffFacet');
const { sportsPersonMetaFacet } = require('./sportsPersonMetaFacet');
const { sportsPersonRankingsFacet } = require('./sportsPersonRankingsFacet');
const { keySeparator } = require('../constants');
const { keyInAggregation } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
/**
 * Creates a MongoDB aggregation pipeline for sports person data aggregation.
 *
 * The pipeline performs the following operations:
 * 1. Matches documents by external ID and scope
 * 2. Uses $facet to run parallel aggregations for meta data, clubs, teams, events, staff, and rankings
 * 3. Projects and normalizes the facet results into a structured format
 * 4. Adds computed fields including current timestamp
 * 5. Merges results into a materialized aggregations collection
 *
 * @param {Object} config - Configuration object containing MongoDB settings
 * @param {string} config.mongo.matAggCollectionName - Target collection name for materialized aggregations
 * @param {string} SP_SCOPE - External ID scope for the sports person
 * @param {string} SP_ID - External ID of the sports person to aggregate
 * @returns {Array} MongoDB aggregation pipeline array
 *
 * @example
 * const aggPipeline = pipeline(config, 'ESPN', 'player123');
 * db.collection.aggregate(aggPipeline);
 */
const pipeline = (config, SP_SCOPE, SP_ID) => [
	{
		$match: { _externalId: SP_ID, _externalIdScope: SP_SCOPE },
	},
	//////////////////////////////////////////////////////////////////////////////
	// $facet: runs the provided sub-facets
	{
		$facet: {
			meta: sportsPersonMetaFacet(),
			clubs: sportsPersonClubMemberFacet(),
			events: sportsPersonEventFacet(),
			// keyMoments: sportsPersonKeyMomentFacet(), // No key moments facet for sports person currently
			rankings: sportsPersonRankingsFacet(),
			staff: sportsPersonStaffFacet(),
			teams: sportsPersonTeamMemberFacet(),
		},
	},
	//////////////////////////////////////////////////////////////////////////////
	// $project: extracts the first/meta values and normalizes facet outputs to arrays (defaults to [])
	{
		$project: {
			resourceType: { $first: '$meta.resourceType' },
			externalKey: { $concat: [{ $first: '$meta.sportsPersonId' }, keySeparator, { $first: '$meta.sportsPersonIdScope' }] },
			gamedayId: { $first: '$meta._id' },
			_externalId: { $first: '$meta.sportsPersonId' },
			_externalIdScope: { $first: '$meta.sportsPersonIdScope' },
			name: { $first: '$meta.name' },
			clubs: { $ifNull: [{ $first: '$clubs.ids' }, []] },
			clubKeys: { $ifNull: [{ $first: '$clubs.keys' }, []] },
			events: { $ifNull: [{ $first: '$events.ids' }, []] },
			eventKeys: { $ifNull: [{ $first: '$events.keys' }, []] },
			// keyMoments: { $ifNull: [{ $first: '$keyMoments.ids' }, []] },
			// keyMomentKeys: { $ifNull: [{ $first: '$keyMoments.keys' }, []] },
			rankings: { $ifNull: [{ $first: '$rankings.ids' }, []] },
			rankingKeys: { $ifNull: [{ $first: '$rankings.keys' }, []] },
			staff: { $ifNull: [{ $first: '$staff.ids' }, []] },
			staffKeys: { $ifNull: [{ $first: '$staff.keys' }, []] },
			teams: { $ifNull: [{ $first: '$teams.ids' }, []] },
			teamKeys: { $ifNull: [{ $first: '$teams.keys' }, []] },
		},
	},
	//////////////////////////////////////////////////////////////////////////////
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
/**
 * Constructs a query object for retrieving a sports person aggregation document.
 *
 * @param {string} sportsPersonId - The unique identifier for the sports person
 * @param {string} sportsPersonIdScope - The scope or context for the sports person ID
 * @returns {Object} Query object with resourceType and externalKey properties
 * @returns {string} returns.resourceType - Always set to 'sportsperson'
 * @returns {string} returns.externalKey - Concatenated key using sportsPersonId, keySeparator, and sportsPersonIdScope
 *
 * @example
 * // Returns { resourceType: 'sportsperson', externalKey: 'player123|scope1' }
 * queryForSportsPersonAggregationDoc('player123', 'scope1');
 */
function queryForSportsPersonAggregationDoc(sportsPersonId, sportsPersonIdScope) {
	return { resourceType: 'sportsperson', externalKey: `${sportsPersonId}${keySeparator}${sportsPersonIdScope}` };
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { pipeline, queryForSportsPersonAggregationDoc };
