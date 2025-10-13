const { nationMetaFacet } = require('./nationMetaFacet');
const { nationSgosFacet } = require('./nationSgosFacet');
const { nationTeamsFacet } = require('./nationTeamsFacet');
const { nationVenuesFacet } = require('./nationVenuesFacet');
const { keyInAggregation, keySeparator } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
/**
 * Builds a MongoDB aggregation pipeline that:
 * 1. Matches a single Nation document by its external composite key.
 * 2. Runs multiple parallel facet sub-pipelines (meta, sgOs, teams, venues) to gather related identifiers & keys.
 * 3. Reshapes (projects) the first facet results into a flattened materialized aggregation document.
 * 4. Normalizes any missing facet outputs to empty arrays.
 * 5. Adds a lastUpdated timestamp (server evaluation time via $$NOW).
 * 6. Upserts (merge) the transformed document into a materialized aggregations collection.
 *
 * External dependencies (must be in scope):
 * - nationMetaFacet, nationSgosFacet, nationTeamsFacet, nationVenuesFacet: facet stage arrays.
 * - keySeparator: string used to join nationId + scope into externalKey.
 * - keyInAggregation: field (or fields spec) used as the merge "on" key.
 *
 * @function pipeline
 * @param {Object} config - Runtime configuration object.
 * @param {Object} [config.mongo] - Mongo-related config.
 * @param {string} [config.mongo.matAggCollectionName='materialisedAggregations'] - Target collection for $merge.
 * @param {string} NATION_SCOPE - External scope identifier used for matching and key construction.
 * @param {string} NATION_ID - External nation identifier used for matching and key construction.
 * @returns {Array<Object>} Ordered aggregation stage objects ready for MongoDB aggregate().
 *   Result (after $project) fields:
 *   - resourceType, externalKey, gamedayId, _externalId, _externalIdScope, name
 *   - sgos (ids), sgoKeys, teamIds, teamKeys, venueIds, venueKeys, lastUpdated
 */
const pipeline = (config, NATION_SCOPE, NATION_ID) => [
	{ $match: { _externalId: NATION_ID, _externalIdScope: NATION_SCOPE } },
	{
		$facet: {
			meta: nationMetaFacet,
			sgos: nationSgosFacet,
			teams: nationTeamsFacet,
			venues: nationVenuesFacet,
		},
	},
	{
		$project: {
			resourceType: { $first: '$meta.resourceType' },
			externalKey: {
				$concat: [{ $first: '$meta.nationId' }, keySeparator, { $first: '$meta.nationIdScope' }],
			},
			gamedayId: { $first: '$meta._id' },
			_externalId: { $first: '$meta.nationId' },
			_externalIdScope: { $first: '$meta.nationIdScope' },
			name: { $first: '$meta.name' },
			sgos: { $ifNull: [{ $first: '$sgos.ids' }, []] },
			sgoKeys: { $ifNull: [{ $first: '$sgos.keys' }, []] },
			teamIds: { $ifNull: [{ $first: '$teams.ids' }, []] },
			teamKeys: { $ifNull: [{ $first: '$teams.keys' }, []] },
			venueIds: { $ifNull: [{ $first: '$venues.ids' }, []] },
			venueKeys: { $ifNull: [{ $first: '$venues.keys' }, []] },
		},
	},
	{
		$addFields: {
			lastUpdated: '$$NOW',
		},
	},
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
function queryForNationAggregationDoc(nationId, nationIdScope) {
	return { resourceType: 'nation', externalKey: `${nationId}${keySeparator}${nationIdScope}` };
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { pipeline, queryForNationAggregationDoc };
