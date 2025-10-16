const { keySeparator } = require('../constants');
const { sgoMetaFacet } = require('./sgoMetaFacet');
const { sgoOutboundSgosFacet } = require('./sgoOutboundSgosFacet');
const { sgoMembershipsInboundFacet } = require('./sgoMembershipsInboundFacet');
const { keyInAggregation } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
/**
 * Creates a MongoDB aggregation pipeline for SGO (Sports Governing Organization) data processing.
 *
 * The pipeline performs the following operations:
 * 1. Filters documents by external ID and scope
 * 2. Aggregates related data using facets (competitions, teams, clubs, venues, sgos)
 * 3. Projects and flattens the aggregated data into a structured format
 * 4. Adds metadata fields including timestamp
 * 5. Merges results into a materialized aggregations collection
 *
 * @param {Object} config - Configuration object containing MongoDB settings
 * @param {string} config.mongo.matAggCollectionName - Target collection name for materialized aggregations
 * @param {string} SGO_SCOPE - External ID scope for filtering SGO documents
 * @param {string} SGO_ID - External ID for filtering SGO documents
 * @returns {Array} MongoDB aggregation pipeline array
 *
 * @example
 * const aggPipeline = pipeline(config, 'FIFA', 'WORLD_CUP_2024');
 * await db.collection('sgos').aggregate(aggPipeline).toArray();
 */
const pipeline = (config, SGO_SCOPE, SGO_ID) => [
	//////////////////////////////////////////////////////////////////////////////
	//$match: filters by _externalId and _externalIdScope (COMP_ID, COMP_SCOPE)
	{ $match: { _externalId: SGO_ID, _externalIdScope: SGO_SCOPE } },
	{
		$facet: {
			meta: sgoMetaFacet,
			competitions: sgoMembershipsInboundFacet(`competitions`),
			teams: sgoMembershipsInboundFacet(`teams`),
			clubs: sgoMembershipsInboundFacet(`clubs`),
			venues: sgoMembershipsInboundFacet(`venues`),
			outboundSgos: sgoOutboundSgosFacet,
			inboundSgos: sgoMembershipsInboundFacet(`sgos`),
			nations: sgoMembershipsInboundFacet(`nations`),
		},
	},
	{
		$project: {
			gamedayId: { $arrayElemAt: ['$meta.gamedayId', 0] },
			externalKey: { $arrayElemAt: ['$meta.externalKey', 0] },
			resourceType: { $arrayElemAt: ['$meta.resourceType', 0] },
			name: { $arrayElemAt: ['$meta.name', 0] },
			clubIds: { $arrayElemAt: ['$clubs.ids', 0] },
			clubKeys: { $arrayElemAt: ['$clubs.keys', 0] },
			competitionIds: { $arrayElemAt: ['$competitions.ids', 0] },
			competitionKeys: { $arrayElemAt: ['$competitions.keys', 0] },
			sgoIds: { $setUnion: [{ $ifNull: [{ $arrayElemAt: ['$inboundSgos.ids', 0] }, []] }, { $ifNull: [{ $arrayElemAt: ['$outboundSgos.ids', 0] }, []] }] },
			sgoKeys: { $mergeObjects: [{ $ifNull: [{ $arrayElemAt: ['$inboundSgos.keys', 0] }, {}] }, { $ifNull: [{ $arrayElemAt: ['$outboundSgos.keys', 0] }, {}] }] },
			teamIds: { $arrayElemAt: ['$teams.ids', 0] },
			teamKeys: { $arrayElemAt: ['$teams.keys', 0] },
			venueIds: { $arrayElemAt: ['$venues.ids', 0] },
			venueKeys: { $arrayElemAt: ['$venues.keys', 0] },
			nationIds: { $arrayElemAt: ['$nations.ids', 0] },
			nationKeys: { $arrayElemAt: ['$nations.keys', 0] },
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
 * Constructs a MongoDB query object for locating an SGO aggregation document.
 *
 * Creates a query that matches aggregation documents by resource type and external key.
 * The external key format follows the standard pattern used across all resource types.
 *
 * @param {string} sgoId - External identifier for the SGO
 * @param {string} sgoIdScope - External scope identifier for the SGO
 * @returns {Object} MongoDB query object for aggregation document lookup
 */
function queryForSgoAggregationDoc(sgoId, sgoIdScope) {
	return { resourceType: 'sgo', externalKey: `${sgoId}${keySeparator}${sgoIdScope}` };
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { pipeline, queryForSgoAggregationDoc };
