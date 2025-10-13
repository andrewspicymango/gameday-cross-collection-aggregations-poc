const { sgoMetaFacet } = require('./sgoMetaFacet');
const { sgoCompetitionsFacet } = require('./sgoCompetitionFacet');
const { sgoTeamsFacet } = require('./sgoTeamsFacet');
const { sgoVenuesFacet } = require('./sgoVenuesFacet');
const { sgoClubsFacet } = require('./sgoClubsFacet');
const { sgoSgosFacet } = require('./sgoSgosFacet');
const { keyInAggregation } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
const pipeline = (config, SGO_SCOPE, SGO_ID) => [
	//////////////////////////////////////////////////////////////////////////////
	//$match: filters by _externalId and _externalIdScope (COMP_ID, COMP_SCOPE)
	{ $match: { _externalId: SGO_ID, _externalIdScope: SGO_SCOPE } },
	{
		$facet: {
			meta: sgoMetaFacet,
			competitions: sgoCompetitionsFacet,
			teams: sgoTeamsFacet,
			clubs: sgoClubsFacet,
			venues: sgoVenuesFacet,
			sgos: sgoSgosFacet,
		},
	},
	{
		$project: {
			gamedayId: { $arrayElemAt: ['$meta.gamedayId', 0] },
			externalKey: { $arrayElemAt: ['$meta.externalKey', 0] },
			resourceType: { $arrayElemAt: ['$meta.resourceType', 0] },
			name: { $arrayElemAt: ['$meta.name', 0] },
			competitionIds: { $arrayElemAt: ['$competitions.ids', 0] },
			competitionKeys: { $arrayElemAt: ['$competitions.keys', 0] },
			clubIds: { $arrayElemAt: ['$clubs.ids', 0] },
			clubKeys: { $arrayElemAt: ['$clubs.keys', 0] },
			sgoIds: { $arrayElemAt: ['$sgos.ids', 0] },
			sgoKeys: { $arrayElemAt: ['$sgos.keys', 0] },
			teamIds: { $arrayElemAt: ['$teams.ids', 0] },
			teamKeys: { $arrayElemAt: ['$teams.keys', 0] },
			venueIds: { $arrayElemAt: ['$venues.ids', 0] },
			venueKeys: { $arrayElemAt: ['$venues.keys', 0] },
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
	const { keySeparator } = require('../constants');
	return {
		resourceType: 'sgo',
		externalKey: `${sgoId}${keySeparator}${sgoIdScope}`,
	};
}

////////////////////////////////////////////////////////////////////////////////
module.exports = {
	pipeline,
	queryForSgoAggregationDoc,
};
