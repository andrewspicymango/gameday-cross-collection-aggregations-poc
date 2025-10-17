const { venuesInboundFacet } = require('./venuesInboundFacet.js');
const { venuesSgosFacet } = require('./venuesSgosFacet.js');
const { venuesMetaFacet } = require('./venuesMetaFacet.js');
const { keySeparator } = require('../constants');
const { keyInAggregation } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
const pipeline = (config, VENUE_SCOPE, VENUE_ID) => [
	//////////////////////////////////////////////////////////////////////////////
	//$match: filters by _externalId and _externalIdScope (COMP_ID, COMP_SCOPE)
	{ $match: { _externalId: VENUE_ID, _externalIdScope: VENUE_SCOPE } },

	//////////////////////////////////////////////////////////////////////////////
	// $facet: runs the provided sub-facets (sgos, stages, events, teams, sportsPersons, venues, meta)
	{
		$facet: {
			clubs: venuesInboundFacet(`clubs`),
			events: venuesInboundFacet(`events`),
			nations: venuesInboundFacet(`nations`),
			teams: venuesInboundFacet(`teams`),
			outboundSgos: venuesSgosFacet,
			inboundSgos: venuesInboundFacet(`sgos`),
			meta: venuesMetaFacet,
		},
	},

	//////////////////////////////////////////////////////////////////////////////
	// $project: extracts the first/meta values and normalizes facet outputs to arrays (defaults to [])
	{
		$project: {
			resourceType: { $first: '$meta.resourceType' },
			externalKey: { $concat: [{ $first: '$meta.venueId' }, keySeparator, { $first: '$meta.venueIdScope' }] },
			gamedayId: { $first: '$meta._id' },
			_externalId: { $first: '$meta.venueId' },
			_externalIdScope: { $first: '$meta.venueIdScope' },
			name: { $first: '$meta.name' },
			clubs: { $arrayElemAt: ['$clubs.ids', 0] },
			clubKeys: { $arrayElemAt: ['$clubs.keys', 0] },
			events: { $arrayElemAt: ['$events.ids', 0] },
			eventKeys: { $arrayElemAt: ['$events.keys', 0] },
			nations: { $arrayElemAt: ['$nations.ids', 0] },
			nationKeys: { $arrayElemAt: ['$nations.keys', 0] },
			teams: { $arrayElemAt: ['$teams.ids', 0] },
			teamKeys: { $arrayElemAt: ['$teams.keys', 0] },
			sgos: { $setUnion: [{ $ifNull: [{ $arrayElemAt: ['$inboundSgos.ids', 0] }, []] }, { $ifNull: [{ $arrayElemAt: ['$outboundSgos.ids', 0] }, []] }] },
			sgoKeys: { $mergeObjects: [{ $ifNull: [{ $arrayElemAt: ['$inboundSgos.keys', 0] }, {}] }, { $ifNull: [{ $arrayElemAt: ['$outboundSgos.keys', 0] }, {}] }] },
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
function queryForVenueAggregationDoc(venueId, venueIdScope) {
	return { resourceType: 'venue', externalKey: `${venueId}${keySeparator}${venueIdScope}` };
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { pipeline, queryForVenueAggregationDoc };
