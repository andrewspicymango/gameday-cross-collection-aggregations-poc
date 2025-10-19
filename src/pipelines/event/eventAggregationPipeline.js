const { eventMetaFacet } = require('./eventMetaFacet');
const { eventStageFacet } = require('./eventStageFacet');
const { eventVenuesFacet } = require('./eventVenuesFacet');
const { eventTeamsFacet } = require('./eventTeamsFacet');
const { eventSportsPersonsFacet } = require('./eventSportsPersonsFacet');
const { eventKeyMomentsFacet } = require('./eventKeyMomentsFacet');
const { eventRankingsFacet } = require('./eventRankingsFacet');
const { keySeparator } = require('../constants');
const { keyInAggregation } = require('../constants');

////////////////////////////////////////////////////////////////////////////////

const pipeline = (config, EVENT_SCOPE, EVENT_ID) => [
	{ $match: { _externalId: EVENT_ID, _externalIdScope: EVENT_SCOPE } },
	//////////////////////////////////////////////////////////////////////////////
	{
		$facet: {
			meta: eventMetaFacet,
			stages: eventStageFacet,
			venues: eventVenuesFacet,
			teams: eventTeamsFacet,
			sportsPersons: eventSportsPersonsFacet,
			keyMoments: eventKeyMomentsFacet,
			rankings: eventRankingsFacet,
		},
	},
	//////////////////////////////////////////////////////////////////////////////
	{
		$project: {
			resourceType: { $first: '$meta.resourceType' },
			externalKey: { $concat: [{ $first: '$meta.eventId' }, keySeparator, { $first: '$meta.eventIdScope' }] },
			gamedayId: { $first: '$meta._id' },
			_externalId: { $first: '$meta.eventId' },
			_externalIdScope: { $first: '$meta.eventIdScope' },
			name: { $first: '$meta.name' },
			stages: { $ifNull: [{ $first: '$stages.ids' }, []] },
			stageKeys: { $ifNull: [{ $first: '$stages.keys' }, []] },
			venues: { $ifNull: [{ $first: '$venues.ids' }, []] },
			venueKeys: { $ifNull: [{ $first: '$venues.keys' }, []] },
			teams: { $ifNull: [{ $first: '$teams.ids' }, []] },
			teamKeys: { $ifNull: [{ $first: '$teams.keys' }, []] },
			sportsPersons: { $ifNull: [{ $first: '$sportsPersons.ids' }, []] },
			sportsPersonKeys: { $ifNull: [{ $first: '$sportsPersons.keys' }, []] },
			keyMoments: { $ifNull: [{ $first: '$keyMoments.ids' }, []] },
			keyMomentKeys: { $ifNull: [{ $first: '$keyMoments.keys' }, []] },
			rankings: { $ifNull: [{ $first: '$rankings.ids' }, []] },
			rankingKeys: { $ifNull: [{ $first: '$rankings.keys' }, []] },
		},
	},
	{
		$addFields: {
			resourceType: { $toLower: '$resourceType' },
			externalKey: '$externalKey',
			gamedayId: '$gamedayId',
			_externalId: '$_externalId',
			_externalIdScope: '$_externalIdScope',
			name: '$name',
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
function queryForEventAggregationDoc(eventId, eventIdScope) {
	return { resourceType: 'event', externalKey: `${eventId}${keySeparator}${eventIdScope}` };
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { pipeline, queryForEventAggregationDoc };
