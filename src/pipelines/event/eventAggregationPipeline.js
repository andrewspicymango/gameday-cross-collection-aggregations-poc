const { eventMetaFacet } = require('./eventMetaFacet');
const { eventStageFacet } = require('./eventStageFacet');
const { eventVenuesFacet } = require('./eventVenuesFacet');
const { eventTeamsFacet } = require('./eventTeamsFacet');
const { eventSportsPersonsFacet } = require('./eventSportsPersonsFacet');
const { eventKeyMomentsFacet } = require('./eventKeyMomentsFacet');
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
			teams: {
				$setUnion: [{ $ifNull: [{ $first: '$teams.ids' }, []] }, { $ifNull: [{ $first: '$keyMoments.teamIds' }, []] }],
			},
			teamKeys: {
				$setUnion: [{ $ifNull: [{ $first: '$teams.keys' }, []] }, { $ifNull: [{ $first: '$keyMoments.teamKeys' }, []] }],
			},
			sportsPersons: {
				$setUnion: [{ $ifNull: [{ $first: '$sportsPersons.ids' }, []] }, { $ifNull: [{ $first: '$keyMoments.sportsPersonIds' }, []] }],
			},
			sportsPersonKeys: {
				$setUnion: [{ $ifNull: [{ $first: '$sportsPersons.keys' }, []] }, { $ifNull: [{ $first: '$keyMoments.sportsPersonKeys' }, []] }],
			},
			keyMoments: { $ifNull: [{ $first: '$keyMoments.keyMomentIds' }, []] },
			keyMomentKeys: { $ifNull: [{ $first: '$keyMoments.keyMomentKeys' }, []] },
		},
	},
	{
		$addFields: {
			resourceType: '$resourceType',
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
