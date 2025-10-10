const { eventMetaFacet } = require('./eventMetaFacet');
const { eventStageFacet } = require('./eventStageFacet');
const { eventCompetitionFacet } = require('./eventCompetitionFacet');
const { eventSgoFacet } = require('./eventSgoFacet');
const { eventVenuesFacet } = require('./eventVenuesFacet');
const { eventTeamsFacet } = require('./eventTeamsFacet');
const { eventSportsPersonsFacet } = require('./eventSportsPersonsFacet');
const eventAggregationTargetType = [`sgo`, `competition`, `stage`, `venue`, `team`, `sportsPerson`].join('/');
const keyInAggregation = ['resourceType', '_externalIdScope', '_externalId', 'targetType'];

////////////////////////////////////////////////////////////////////////////////
const pipeline = (config, EVENT_SCOPE, EVENT_ID) => [
	{ $match: { _externalId: EVENT_ID, _externalIdScope: EVENT_SCOPE } },
	//////////////////////////////////////////////////////////////////////////////
	{
		$facet: {
			meta: eventMetaFacet,
			sgos: eventSgoFacet,
			competitions: eventCompetitionFacet,
			stages: eventStageFacet,
			venues: eventVenuesFacet,
			teams: eventTeamsFacet,
			sportsPersons: eventSportsPersonsFacet,
		},
	},
	//////////////////////////////////////////////////////////////////////////////
	{
		$project: {
			gamedayId: { $first: '$meta._id' },
			_externalId: { $first: '$meta.eventId' },
			_externalIdScope: { $first: '$meta.eventIdScope' },
			resourceType: { $first: '$meta.resourceType' },
			sgos: { $ifNull: [{ $first: '$sgos.ids' }, []] },
			sgoKeys: { $ifNull: [{ $first: '$sgos.keys' }, []] },
			competitions: { $ifNull: [{ $first: '$competitions.ids' }, []] },
			competitionKeys: { $ifNull: [{ $first: '$competitions.keys' }, []] },
			stages: { $ifNull: [{ $first: '$stages.ids' }, []] },
			stageKeys: { $ifNull: [{ $first: '$stages.keys' }, []] },
			venues: { $ifNull: [{ $first: '$venues.ids' }, []] },
			venueKeys: { $ifNull: [{ $first: '$venues.keys' }, []] },
			teams: { $ifNull: [{ $first: '$teams.ids' }, []] },
			teamKeys: { $ifNull: [{ $first: '$teams.keys' }, []] },
			sportsPersons: { $ifNull: [{ $first: '$sportsPersons.ids' }, []] },
			sportsPersonKeys: { $ifNull: [{ $first: '$sportsPersons.keys' }, []] },
		},
	},

	{
		$addFields: {
			resourceType: '$resourceType',
			_externalId: '$_externalId',
			_externalIdScope: '$_externalIdScope',
			targetType: eventAggregationTargetType,
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
function getEventQueryToFindMergedDocument(eventId, eventIdScope) {
	return { resourceType: 'event', _externalIdScope: eventIdScope, _externalId: eventId, targetType: eventAggregationTargetType };
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { pipeline, getEventQueryToFindMergedDocument, eventAggregationTargetType };
