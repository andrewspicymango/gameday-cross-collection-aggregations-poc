const { eventMetaFacet } = require('./eventMetaFacet');
const { eventStageFacet } = require('./eventStageFacet');
const { eventCompetitionFacet } = require('./eventCompetitionFacet');
const { eventSgoFacet } = require('./eventSgoFacet');
const { eventVenuesFacet } = require('./eventVenuesFacet');
const { eventTeamsFacet } = require('./eventTeamsFacet');
const { eventSportsPersonsFacet } = require('./eventSportsPersonsFacet');
const { eventKeyMomentsFacet } = require('./eventKeyMomentsFacet');
const targetType = [`sgo`, `competition`, `stage`, `keyMoment`, `venue`, `team`, `sportsPerson`].join('/');
const keyInAggregation = ['resourceType', '_externalIdScope', '_externalId', 'targetType'];

const pipeline = (EVENT_SCOPE, EVENT_ID) => [
	{ $match: { _externalId: EVENT_ID, _externalIdScope: EVENT_SCOPE } },
	{
		$facet: {
			meta: eventMetaFacet,
			sgos: eventSgoFacet,
			competitions: eventCompetitionFacet,
			stages: eventStageFacet,
			venues: eventVenuesFacet,
			teams: eventTeamsFacet,
			sportsPersons: eventSportsPersonsFacet,
			keyMoments: eventKeyMomentsFacet,
		},
	},
	{
		$project: {
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
			_externalId: '$_externalId',
			_externalIdScope: '$_externalIdScope',
			targetType,
			lastUpdated: '$$NOW',
		},
	},
	{
		$merge: {
			into: 'materialisedAggregations',
			on: keyInAggregation,
			whenMatched: 'replace',
			whenNotMatched: 'insert',
		},
	},
];

module.exports = pipeline;
