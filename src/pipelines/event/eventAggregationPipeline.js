const { eventMetaFacet } = require('./eventMetaFacet');
const { eventStageFacet } = require('./eventStageFacet');
const { eventCompetitionFacet } = require('./eventCompetitionFacet');
const { eventSgoFacet } = require('./eventSgoFacet');
const { eventVenuesFacet } = require('./eventVenuesFacet');
const { eventTeamsFacet } = require('./eventTeamsFacet');
const { eventSportsPersonsFacet } = require('./eventSportsPersonsFacet');
const { eventKeyMomentsFacet } = require('./eventKeyMomentsFacet');
const eventAggregationTargetType = [`stage`, `venue`, `team`, `sportsPerson`, `keyMoment`].join('/');
const keyInAggregation = ['resourceType', '_externalIdScope', '_externalId', 'targetType'];

////////////////////////////////////////////////////////////////////////////////
/**
 * Creates a MongoDB aggregation pipeline that materialises a single event's aggregated
 * relationships and metadata into a target collection.
 *
 * The pipeline:
 * 1. Filters documents to the given event by matching `_externalId` and `_externalIdScope`.
 * 2. Runs a `$facet` to produce separate facets for event metadata, stages, venues,
 *    teams, sports persons and key moments. (The facet stage definitions are expected
 *    to be provided externally: `eventMetaFacet`, `eventStageFacet`, `eventVenuesFacet`,
 *    `eventTeamsFacet`, `eventSportsPersonsFacet`, `eventKeyMomentsFacet`.)
 * 3. `$project`s a single aggregated document using `$first`, `$ifNull` and `$setUnion`
 *    to build canonical arrays and keys:
 *    - gamedayId from `meta._id`
 *    - external id and scope from meta
 *    - resourceType from meta
 *    - canonical arrays for stages, venues, teams, sportsPersons and keyMoments
 *    - unions teams and sports persons with any ids referenced from key moments
 * 4. Adds standard fields via `$addFields` including `targetType` (from external
 *    `eventAggregationTargetType`) and `lastUpdated` set to `$$NOW`.
 * 5. `$merge`s (upserts) the resulting document into a materialised aggregation
 *    collection (default 'materialisedAggregations' or `config.mongo.matAggCollectionName`),
 *    using the join key defined by the external `keyInAggregation` expression.
 *
 * Important external dependencies:
 * - eventMetaFacet, eventStageFacet, eventVenuesFacet, eventTeamsFacet,
 *   eventSportsPersonsFacet, eventKeyMomentsFacet: facet definitions used in `$facet`.
 * - eventAggregationTargetType: value used for the `targetType` field.
 * - keyInAggregation: join key specification used by `$merge`.
 *
 * Parameters:
 * @param {Object} config - Configuration object. Optional nested path:
 *                          `config.mongo.matAggCollectionName` (string) to override
 *                          the target collection name for the `$merge` stage.
 * @param {string|number} EVENT_SCOPE - The external id scope value to match (`_externalIdScope`).
 * @param {string|number} EVENT_ID - The external id value to match (`_externalId`).
 *
 * Returns:
 * @returns {Array<Object>} A MongoDB aggregation pipeline (array of stage documents)
 *                          ready to be executed with Collection.aggregate().
 *
 * Side effects:
 * - Performs a $merge which will insert or replace a document in the target collection.
 * - Uses server variable `$$NOW` to populate `lastUpdated`.
 */
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
			gamedayId: { $first: '$meta._id' },
			_externalId: { $first: '$meta.eventId' },
			_externalIdScope: { $first: '$meta.eventIdScope' },
			resourceType: { $first: '$meta.resourceType' },
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
			gamedayId: '$gamedayId',
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
function queryForEventAggregationDoc(eventId, eventIdScope) {
	return { resourceType: 'event', _externalIdScope: eventIdScope, _externalId: eventId, targetType: eventAggregationTargetType };
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { pipeline, queryForEventAggregationDoc, eventAggregationTargetType };
