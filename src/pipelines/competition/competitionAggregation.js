const { competitionMetaFacet } = require('./competitionMetaFacet');
const { competitionEventsFacet } = require('./competitionEventsFacet');
const { competitionSgoFacet } = require('./competitionSgoFacet');
const { competitionStagesFacet } = require('./competitionStagesFacet');
const { competitionTeamsFacet } = require('./competitionTeamsFacet');
const { competitionSportsPersonsFacet } = require('./competitionSportsPersonsFacet');
const { competitionVenuesFacet } = require('./competitionVenuesFacet');
const competitionAggregationTargetType = [`sgo`, `stage`, `event`, `venue`, `team`, `sportsPerson`].join('/');
const keyInAggregation = ['resourceType', '_externalIdScope', '_externalId', 'targetType'];

////////////////////////////////////////////////////////////////////////////////

/**
 * Builds a MongoDB aggregation pipeline to materialise a competition document by
 * matching external id/scope, running facets (sgos, stages, events, teams,
 * sportsPersons, venues, meta), normalising outputs, stamping metadata and merging
 * into the configured materialised collection.
 *
 * @param {Object} config - configuration object (expects config.mongo.matAggCollectionName)
 * @param {string} COMP_SCOPE - external id scope to match
 * @param {string|number} COMP_ID - external id to match
 * @returns {Array<Object>} MongoDB aggregation pipeline stages
 */
const pipeline = (config, COMP_SCOPE, COMP_ID) => [
	//////////////////////////////////////////////////////////////////////////////
	//$match: filters by _externalId and _externalIdScope (COMP_ID, COMP_SCOPE)
	{ $match: { _externalId: COMP_ID, _externalIdScope: COMP_SCOPE } },

	//////////////////////////////////////////////////////////////////////////////
	// $facet: runs the provided sub-facets (sgos, stages, events, teams, sportsPersons, venues, meta)
	{
		$facet: {
			sgos: competitionSgoFacet,
			stages: competitionStagesFacet,
			events: competitionEventsFacet,
			teams: competitionTeamsFacet,
			sportsPersons: competitionSportsPersonsFacet,
			venues: competitionVenuesFacet,
			meta: competitionMetaFacet,
		},
	},

	//////////////////////////////////////////////////////////////////////////////
	// $project: extracts the first/meta values and normalizes facet outputs to arrays (defaults to [])
	{
		$project: {
			gamedayId: { $first: '$meta._id' },
			_externalId: { $first: '$meta.competitionId' },
			_externalIdScope: { $first: '$meta.competitionIdScope' },
			resourceType: { $first: '$meta.resourceType' },
			sgos: {
				$ifNull: [{ $first: '$sgos.ids' }, []],
			},
			sgoKeys: {
				$ifNull: [{ $first: '$sgos.keys' }, []],
			},
			stages: {
				$ifNull: [{ $first: '$stages.ids' }, []],
			},
			stageKeys: {
				$ifNull: [{ $first: '$stages.keys' }, []],
			},
			events: {
				$ifNull: [{ $first: '$events.ids' }, []],
			},
			eventKeys: {
				$ifNull: [{ $first: '$events.keys' }, []],
			},
			teams: {
				$ifNull: [{ $first: '$teams.ids' }, []],
			},
			teamKeys: {
				$ifNull: [{ $first: '$teams.keys' }, []],
			},
			sportsPersons: {
				$ifNull: [{ $first: '$sportsPersons.ids' }, []],
			},
			sportsPersonKeys: {
				$ifNull: [{ $first: '$sportsPersons.keys' }, []],
			},
			venues: {
				$ifNull: [{ $first: '$venues.ids' }, []],
			},
			venueKeys: {
				$ifNull: [{ $first: '$venues.keys' }, []],
			},
		},
	},

	//////////////////////////////////////////////////////////////////////////////
	// $addFields: sets output metadata (resourceType, _externalId, _externalIdScope, targetType)
	// and stamps lastUpdated with $$NOW (pipeline execution time)
	{
		$addFields: {
			resourceType: '$resourceType',
			_externalId: '$_externalId',
			_externalIdScope: '$_externalIdScope',
			targetType: competitionAggregationTargetType,
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
 * Build a query object to locate a merged competition document.
 *
 * The returned object is intended for use with the application's storage/query layer
 * and contains the fixed resourceType "competition" together with the provided
 * external identifier and its scope.
 *
 * @param {string} competitionId - External competition identifier to match against the `_externalId` field.
 * @param {string} competitionIdScope - Scope or namespace for the external identifier, used for `_externalIdScope`.
 * @returns {{resourceType: string, _externalId: string, _externalIdScope: string, targetType: *}} Object representing the query:
 *  - resourceType: 'competition'
 *  - _externalId: compId
 *  - _externalIdScope: compScope
 *  - targetType: value taken from the surrounding scope
 */
function getCompetitionQueryToFindMergedDocument(competitionId, competitionIdScope) {
	return { resourceType: 'competition', _externalIdScope: competitionIdScope, _externalId: competitionId, targetType: competitionAggregationTargetType };
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { pipeline, getCompetitionQueryToFindMergedDocument, competitionAggregationTargetType };
