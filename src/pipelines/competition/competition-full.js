const { competitionMetaFacet } = require('./competitionMetaFacet');
const { competitionEventsFacet } = require('./competitionEventsFacet');
const { competitionSgoFacet } = require('./competitionSgoFacet');
const { competitionStagesFacet } = require('./competitionStagesFacet');
const { competitionTeamsFacet } = require('./competitionTeamsFacet');
const { competitionSportsPersonsFacet } = require('./competitionSportsPersonsFacet');
const { competitionVenuesFacet } = require('./competitionVenuesFacet');

const targetType = [`sgo`, `stage`, `event`, `venue`, `team`, `sportsPerson`].join('/');
const keyInAggregation = ['resourceType', '_externalIdScope', '_externalId', 'targetType'];

////////////////////////////////////////////////////////////////////////////////
/**
 * Builds a MongoDB aggregation pipeline that selects a competition by external id/scope
 * and assembles related resource id/key lists via a $facet stage.
 *
 * The pipeline:
 *  - $match: filters by _externalId and _externalIdScope (COMP_ID, COMP_SCOPE)
 *  - $facet: runs the provided sub-facets (sgos, stages, events, teams, sportsPersons, venues, meta)
 *  - $project: extracts the first/meta values and normalizes facet outputs to arrays (defaults to [])
 *  - $addFields: sets output metadata (resourceType, _externalId, _externalIdScope, targetType)
 *    and stamps lastUpdated with $$NOW (pipeline execution time)
 *
 * @param {string} COMP_SCOPE - External id scope to match (_externalIdScope).
 * @param {string} COMP_ID - External id to match (_externalId).
 * @returns {Array<Object>} MongoDB aggregation pipeline array of stages.
 */
const pipeline = (COMP_SCOPE, COMP_ID) => [
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
			targetType,
			lastUpdated: '$$NOW', // current pipeline execution time
		},
	},

	//////////////////////////////////////////////////////////////////////////////
	{
		$merge: {
			into: 'materialisedAggregations',
			on: keyInAggregation,
			whenMatched: 'replace',
			whenNotMatched: 'insert',
		},
	},
];

////////////////////////////////////////////////////////////////////////////////
module.exports = pipeline;
