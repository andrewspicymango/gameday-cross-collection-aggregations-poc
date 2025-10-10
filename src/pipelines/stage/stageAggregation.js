const { stageMetaFacet } = require('./stageMetaFacet');
const { stageEventsFacet } = require('./stageEventsFacet');
const { stageSgoFacet } = require('./stageSgoFacet');
const { stageCompetitionFacet } = require('./stageCompetitionFacet');
const { stageTeamsFacet } = require('./stageTeamsFacet');
const { stageSportsPersonsFacet } = require('./stageSportsPersonsFacet');
const { stageVenuesFacet } = require('./stageVenuesFacet');

const stageAggregationTargetType = [`sgo`, `competition`, `event`, `venue`, `team`, `sportsPerson`].join('/');
const keyInAggregation = ['resourceType', '_externalIdScope', '_externalId', 'targetType'];

////////////////////////////////////////////////////////////////////////////////
const pipeline = (config, STAGE_SCOPE, STAGE_ID) => [
	//////////////////////////////////////////////////////////////////////////////
	//$match: filters by _externalId and _externalIdScope (COMP_ID, COMP_SCOPE)
	{ $match: { _externalId: STAGE_ID, _externalIdScope: STAGE_SCOPE } },

	//////////////////////////////////////////////////////////////////////////////
	// $facet: runs the provided sub-facets (sgos, stages, events, teams, sportsPersons, venues, meta)
	{
		$facet: {
			meta: stageMetaFacet,
			sgos: stageSgoFacet,
			competitions: stageCompetitionFacet,
			events: stageEventsFacet,
			teams: stageTeamsFacet,
			sportsPersons: stageSportsPersonsFacet,
			venues: stageVenuesFacet,
		},
	},

	//////////////////////////////////////////////////////////////////////////////
	// $project: extracts the first/meta values and normalizes facet outputs to arrays (defaults to [])
	{
		$project: {
			gamedayId: { $first: '$meta._id' },
			_externalId: { $first: '$meta.stageId' },
			_externalIdScope: { $first: '$meta.stageIdScope' },
			resourceType: { $first: '$meta.resourceType' },
			sgos: {
				$ifNull: [{ $first: '$sgos.ids' }, []],
			},
			sgoKeys: {
				$ifNull: [{ $first: '$sgos.keys' }, []],
			},
			competitions: {
				$ifNull: [{ $first: '$competitions.ids' }, []],
			},
			competitionKeys: {
				$ifNull: [{ $first: '$competitions.keys' }, []],
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
			targetType: stageAggregationTargetType,
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
function getStageQueryToFindMergedDocument(stageId, stageIdScope) {
	return { resourceType: 'stage', _externalIdScope: stageIdScope, _externalId: stageId, targetType: stageAggregationTargetType };
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { pipeline, getStageQueryToFindMergedDocument, stageAggregationTargetType };
