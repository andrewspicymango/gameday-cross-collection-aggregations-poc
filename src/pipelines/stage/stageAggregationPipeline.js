const { stageMetaFacet } = require('./stageMetaFacet');
const { stageEventsFacet } = require('./stageEventsFacet');
const { stageCompetitionFacet } = require('./stageCompetitionFacet');
const { keySeparator } = require('../constants');
const { keyInAggregation } = require('../constants');

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
			competitions: stageCompetitionFacet,
			events: stageEventsFacet,
		},
	},

	//////////////////////////////////////////////////////////////////////////////
	// $project: extracts the first/meta values and normalizes facet outputs to arrays (defaults to [])
	{
		$project: {
			resourceType: { $first: '$meta.resourceType' },
			externalKey: { $concat: [{ $first: '$meta.stageId' }, keySeparator, { $first: '$meta.stageIdScope' }] },
			gamedayId: { $first: '$meta._id' },
			_externalId: { $first: '$meta.stageId' },
			_externalIdScope: { $first: '$meta.stageIdScope' },
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
		},
	},

	//////////////////////////////////////////////////////////////////////////////
	// $addFields: sets output metadata (resourceType, _externalId, _externalIdScope, targetType)
	// and stamps lastUpdated with $$NOW (pipeline execution time)
	{
		$addFields: {
			resourceType: '$resourceType',
			externalKey: '$externalKey',
			gamedayId: '$gamedayId',
			_externalId: '$_externalId',
			_externalIdScope: '$_externalIdScope',
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
 * Builds a Mongo facet filter for the stage aggregation document.
 *
 * @param {string} stageId - Identifier for the stage.
 * @param {string} stageIdScope - Scope appended to the stage identifier.
 * @returns {{resourceType: string, externalKey: string}} Query object matching resourceType 'stage' and externalKey `${stageId}${keySeparator}${stageIdScope}`.
 */
function queryForStageAggregationDoc(stageId, stageIdScope) {
	return { resourceType: 'stage', externalKey: `${stageId}${keySeparator}${stageIdScope}` };
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { pipeline, queryForStageAggregationDoc };
