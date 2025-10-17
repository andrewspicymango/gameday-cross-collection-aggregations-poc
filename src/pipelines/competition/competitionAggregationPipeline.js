const { competitionMetaFacet } = require('./competitionMetaFacet');
const { competitionSgosFacet } = require('./competitionSgoFacet');
const { competitionStagesFacet } = require('./competitionStagesFacet');
const { keySeparator } = require('../constants');
const { keyInAggregation } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
/**
 * Builds a MongoDB aggregation pipeline to materialize a competition-level facet.
 * Matches a competition by external id/scope, runs sub-facets (sgos, stages, meta),
 * projects normalized arrays (sgos, sgoKeys, stages, stageKeys) and core meta fields,
 * adds computed fields (targetType, lastUpdated) and merges the result into a materialised collection.
 *
 * @param {Object} config - Runtime configuration (may include mongo.matAggCollectionName).
 * @param {string} COMP_SCOPE - External id scope used to filter the competition.
 * @param {string|number} COMP_ID - External id used to filter the competition.
 * @returns {Array<Object>} A MongoDB aggregation pipeline (stages: $match, $facet, $project, $addFields, $merge).
 * @notes The $facet stage relies on competitionSgoFacet, competitionStagesFacet and competitionMetaFacet.
 * @notes The $merge writes to config.mongo.matAggCollectionName || 'materialisedAggregations',
 *        merging on keyInAggregation with whenMatched:'replace' and whenNotMatched:'insert'.
 */
const pipeline = (config, COMP_SCOPE, COMP_ID) => [
	//////////////////////////////////////////////////////////////////////////////
	//$match: filters by _externalId and _externalIdScope (COMP_ID, COMP_SCOPE)
	{ $match: { _externalId: COMP_ID, _externalIdScope: COMP_SCOPE } },

	//////////////////////////////////////////////////////////////////////////////
	// $facet: runs the provided sub-facets (sgos, stages, events, teams, sportsPersons, venues, meta)
	{
		$facet: {
			sgos: competitionSgosFacet,
			stages: competitionStagesFacet,
			meta: competitionMetaFacet,
		},
	},

	//////////////////////////////////////////////////////////////////////////////
	// $project: extracts the first/meta values and normalizes facet outputs to arrays (defaults to [])
	{
		$project: {
			resourceType: { $first: '$meta.resourceType' },
			externalKey: { $concat: [{ $first: '$meta.competitionId' }, keySeparator, { $first: '$meta.competitionIdScope' }] },
			gamedayId: { $first: '$meta._id' },
			_externalId: { $first: '$meta.competitionId' },
			_externalIdScope: { $first: '$meta.competitionIdScope' },
			name: { $first: '$meta.name' },
			sgos: { $ifNull: [{ $first: '$sgos.ids' }, []] },
			sgoKeys: { $ifNull: [{ $first: '$sgos.keys' }, []] },
			stages: { $ifNull: [{ $first: '$stages.ids' }, []] },
			stageKeys: { $ifNull: [{ $first: '$stages.keys' }, []] },
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
/**
 * Build a Mongo match object (facet) for an aggregation that selects a competition document.
 * Matches resourceType 'competition' and externalKey formed by concatenating competitionId + keySeparator + competitionIdScope.
 * @param {string} competitionId - Primary competition identifier.
 * @param {string} competitionIdScope - Scope/namespace appended to competitionId.
 * @returns {{resourceType: string, externalKey: string}} Query object suitable for a $match stage or facet.
 */
function queryForCompetitionAggregationDoc(competitionId, competitionIdScope) {
	return { resourceType: 'competition', externalKey: `${competitionId}${keySeparator}${competitionIdScope}` };
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { pipeline, queryForCompetitionAggregationDoc };
