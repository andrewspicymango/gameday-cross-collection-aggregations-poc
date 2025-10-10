const { competitionMetaFacet } = require('./competitionMetaFacet');
const { competitionSgoFacet } = require('./competitionSgoFacet');
const { competitionStagesFacet } = require('./competitionStagesFacet');
const competitionAggregationTargetType = [`sgo`, `stage`].join('/');
const keyInAggregation = ['resourceType', '_externalIdScope', '_externalId', 'targetType'];

////////////////////////////////////////////////////////////////////////////////
/**
 * Builds a MongoDB aggregation pipeline for producing a materialised competition aggregation document.
 *
 * The pipeline:
 * 1. $match: Filters the collection by the provided external competition identifier and scope.
 * 2. $facet: Executes multiple parallel sub-pipelines (facets) to produce structured pieces of data:
 *    - competitionSgoFacet (sgos)
 *    - competitionStagesFacet (stages)
 *    - competitionMetaFacet (meta)
 * 3. $project: Normalises the facet results by extracting the first/meta values and ensuring expected
 *    outputs are arrays (defaults to empty arrays). Projects:
 *    - gamedayId        : first value from meta._id
 *    - _externalId      : first value from meta.competitionId
 *    - _externalIdScope : first value from meta.competitionIdScope
 *    - resourceType     : first value from meta.resourceType
 *    - sgos             : first sgos.ids or []
 *    - sgoKeys          : first sgos.keys or []
 *    - stages           : first stages.ids or []
 *    - stageKeys        : first stages.keys or []
 * 4. $addFields: Adds/normalises top-level fields for the aggregation document:
 *    - preserves gamedayId, resourceType, _externalId, _externalIdScope
 *    - sets targetType to competitionAggregationTargetType (external constant)
 *    - sets lastUpdated to $$NOW (pipeline execution time)
 * 5. $merge: Writes the resulting document into a materialised aggregations collection (config-driven name
 *    or 'materialisedAggregations' fallback). Merge behavior:
 *    - into: config?.mongo?.matAggCollectionName || 'materialisedAggregations'
 *    - on: keyInAggregation (external key definition)
 *    - whenMatched: 'replace'
 *    - whenNotMatched: 'insert'
 *
 * Notes:
 * - The pipeline depends on external constants/objects: competitionSgoFacet, competitionStagesFacet,
 *   competitionMetaFacet, competitionAggregationTargetType, and keyInAggregation. These must be in scope
 *   where the pipeline is constructed/executed.
 * - lastUpdated uses the aggregation variable $$NOW to capture the pipeline execution time.
 *
 * @function pipeline
 * @param {Object} config - Optional runtime configuration. Expected shape (partial):
 *   { mongo: { matAggCollectionName?: string } }
 * @param {string|number} COMP_SCOPE - External identifier scope used to filter competition documents.
 * @param {string|number} COMP_ID - External competition identifier used to filter competition documents.
 * @returns {Array<Object>} MongoDB aggregation pipeline (array of stages) that, when executed,
 *   produces/updates a single materialised aggregation document for the specified competition.
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
		},
	},

	//////////////////////////////////////////////////////////////////////////////
	{
		$addFields: {
			gamedayId: '$gamedayId',
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
function queryForCompetitionAggregationDoc(competitionId, competitionIdScope) {
	return { resourceType: 'competition', _externalIdScope: competitionIdScope, _externalId: competitionId, targetType: competitionAggregationTargetType };
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { pipeline, queryForCompetitionAggregationDoc, competitionAggregationTargetType };
