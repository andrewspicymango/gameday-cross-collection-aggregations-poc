const { stageMetaFacet } = require('./stageMetaFacet');
const { stageEventsFacet } = require('./stageEventsFacet');
const { stageCompetitionFacet } = require('./stageCompetitionFacet');
const stageAggregationTargetType = [`competition`, `event`].join('/');
const keyInAggregation = ['resourceType', '_externalIdScope', '_externalId', 'targetType'];

////////////////////////////////////////////////////////////////////////////////
/**
 * Builds an aggregation pipeline that materialises a "stage" document by:
 *  1. matching a source document by its external id/scope,
 *  2. running several facet sub-pipelines to collect metadata, competition and event references,
 *  3. projecting and normalising facet outputs into a single shaped document,
 *  4. stamping metadata and the pipeline execution time,
 *  5. and merging (upserting) the result into a configured materialised-aggregations collection.
 *
 * The pipeline stages (in order) are:
 *  - $match: filters the input by {_externalId: STAGE_ID, _externalIdScope: STAGE_SCOPE}.
 *  - $facet: executes the supplied facet pipelines and outputs named facets: meta, competitions, events.
 *  - $project: extracts single values from facet results (using $first) and normalises optional arrays with $ifNull.
 *  - $addFields: sets/propagates metadata fields (gamedayId, resourceType, _externalId, _externalIdScope),
 *      sets targetType to a predefined value, and stamps lastUpdated with $$NOW.
 *  - $merge: upserts the final document into the target materialised collection, joining on a precomputed key.
 *
 * @param {Object} config - Optional runtime configuration object.
 * @param {Object} [config.mongo] - Mongo-specific configuration.
 * @param {string} [config.mongo.matAggCollectionName='materialisedAggregations'] - Target collection name for $merge.
 * @param {string|number} STAGE_SCOPE - The external id scope used to identify the stage (stored as _externalIdScope).
 * @param {string|number} STAGE_ID - The external id used to identify the stage (stored as _externalId).
 * @returns {Array<Object>} An array of aggregation stage documents suitable for collection.aggregate(...).
 *
 * External dependencies (must be available in the enclosing module/scope):
 *  - stageMetaFacet: facet pipeline producing meta information (expected fields: _id, stageId, stageIdScope, resourceType).
 *  - stageCompetitionFacet: facet pipeline producing competition ids and keys (expected fields: ids, keys).
 *  - stageEventsFacet: facet pipeline producing event ids and keys (expected fields: ids, keys).
 *  - stageAggregationTargetType: value to set as targetType on the resulting document.
 *  - keyInAggregation: document key specification used by $merge's "on" option.
 *
 * Behavioural notes:
 *  - The $project stage uses $first to pick the primary result from each facet and $ifNull to default missing arrays to [].
 *  - The lastUpdated field is set with $$NOW so it reflects the pipeline execution time on the server.
 *  - $merge uses whenMatched: "replace" and whenNotMatched: "insert", so existing aggregated documents are fully replaced.
 */
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
			gamedayId: { $first: '$meta._id' },
			_externalId: { $first: '$meta.stageId' },
			_externalIdScope: { $first: '$meta.stageIdScope' },
			resourceType: { $first: '$meta.resourceType' },
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
			gamedayId: '$gamedayId',
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
function queryForStageAggregationDoc(stageId, stageIdScope) {
	return { resourceType: 'stage', _externalIdScope: stageIdScope, _externalId: stageId, targetType: stageAggregationTargetType };
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { pipeline, queryForStageAggregationDoc, stageAggregationTargetType };
