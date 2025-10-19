const { stageMetaFacet } = require('./stageMetaFacet');
const { stageEventsFacet } = require('./stageEventsFacet');
const { stageCompetitionFacet } = require('./stageCompetitionFacet');
const { stageRankingsFacet } = require('./stageRankingsFacet');
const { keySeparator } = require('../constants');
const { keyInAggregation } = require('../constants');

////////////////////////////////////////////////////////////////////////////////
/**
 * Builds a MongoDB aggregation pipeline that materialises a "stage" aggregation document.
 *
 * The pipeline:
 *  - $match: filters documents by provided external stage id and scope
 *  - $facet: runs composed sub-facets (stageMetaFacet, stageCompetitionFacet, stageEventsFacet)
 *  - $project: extracts first values from the meta facet and normalises facet outputs to arrays with defaults
 *  - $addFields: sets stable fields and stamps lastUpdated with the pipeline execution time ($$NOW)
 *  - $merge: writes the resulting aggregation into a materialised collection (replace on match, insert otherwise)
 *
 * @param {Object} config - Runtime configuration; expects config.mongo.matAggCollectionName (optional)
 * @param {string} STAGE_SCOPE - External scope identifier used to match the stage (e.g. league or provider scope)
 * @param {string} STAGE_ID - External id of the stage to materialise
 * @returns {Array<Object>} MongoDB aggregation pipeline array suitable for collection.aggregate(...)
 *
 * @sideEffect Writes/merges a materialised aggregation document into
 *   config.mongo.matAggCollectionName or the default 'materialisedAggregations'.
 *
 * @remarks
 *  - Expects the following variables to be available in the module scope:
 *    stageMetaFacet, stageCompetitionFacet, stageEventsFacet, keySeparator, keyInAggregation.
 *  - Uses {$first: ...} and {$ifNull: [..., []]} to produce stable, deterministic fields.
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
			rankings: stageRankingsFacet,
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
			name: { $first: '$meta.name' },
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
			rankings: {
				$ifNull: [{ $first: '$rankings.ids' }, []],
			},
			rankingKeys: {
				$ifNull: [{ $first: '$rankings.keys' }, []],
			},
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
