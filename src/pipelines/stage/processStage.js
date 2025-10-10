////////////////////////////////////////////////////////////////////////////////
// When a stage is created or updated, we need to build its aggregated view we need to:
// - build the full materialised view for the stage
// - we also need to update any competitions that reference this stage
//   (as they have a list of stage ids/keys in their materialised view, and this stage
//   may be new to that list or moved from a previous competition to a new one)
//
// The process is:
// 1. Retrieve the previous version of the stage aggregation (if it exists) to determine the competition
//    it was previously associated with (if any)
// 2. Build the stage aggregation view
// 3. If the stage was previously associated with a competition, rebuild that competition's
//    aggregation view to ensure it is up to date
// 4. If the stage is now associated with a different competition, rebuild that competition's
//    aggregation view to ensure it is up to date
////////////////////////////////////////////////////////////////////////////////
const _ = require('lodash');
const { keySeparator } = require('../constants');
const { processCompetition } = require('../competition/processCompetition');
const { pipeline, getStageQueryToFindMergedDocument } = require('./stage-full');
const runPipeline = require('../runPipeline');

////////////////////////////////////////////////////////////////////////////////
/**
 * Process and rebuild a single stage aggregation and, if required, trigger
 * rebuilds of affected competition aggregations.
 *
 * This async function:
 * - Validates that config.mongo.matAggCollectionName is provided as a string.
 * - Returns null immediately if either stageId or stageIdScope is falsy.
 * - Constructs a pipeline for the given stage and a query to locate the
 *   merged stage aggregation document.
 * - Fetches the existing (old) stage aggregation document from the configured
 *   materialized aggregation collection and derives the old competition key
 *   (and its split parts using the module-level `keySeparator`).
 * - Executes the stage aggregation pipeline to rebuild the stage aggregation
 *   view (via `runPipeline`).
 * - Fetches the newly produced (new) stage aggregation document and derives
 *   the new competition key and split parts.
 * - If the external competition id or its scope has changed between the old
 *   and new aggregation, invokes `processCompetition` for both the old and
 *   new competition identifiers (to rebuild affected competition aggregations).
 *
 * Side effects:
 * - Reads and writes to the configured materialized aggregation collection.
 * - Invokes external functions: `pipeline`, `getStageQueryToFindMergedDocument`,
 *   `runPipeline`, and `processCompetition`.
 * - Relies on a module-level `keySeparator` to split competition keys.
 *
 * @async
 * @function processStage
 * @param {Object} config - Application configuration object.
 * @param {Object} config.mongo - Mongo-related configuration.
 * @param {string} config.mongo.matAggCollectionName - Name of the materialized aggregation collection.
 * @param {Object} mongo - Mongo access object exposing `db.collection(...).findOne(...)` (e.g., a Mongo client/DB wrapper).
 * @param {string} stageIdScope - The scope/namespace for the stage identifier.
 * @param {string} stageId - The identifier of the stage to process.
 * @param {string} [requestId] - Optional request identifier used for logging/tracing while running the pipeline.
 * @returns {Promise<Object|null>} Resolves with the newly built stage aggregation document, or null if processing is skipped
 *                                 due to missing `stageId` or `stageIdScope`.
 * @throws {Error} If config.mongo.matAggCollectionName is not a string, or if underlying Mongo or pipeline operations fail.
 */
async function processStage(config, mongo, stageIdScope, stageId, requestId) {
	if (!_.isString(config?.mongo?.matAggCollectionName)) throw new Error('Invalid configuration: config.mongo.matAggCollectionName must be a string');
	if (!stageId || !stageIdScope) return null;
	//////////////////////////////////////////////////////////////////////////////
	const pipelineObj = pipeline(config, stageIdScope, stageId);
	const stageAggregationDocQuery = getStageQueryToFindMergedDocument(stageId, stageIdScope);
	//////////////////////////////////////////////////////////////////////////////
	// Retrieve the previous version of the stage aggregation (if it exists) and calculate old competition keys
	const oldAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(stageAggregationDocQuery);
	const oldCompetitionKey = oldAggregationDoc?.competitionKeys[0] || null;
	const oldExternalCompetitionId = oldCompetitionKey ? oldCompetitionKey.split(keySeparator)[0] || null : null;
	const oldExternalCompetitionIdScope = oldCompetitionKey ? oldCompetitionKey.split(keySeparator)[1] || null : null;
	//////////////////////////////////////////////////////////////////////////////
	// Build the stage aggregation view
	await runPipeline(mongo, 'stages', pipelineObj, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// Retrieve the new version of the stage aggregation and calculate new competition keys
	const newAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(stageAggregationDocQuery);
	const newCompetitionKey = newAggregationDoc?.competitionKeys[0] || null;
	const newExternalCompetitionId = newCompetitionKey ? newCompetitionKey.split(keySeparator)[0] || null : null;
	const newExternalCompetitionIdScope = newCompetitionKey ? newCompetitionKey.split(keySeparator)[1] || null : null;
	//////////////////////////////////////////////////////////////////////////////
	// We have a different competition associated with this stage, so we need to rebuild both the old and new competitions
	if (oldExternalCompetitionId != newExternalCompetitionId || oldExternalCompetitionIdScope != newExternalCompetitionIdScope) {
		await processCompetition(config, mongo, oldExternalCompetitionIdScope, oldExternalCompetitionId, requestId);
		await processCompetition(config, mongo, newExternalCompetitionIdScope, newExternalCompetitionId, requestId);
	}
	return newAggregationDoc;
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { processStage };
