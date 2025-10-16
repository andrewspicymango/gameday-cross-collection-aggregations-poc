////////////////////////////////////////////////////////////////////////////////
const _ = require('lodash');
const { debug } = require('../../log');
const { runPipeline } = require('../runPipeline');
const { pipeline, queryForSgoAggregationDoc } = require('./sgoAggregationPipeline');
const { buildOperationsForReferenceChange } = require('../referenceManagement');
const { executeOperationsForReferenceChange } = require('../referenceManagement');

////////////////////////////////////////////////////////////////////////////////
/**
 * Builds or rebuilds SGO aggregation documents and synchronizes cross-references.
 *
 * This function performs a complete aggregation build for an SGO, capturing all
 * related resources (competitions, stages, events, teams, sports persons, venues) and
 * maintaining referential integrity across the materialized aggregation collection.
 *
 * Process flow:
 * 1. Validates SGO existence to avoid expensive pipeline execution on missing data
 * 2. Captures current aggregation state (old keys) before rebuilding
 * 3. Executes aggregation pipeline to rebuild SGO materialized view
 * 4. Compares old vs new keys to identify relationship changes
 * 5. Updates cross-references in related resource aggregations:
 *    - Competition aggregations (add/remove SGO reference for membership changes)
 *    - Team aggregations (add/remove SGO reference for membership changes)
 *    - Venue aggregations (add/remove SGO reference for membership changes)
 *
 * The function ensures that when an SGO changes its membership associations,
 * all affected aggregation documents are updated to maintain consistency.
 *
 * @async
 * @param {Object} config - Configuration containing mongo.matAggCollectionName
 * @param {Object} mongo - MongoDB connection with db.collection access
 * @param {string} sgoIdScope - External scope identifier for the SGO
 * @param {string} sgoId - External identifier for the SGO
 * @param {string} requestId - Unique identifier for request tracking/logging
 * @returns {Promise<Object|number>} New aggregation document or 404 if SGO not found
 * @throws {Error} If configuration is invalid or required parameters are missing
 */
async function processSgo(config, mongo, sgoIdScope, sgoId, requestId) {
	if (!_.isString(config?.mongo?.matAggCollectionName)) throw new Error('Invalid configuration: config.mongo.matAggCollectionName must be a string');
	if (!sgoId || !sgoIdScope) throw new Error('Invalid parameters: sgoId and sgoIdScope are required');
	//////////////////////////////////////////////////////////////////////////////
	debug(`sgoIdScope=${sgoIdScope}, sgoId=${sgoId}`, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// Check if SGO exists before running expensive pipeline
	const sgoExists = await mongo.db.collection('sgos').countDocuments({ _externalId: sgoId, _externalIdScope: sgoIdScope }, { limit: 1 });
	//////////////////////////////////////////////////////////////////////////////
	if (sgoExists === 0) {
		debug(`SGO not found: ${sgoId}@${sgoIdScope}`, requestId);
		return 404;
	}

	//////////////////////////////////////////////////////////////////////////////
	const pipelineObj = pipeline(config, sgoIdScope, sgoId);
	const sgoAggregationDocQuery = queryForSgoAggregationDoc(sgoId, sgoIdScope);
	//////////////////////////////////////////////////////////////////////////////
	// Retrieve the previous version of the SGO aggregation
	const oldAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(sgoAggregationDocQuery);
	//////////////////////////////////////////////////////////////////////////////
	// Build the SGO aggregation view
	await runPipeline(mongo, 'sgos', pipelineObj, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// Retrieve the new version of the SGO aggregation and calculate new outbound keys
	const newAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(sgoAggregationDocQuery);
	//////////////////////////////////////////////////////////////////////////////
	const operations = buildOperationsForReferenceChange(oldAggregationDoc, newAggregationDoc);
	await executeOperationsForReferenceChange(mongo, config, operations, requestId);

	//////////////////////////////////////////////////////////////////////////////
	return newAggregationDoc;
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { processSgo };
