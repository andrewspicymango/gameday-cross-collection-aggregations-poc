const _ = require('lodash');
const { debug } = require('../../log');
const { runPipeline } = require('../runPipeline');
const { pipeline } = require('./stageAggregationPipeline');
const { queryForStageAggregationDoc } = require('./stageAggregationPipeline');
const { buildOperationsForReferenceChange } = require('../referenceManagement');
const { executeOperationsForReferenceChange } = require('../referenceManagement');

////////////////////////////////////////////////////////////////////////////////
/**
 * Processes a stage aggregation by building and updating the materialized aggregation data.
 *
 * @async
 * @function processStage
 * @param {Object} config - Configuration object containing MongoDB settings
 * @param {string} config.mongo.matAggCollectionName - Name of the materialized aggregation collection
 * @param {Object} mongo - MongoDB connection object with db property
 * @param {string} stageIdScope - External ID scope for the stage
 * @param {string} stageId - External ID of the stage to process
 * @param {string} requestId - Unique identifier for tracking the request
 * @returns {Promise<Object|number>} Returns the new aggregation document on success, or 404 if stage not found
 * @throws {Error} Throws error if configuration is invalid or required parameters are missing
 *
 * @description
 * 1. Validates configuration and parameters
 * 2. Checks if the stage exists in the database
 * 3. Retrieves the current aggregation document (if exists)
 * 4. Runs the aggregation pipeline to build new data
 * 5. Compares old vs new aggregation and updates references accordingly
 */
async function processStage(config, mongo, stageIdScope, stageId, requestId) {
	if (!_.isString(config?.mongo?.matAggCollectionName)) throw new Error('Invalid configuration: config.mongo.matAggCollectionName must be a string');
	if (!stageId || !stageIdScope) throw new Error('Invalid parameters: stageId and stageIdScope are required');
	//////////////////////////////////////////////////////////////////////////////
	debug(`stageIdScope=${stageIdScope}, stageId=${stageId}`, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// Check if competition exists before running expensive pipeline
	const stageExists = await mongo.db.collection('stages').countDocuments({ _externalId: stageId, _externalIdScope: stageIdScope }, { limit: 1 });
	//////////////////////////////////////////////////////////////////////////////
	if (stageExists === 0) {
		debug(`Stage not found: ${stageId}@${stageIdScope}`, requestId);
		return 404;
	}
	//////////////////////////////////////////////////////////////////////////////
	const pipelineObj = pipeline(config, stageIdScope, stageId);
	const stageAggregationDocQuery = queryForStageAggregationDoc(stageId, stageIdScope);
	//////////////////////////////////////////////////////////////////////////////
	// Retrieve the previous version of the stage aggregation (if it exists)
	const oldAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(stageAggregationDocQuery);
	//////////////////////////////////////////////////////////////////////////////
	// Build the stage aggregation view
	await runPipeline(mongo, 'stages', pipelineObj, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// Retrieve the new version of the stage aggregation
	const newAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(stageAggregationDocQuery);
	//////////////////////////////////////////////////////////////////////////////
	// Compare old and new aggregation documents to determine if references need to be updated
	const operations = buildOperationsForReferenceChange(oldAggregationDoc, newAggregationDoc);
	await executeOperationsForReferenceChange(mongo, config, operations, requestId);
	//////////////////////////////////////////////////////////////////////////////
	return newAggregationDoc;
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { processStage };
