////////////////////////////////////////////////////////////////////////////////
const _ = require('lodash');
const { debug, warn } = require('../../log');
const { runPipeline } = require('../runPipeline');
const { pipeline, queryForSgoAggregationDoc } = require('./sgoAggregationPipeline');
const { buildOperationsForReferenceChange } = require('../referenceManagement');
const { executeOperationsForReferenceChange } = require('../referenceManagement');

////////////////////////////////////////////////////////////////////////////////
/**
 * Processes a single SGO (Sports Governmental Organisation) by building its aggregation view and updating references.
 *
 * @async
 * @function processSgo
 * @param {Object} config - Configuration object containing MongoDB collection names and other settings
 * @param {string} config.mongo.matAggCollectionName - Name of the materialized aggregation collection
 * @param {Object} mongo - MongoDB connection object with db property
 * @param {string} sgoIdScope - External ID scope for the SGO
 * @param {string} sgoId - External ID of the SGO to process
 * @param {string} requestId - Unique identifier for tracking the request
 * @param {boolean} [updatedReferences=true] - Whether to update references after building aggregation
 * @returns {Promise<Object|number|null>} Returns 404 if SGO not found, null if aggregation failed,
 *   or the new aggregation document if successful
 * @throws {Error} Throws error if configuration is invalid or required parameters are missing
 */
async function processSgo(config, mongo, sgoIdScope, sgoId, requestId, updatedReferences = true) {
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
	if (!_.isObject(newAggregationDoc)) {
		warn(`Failed to build new aggregation document`, requestId);
		return null;
	}
	//////////////////////////////////////////////////////////////////////////////
	// Compare old and new aggregation documents to determine if references need to be updated
	if (updatedReferences === true) {
		const operations = buildOperationsForReferenceChange(oldAggregationDoc, newAggregationDoc);
		await executeOperationsForReferenceChange(mongo, config, operations, requestId);
	}
	//////////////////////////////////////////////////////////////////////////////
	return newAggregationDoc;
}

////////////////////////////////////////////////////////////////////////////////
module.exports = { processSgo };
