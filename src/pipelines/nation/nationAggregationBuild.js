////////////////////////////////////////////////////////////////////////////////
// Builds or rebuilds nation aggregation documents and updates inbound references
// (teams, venues, sgos) to maintain bidirectional consistency.
////////////////////////////////////////////////////////////////////////////////
const _ = require('lodash');
const { debug, warn } = require('../../log');
const { runPipeline } = require('../runPipeline');
const { pipeline, queryForNationAggregationDoc } = require('./nationAggregationPipeline');
const { buildOperationsForReferenceChange } = require('../referenceManagement');
const { executeOperationsForReferenceChange } = require('../referenceManagement');

////////////////////////////////////////////////////////////////////////////////
/**
 * Processes nation aggregation by rebuilding materialized aggregation documents
 * and optionally updating related references.
 *
 * @async
 * @function processNation
 * @param {Object} config - Configuration object containing mongo settings
 * @param {string} config.mongo.matAggCollectionName - Name of materialized aggregation collection
 * @param {Object} mongo - MongoDB connection object with db property
 * @param {string} nationIdScope - External ID scope for the nation
 * @param {string} nationId - External ID of the nation to process
 * @param {string} requestId - Request identifier for logging/debugging
 * @param {boolean} [updatedReferences=true] - Whether to update related references
 * @returns {Promise<Object|number|null>} Returns the new aggregation document on success,
 *   404 if nation not found, or null on failure
 * @throws {Error} When configuration is invalid or required parameters are missing
 *
 * @description
 * 1. Validates nation exists in the database
 * 2. Runs aggregation pipeline to rebuild materialized view
 * 3. Compares old vs new aggregation documents
 * 4. Updates references in related collections if specified
 */
async function processNation(config, mongo, nationIdScope, nationId, requestId, updatedReferences = true) {
	if (!_.isString(config?.mongo?.matAggCollectionName)) throw new Error('Invalid configuration: config.mongo.matAggCollectionName must be a string');
	if (!nationId || !nationIdScope) throw new Error('Invalid parameters: nationId and nationIdScope are required');
	//////////////////////////////////////////////////////////////////////////////
	debug(`nationIdScope=${nationIdScope}, nationId=${nationId}`, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// Validate nation exists
	const exists = await mongo.db.collection('nations').countDocuments({ _externalId: nationId, _externalIdScope: nationIdScope }, { limit: 1 });
	if (exists === 0) {
		debug(`Nation not found: ${nationId}@${nationIdScope}`, requestId);
		return 404;
	}
	//////////////////////////////////////////////////////////////////////////////
	const pipelineObj = pipeline(config, nationIdScope, nationId);
	const nationAggQuery = queryForNationAggregationDoc(nationId, nationIdScope);
	//////////////////////////////////////////////////////////////////////////////
	// Previous aggregation (for diff)
	const oldAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(nationAggQuery);
	//////////////////////////////////////////////////////////////////////////////
	// Rebuild
	await runPipeline(mongo, 'nations', pipelineObj, requestId);
	//////////////////////////////////////////////////////////////////////////////
	// Retrieve the new version of the nation aggregation and calculate new reference keys
	const newAggregationDoc = await mongo.db.collection(config.mongo.matAggCollectionName).findOne(nationAggQuery);
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
module.exports = { processNation };
